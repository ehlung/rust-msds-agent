//! 합치기: 에이전트 결과를 결과 스키마(`msds-result/1`) 하나로 모은다. LLM 없이 코드로 한다.

use pekko_agent_msds::message::{AgentKind, AgentTask, JobCreated, TaskOutcome, TaskResult};
use pekko_agent_msds::output::CompositionOutput;
use pekko_agent_msds::profile::{DocumentProfile, Language};
use pekko_agent_msds::result::{DocumentInfo, MsdsResult, Product, Provenance, Severity, Warning, SCHEMA};

/// 작업 하나와 그 결과(최종 시도). 결과가 없으면 응답이 오지 않은 채 끝난 작업이다.
pub struct Finished<'a> {
    pub task: &'a AgentTask,
    pub result: Option<&'a TaskResult>,
}

pub fn merge(job: &JobCreated, profile: &DocumentProfile, tasks: &[Finished<'_>], mut warnings: Vec<Warning>) -> MsdsResult {
    let mut products: Vec<Product> = profile
        .products
        .iter()
        .map(|p| Product {
            index: p.index,
            pages: p.pages,
            format: p.format,
            identification: Default::default(),
            hazards: Default::default(),
            components: Vec::new(),
            hazardous_substances: Vec::new(),
            handling: Default::default(),
            sections: Vec::new(),
        })
        .collect();
    let mut provenance = Vec::new();

    for f in tasks {
        let t = f.task;
        provenance.push(Provenance {
            agent: t.agent,
            model: t.model.name.clone(),
            effort: t.model.effort,
            prompt_version: t.prompt_version.clone(),
            attempts: t.attempt,
        });
        let path = format!("products[{}]", t.product_index);
        let Some(r) = f.result else {
            warnings.push(Warning {
                code: "task_no_response".into(),
                severity: Severity::Error,
                message: format!("{:?} 에이전트가 {}번 시도에도 응답하지 않음", t.agent, t.attempt),
                path: Some(path),
            });
            continue;
        };
        warnings.extend(r.warnings.iter().cloned());
        let output = match &r.outcome {
            TaskOutcome::Ok { output } => output,
            TaskOutcome::Failed { error } => {
                warnings.push(Warning {
                    code: format!("task_failed:{}", error.code),
                    severity: Severity::Error,
                    message: format!("{:?} 에이전트 실패: {}", t.agent, error.message),
                    path: Some(path),
                });
                continue;
            }
        };
        let Some(product) = products.get_mut(t.product_index as usize) else { continue };
        match t.agent {
            AgentKind::Composition => match serde_json::from_value::<CompositionOutput>(output.clone()) {
                Ok(c) => {
                    product.identification = c.identification;
                    product.hazards = c.hazards;
                    product.components = c.components;
                    product.hazardous_substances = c.hazardous_substances;
                }
                Err(e) => warnings.push(Warning {
                    code: "output_unreadable".into(),
                    severity: Severity::Error,
                    message: format!("성분 결과를 읽지 못함: {e}"),
                    path: Some(path),
                }),
            },
            // 번역·취급 에이전트는 아직 없다
            AgentKind::Translate | AgentKind::Handling => {}
        }
    }

    if profile.language == Language::En {
        unify_names(&mut products, &mut warnings);
    }

    MsdsResult {
        schema: SCHEMA.into(),
        job_id: job.job_id,
        document: DocumentInfo {
            file_name: job.file_name.clone(),
            sha256: job.sha256.clone(),
            pages: profile.pages,
            language: profile.language,
            text_layer: profile.text_layer,
        },
        products,
        images: Vec::new(),
        warnings,
        provenance,
    }
}

/// 같은 CAS 번호의 한국어 이름을 문서 안에서 하나로 맞춘다. 처음 나온 이름을 쓴다.
///
/// 제품마다 성분 에이전트를 따로 부르기 때문에, 같은 물질이 제품마다 다른 한국어 이름으로 나올 수 있다
/// (2026-10-23 V66VC229: 아세트산 n-뷰틸 / 아세트산 뷰틸 / 아세트산 n-부틸). 영문 문서에만 쓴다.
/// 한글 문서는 문서에 적힌 국문명을 그대로 두기 때문이다.
fn unify_names(products: &mut [Product], warnings: &mut Vec<Warning>) {
    let mut first: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for (pi, p) in products.iter_mut().enumerate() {
        for (ci, c) in p.components.iter_mut().enumerate() {
            let Some(cas) = c.cas_no.clone() else { continue };
            match first.get(&cas) {
                None => {
                    first.insert(cas, c.name.ko.clone());
                }
                Some(ko) if *ko != c.name.ko => {
                    warnings.push(Warning {
                        code: "name_unified".into(),
                        severity: Severity::Info,
                        message: format!("같은 CAS {cas}의 한국어 이름을 맞춤: {} → {ko}", c.name.ko),
                        path: Some(format!("products[{pi}].components[{ci}].name.ko")),
                    });
                    c.name.ko = ko.clone();
                }
                Some(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pekko_agent_msds::profile::DocFormat;
    use pekko_agent_msds::result::{CasCheck, Component, Text};

    fn product(ko: &str) -> Product {
        Product {
            index: 0,
            pages: [1, 1],
            format: DocFormat::LegacyUs,
            identification: Default::default(),
            hazards: Default::default(),
            components: vec![Component {
                name: Text { src: "n-Butyl Acetate".into(), ko: ko.into() },
                cas_no: Some("123-86-4".into()),
                cas_check: CasCheck::Valid,
                concentration: None,
                exposure_limits: vec![],
                regulatory_marks: vec![],
                pages: vec![],
            }],
            hazardous_substances: vec![],
            handling: Default::default(),
            sections: vec![],
        }
    }

    #[test]
    fn same_cas_gets_first_name() {
        let mut products = vec![product("아세트산 n-뷰틸"), product("아세트산 뷰틸"), product("아세트산 n-뷰틸")];
        let mut warnings = vec![];
        unify_names(&mut products, &mut warnings);
        assert!(products.iter().all(|p| p.components[0].name.ko == "아세트산 n-뷰틸"));
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].path.as_deref(), Some("products[1].components[0].name.ko"));
    }
}
