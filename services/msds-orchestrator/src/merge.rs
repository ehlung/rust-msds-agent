//! 합치기: 에이전트 결과를 결과 스키마(`msds-result/1`) 하나로 모은다. LLM 없이 코드로 한다.

use pekko_agent_msds::message::{AgentKind, AgentTask, JobCreated, TaskOutcome, TaskResult};
use pekko_agent_msds::output::CompositionOutput;
use pekko_agent_msds::profile::DocumentProfile;
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
