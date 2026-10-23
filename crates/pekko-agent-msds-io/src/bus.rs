use std::time::Duration;

use anyhow::{anyhow, Context};
use pekko_agent_msds::Envelope;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{ClientConfig, Message};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::info;

/// 서비스 하나가 쓰는 Kafka 연결. 보내기는 공유하고, 받기는 [`Bus::consumer`]로 만든다.
#[derive(Clone)]
pub struct Bus {
    brokers: String,
    service: String,
    producer: FutureProducer,
}

impl Bus {
    pub fn new(brokers: &str, service: &str) -> anyhow::Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("client.id", service)
            // 재전송 때 같은 메시지가 두 번 쓰이지 않게 한다
            .set("enable.idempotence", "true")
            // 섹션 본문이 든 메시지가 기본 한도(1MB) 근처까지 갈 수 있어 여유를 둔다
            .set("message.max.bytes", "4000000")
            // lz4는 librdkafka에 기본으로 들어 있다(zstd는 별도 빌드 기능이 필요)
            .set("compression.type", "lz4")
            .create()
            .context("Kafka producer 생성 실패")?;
        Ok(Self { brokers: brokers.to_string(), service: service.to_string(), producer })
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    /// 토픽이 없으면 만든다. 브로커의 자동 생성을 꺼 두었기 때문에 서비스가 시작할 때 부른다.
    pub async fn ensure_topics(&self, topics: &[&str], partitions: i32) -> anyhow::Result<()> {
        let admin: AdminClient<DefaultClientContext> =
            ClientConfig::new().set("bootstrap.servers", &self.brokers).create()?;
        let new: Vec<NewTopic> =
            topics.iter().map(|t| NewTopic::new(t, partitions, TopicReplication::Fixed(1))).collect();
        for r in admin.create_topics(&new, &AdminOptions::new()).await? {
            match r {
                Ok(t) => info!(topic = %t, "토픽 생성"),
                Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((t, e)) => return Err(anyhow!("토픽 {t} 생성 실패: {e}")),
            }
        }
        Ok(())
    }

    /// 봉투를 JSON으로 보낸다. 키는 작업 ID라 한 작업의 메시지는 순서가 지켜진다.
    pub async fn publish<T: Serialize>(&self, topic: &str, envelope: &Envelope<T>) -> anyhow::Result<()> {
        let body = serde_json::to_vec(envelope)?;
        let key = envelope.key();
        self.producer
            .send(FutureRecord::to(topic).key(&key).payload(&body), Duration::from_secs(10))
            .await
            .map_err(|(e, _)| anyhow!("{topic}로 보내기 실패: {e}"))?;
        info!(topic, job_id = %key, event = %envelope.event_type, bytes = body.len(), "보냄");
        Ok(())
    }

    /// 소비자 그룹으로 받는다. 같은 그룹의 인스턴스끼리 파티션을 나눠 갖는다.
    /// 자동 커밋을 끄므로, 처리를 마친 뒤 [`commit`]을 불러야 다음 메시지로 넘어간 것이 기록된다.
    pub fn consumer(&self, group: &str, topics: &[&str]) -> anyhow::Result<StreamConsumer> {
        let c: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", group)
            .set("client.id", &self.service)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("fetch.message.max.bytes", "4000000")
            // LLM 호출처럼 오래 걸리는 처리 중에 그룹에서 빠지지 않게 한다
            .set("max.poll.interval.ms", "1800000")
            .create()
            .context("Kafka consumer 생성 실패")?;
        c.subscribe(topics)?;
        Ok(c)
    }
}

/// 받은 메시지를 봉투로 읽는다.
pub fn decode<T: DeserializeOwned>(msg: &BorrowedMessage<'_>) -> anyhow::Result<Envelope<T>> {
    let body = msg.payload().ok_or_else(|| anyhow!("빈 메시지"))?;
    serde_json::from_slice(body).with_context(|| {
        format!("{} partition {} offset {} 메시지를 읽지 못함", msg.topic(), msg.partition(), msg.offset())
    })
}

pub fn commit(consumer: &StreamConsumer, msg: &BorrowedMessage<'_>) -> anyhow::Result<()> {
    consumer.commit_message(msg, CommitMode::Sync)?;
    Ok(())
}
