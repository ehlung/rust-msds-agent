// admin_client.rs - Administrative API for Event Bus Management
// Topic CRUD, consumer group management, offset operations, cluster info

use std::collections::HashMap;
use std::time::{Duration, Instant};

// ============================================================================
// Admin Operations
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum AdminOperation {
    CreateTopic,
    DeleteTopic,
    AlterTopicConfig,
    ListTopics,
    DescribeTopic,
    CreatePartitions,
    ListConsumerGroups,
    DescribeConsumerGroup,
    DeleteConsumerGroup,
    ResetOffsets,
    DeleteRecords,
    DescribeCluster,
}

impl std::fmt::Display for AdminOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdminOperation::CreateTopic => write!(f, "CreateTopic"),
            AdminOperation::DeleteTopic => write!(f, "DeleteTopic"),
            AdminOperation::AlterTopicConfig => write!(f, "AlterTopicConfig"),
            AdminOperation::ListTopics => write!(f, "ListTopics"),
            AdminOperation::DescribeTopic => write!(f, "DescribeTopic"),
            AdminOperation::CreatePartitions => write!(f, "CreatePartitions"),
            AdminOperation::ListConsumerGroups => write!(f, "ListConsumerGroups"),
            AdminOperation::DescribeConsumerGroup => write!(f, "DescribeConsumerGroup"),
            AdminOperation::DeleteConsumerGroup => write!(f, "DeleteConsumerGroup"),
            AdminOperation::ResetOffsets => write!(f, "ResetOffsets"),
            AdminOperation::DeleteRecords => write!(f, "DeleteRecords"),
            AdminOperation::DescribeCluster => write!(f, "DescribeCluster"),
        }
    }
}

// ============================================================================
// Topic Description
// ============================================================================

#[derive(Debug, Clone)]
pub struct TopicDescription {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub config: HashMap<String, String>,
    pub is_internal: bool,
    pub created_at: Option<Instant>,
    pub total_events: u64,
}

impl TopicDescription {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            partitions: Vec::new(),
            config: HashMap::new(),
            is_internal: false,
            created_at: None,
            total_events: 0,
        }
    }

    pub fn add_partition(mut self, info: PartitionInfo) -> Self {
        self.partitions.push(info);
        self
    }

    pub fn with_config(mut self, key: &str, value: &str) -> Self {
        self.config.insert(key.to_string(), value.to_string());
        self
    }

    pub fn internal(mut self) -> Self {
        self.is_internal = true;
        self
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn total_events(&self) -> u64 {
        self.partitions.iter().map(|p| p.end_offset - p.start_offset).sum()
    }
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub id: u32,
    pub leader: Option<String>,
    pub replicas: Vec<String>,
    pub in_sync_replicas: Vec<String>,
    pub start_offset: u64,
    pub end_offset: u64,
}

impl PartitionInfo {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            leader: None,
            replicas: Vec::new(),
            in_sync_replicas: Vec::new(),
            start_offset: 0,
            end_offset: 0,
        }
    }

    pub fn with_leader(mut self, leader: &str) -> Self {
        self.leader = Some(leader.to_string());
        self
    }

    pub fn with_offsets(mut self, start: u64, end: u64) -> Self {
        self.start_offset = start;
        self.end_offset = end;
        self
    }

    pub fn size(&self) -> u64 {
        self.end_offset - self.start_offset
    }

    pub fn is_under_replicated(&self) -> bool {
        self.in_sync_replicas.len() < self.replicas.len()
    }
}

// ============================================================================
// Consumer Group Description
// ============================================================================

#[derive(Debug, Clone)]
pub struct ConsumerGroupDescription {
    pub group_id: String,
    pub state: ConsumerGroupState,
    pub members: Vec<MemberDescription>,
    pub coordinator: Option<String>,
    pub partition_assignor: String,
}

impl ConsumerGroupDescription {
    pub fn new(group_id: &str) -> Self {
        Self {
            group_id: group_id.to_string(),
            state: ConsumerGroupState::Empty,
            members: Vec::new(),
            coordinator: None,
            partition_assignor: "range".to_string(),
        }
    }

    pub fn with_state(mut self, state: ConsumerGroupState) -> Self {
        self.state = state;
        self
    }

    pub fn add_member(mut self, member: MemberDescription) -> Self {
        self.members.push(member);
        self
    }

    pub fn with_coordinator(mut self, coord: &str) -> Self {
        self.coordinator = Some(coord.to_string());
        self
    }

    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    pub fn total_assigned_partitions(&self) -> usize {
        self.members.iter().map(|m| m.assigned_partitions.len()).sum()
    }

    pub fn is_active(&self) -> bool {
        self.state == ConsumerGroupState::Stable
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConsumerGroupState {
    Empty,
    Stable,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
}

impl std::fmt::Display for ConsumerGroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumerGroupState::Empty => write!(f, "Empty"),
            ConsumerGroupState::Stable => write!(f, "Stable"),
            ConsumerGroupState::PreparingRebalance => write!(f, "PreparingRebalance"),
            ConsumerGroupState::CompletingRebalance => write!(f, "CompletingRebalance"),
            ConsumerGroupState::Dead => write!(f, "Dead"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemberDescription {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub assigned_partitions: Vec<(String, u32)>,
}

impl MemberDescription {
    pub fn new(member_id: &str, client_id: &str) -> Self {
        Self {
            member_id: member_id.to_string(),
            client_id: client_id.to_string(),
            host: "localhost".to_string(),
            assigned_partitions: Vec::new(),
        }
    }

    pub fn with_host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    pub fn assign(mut self, topic: &str, partition: u32) -> Self {
        self.assigned_partitions.push((topic.to_string(), partition));
        self
    }
}

// ============================================================================
// Offset Reset Strategy
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum OffsetResetStrategy {
    Earliest,
    Latest,
    ToOffset(u64),
    ToTimestamp(u64),
    ShiftBy(i64),
    ToDateTime(String),
}

impl std::fmt::Display for OffsetResetStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OffsetResetStrategy::Earliest => write!(f, "earliest"),
            OffsetResetStrategy::Latest => write!(f, "latest"),
            OffsetResetStrategy::ToOffset(o) => write!(f, "offset:{}", o),
            OffsetResetStrategy::ToTimestamp(t) => write!(f, "timestamp:{}", t),
            OffsetResetStrategy::ShiftBy(s) => write!(f, "shift:{}", s),
            OffsetResetStrategy::ToDateTime(d) => write!(f, "datetime:{}", d),
        }
    }
}

// ============================================================================
// Cluster Info
// ============================================================================

#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub cluster_id: String,
    pub nodes: Vec<NodeInfo>,
    pub controller_id: Option<String>,
    pub topic_count: usize,
    pub total_partitions: usize,
}

impl ClusterInfo {
    pub fn new(cluster_id: &str) -> Self {
        Self {
            cluster_id: cluster_id.to_string(),
            nodes: Vec::new(),
            controller_id: None,
            topic_count: 0,
            total_partitions: 0,
        }
    }

    pub fn add_node(mut self, node: NodeInfo) -> Self {
        self.nodes.push(node);
        self
    }

    pub fn with_controller(mut self, id: &str) -> Self {
        self.controller_id = Some(id.to_string());
        self
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_healthy(&self) -> bool {
        self.controller_id.is_some() && self.nodes.iter().all(|n| n.is_alive)
    }
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
    pub is_alive: bool,
}

impl NodeInfo {
    pub fn new(id: &str, host: &str, port: u16) -> Self {
        Self {
            id: id.to_string(),
            host: host.to_string(),
            port,
            rack: None,
            is_alive: true,
        }
    }

    pub fn with_rack(mut self, rack: &str) -> Self {
        self.rack = Some(rack.to_string());
        self
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

// ============================================================================
// Admin Result
// ============================================================================

#[derive(Debug, Clone)]
pub struct AdminResult {
    pub operation: AdminOperation,
    pub success: bool,
    pub message: String,
    pub duration: Duration,
}

impl AdminResult {
    pub fn ok(operation: AdminOperation, message: &str) -> Self {
        Self {
            operation,
            success: true,
            message: message.to_string(),
            duration: Duration::ZERO,
        }
    }

    pub fn err(operation: AdminOperation, message: &str) -> Self {
        Self {
            operation,
            success: false,
            message: message.to_string(),
            duration: Duration::ZERO,
        }
    }

    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }
}

// ============================================================================
// Admin Client
// ============================================================================

#[derive(Debug)]
pub struct AdminClient {
    topics: HashMap<String, TopicDescription>,
    consumer_groups: HashMap<String, ConsumerGroupDescription>,
    cluster: ClusterInfo,
    operation_log: Vec<AdminLogEntry>,
    max_log_entries: usize,
}

#[derive(Debug, Clone)]
pub struct AdminLogEntry {
    pub operation: AdminOperation,
    pub target: String,
    pub success: bool,
    pub timestamp: Instant,
}

impl AdminClient {
    pub fn new(cluster_id: &str) -> Self {
        Self {
            topics: HashMap::new(),
            consumer_groups: HashMap::new(),
            cluster: ClusterInfo::new(cluster_id),
            operation_log: Vec::new(),
            max_log_entries: 1000,
        }
    }

    fn log_operation(&mut self, op: AdminOperation, target: &str, success: bool) {
        self.operation_log.push(AdminLogEntry {
            operation: op,
            target: target.to_string(),
            success,
            timestamp: Instant::now(),
        });
        if self.operation_log.len() > self.max_log_entries {
            self.operation_log.remove(0);
        }
    }

    // Topic operations
    pub fn create_topic(&mut self, name: &str, partitions: u32) -> AdminResult {
        if self.topics.contains_key(name) {
            self.log_operation(AdminOperation::CreateTopic, name, false);
            return AdminResult::err(AdminOperation::CreateTopic, &format!("topic '{}' already exists", name));
        }
        let mut desc = TopicDescription::new(name);
        desc.created_at = Some(Instant::now());
        for i in 0..partitions {
            desc.partitions.push(PartitionInfo::new(i));
        }
        self.topics.insert(name.to_string(), desc);
        self.cluster.topic_count += 1;
        self.cluster.total_partitions += partitions as usize;
        self.log_operation(AdminOperation::CreateTopic, name, true);
        AdminResult::ok(AdminOperation::CreateTopic, &format!("created topic '{}' with {} partitions", name, partitions))
    }

    pub fn delete_topic(&mut self, name: &str) -> AdminResult {
        match self.topics.remove(name) {
            Some(desc) => {
                self.cluster.topic_count = self.cluster.topic_count.saturating_sub(1);
                self.cluster.total_partitions = self.cluster.total_partitions.saturating_sub(desc.partitions.len());
                self.log_operation(AdminOperation::DeleteTopic, name, true);
                AdminResult::ok(AdminOperation::DeleteTopic, &format!("deleted topic '{}'", name))
            }
            None => {
                self.log_operation(AdminOperation::DeleteTopic, name, false);
                AdminResult::err(AdminOperation::DeleteTopic, &format!("topic '{}' not found", name))
            }
        }
    }

    pub fn describe_topic(&self, name: &str) -> Option<&TopicDescription> {
        self.topics.get(name)
    }

    pub fn list_topics(&self) -> Vec<&str> {
        self.topics.keys().map(|s| s.as_str()).collect()
    }

    pub fn topic_exists(&self, name: &str) -> bool {
        self.topics.contains_key(name)
    }

    pub fn alter_topic_config(&mut self, name: &str, key: &str, value: &str) -> AdminResult {
        match self.topics.get_mut(name) {
            Some(desc) => {
                desc.config.insert(key.to_string(), value.to_string());
                self.log_operation(AdminOperation::AlterTopicConfig, name, true);
                AdminResult::ok(AdminOperation::AlterTopicConfig, &format!("set {}={} on '{}'", key, value, name))
            }
            None => {
                self.log_operation(AdminOperation::AlterTopicConfig, name, false);
                AdminResult::err(AdminOperation::AlterTopicConfig, &format!("topic '{}' not found", name))
            }
        }
    }

    pub fn add_partitions(&mut self, name: &str, additional: u32) -> AdminResult {
        match self.topics.get_mut(name) {
            Some(desc) => {
                let current = desc.partitions.len() as u32;
                for i in 0..additional {
                    desc.partitions.push(PartitionInfo::new(current + i));
                }
                self.cluster.total_partitions += additional as usize;
                self.log_operation(AdminOperation::CreatePartitions, name, true);
                AdminResult::ok(AdminOperation::CreatePartitions, &format!("added {} partitions to '{}'", additional, name))
            }
            None => {
                self.log_operation(AdminOperation::CreatePartitions, name, false);
                AdminResult::err(AdminOperation::CreatePartitions, &format!("topic '{}' not found", name))
            }
        }
    }

    // Consumer group operations
    pub fn register_consumer_group(&mut self, desc: ConsumerGroupDescription) {
        self.consumer_groups.insert(desc.group_id.clone(), desc);
    }

    pub fn describe_consumer_group(&self, group_id: &str) -> Option<&ConsumerGroupDescription> {
        self.consumer_groups.get(group_id)
    }

    pub fn list_consumer_groups(&self) -> Vec<&str> {
        self.consumer_groups.keys().map(|s| s.as_str()).collect()
    }

    pub fn delete_consumer_group(&mut self, group_id: &str) -> AdminResult {
        match self.consumer_groups.remove(group_id) {
            Some(_) => {
                self.log_operation(AdminOperation::DeleteConsumerGroup, group_id, true);
                AdminResult::ok(AdminOperation::DeleteConsumerGroup, &format!("deleted group '{}'", group_id))
            }
            None => {
                self.log_operation(AdminOperation::DeleteConsumerGroup, group_id, false);
                AdminResult::err(AdminOperation::DeleteConsumerGroup, &format!("group '{}' not found", group_id))
            }
        }
    }

    // Cluster operations
    pub fn add_node(&mut self, node: NodeInfo) {
        self.cluster.nodes.push(node);
    }

    pub fn describe_cluster(&self) -> &ClusterInfo {
        &self.cluster
    }

    // Operation log
    pub fn operation_log(&self) -> &[AdminLogEntry] {
        &self.operation_log
    }

    pub fn operation_count(&self) -> usize {
        self.operation_log.len()
    }

    pub fn stats(&self) -> AdminClientStats {
        let successful = self.operation_log.iter().filter(|e| e.success).count();
        let failed = self.operation_log.iter().filter(|e| !e.success).count();
        AdminClientStats {
            topic_count: self.topics.len(),
            consumer_group_count: self.consumer_groups.len(),
            node_count: self.cluster.nodes.len(),
            total_operations: self.operation_log.len(),
            successful_operations: successful,
            failed_operations: failed,
        }
    }
}

// ============================================================================
// Admin Client Stats
// ============================================================================

#[derive(Debug, Clone)]
pub struct AdminClientStats {
    pub topic_count: usize,
    pub consumer_group_count: usize,
    pub node_count: usize,
    pub total_operations: usize,
    pub successful_operations: usize,
    pub failed_operations: usize,
}

impl AdminClientStats {
    pub fn success_rate(&self) -> f64 {
        if self.total_operations == 0 {
            1.0
        } else {
            self.successful_operations as f64 / self.total_operations as f64
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_operation_display() {
        assert_eq!(AdminOperation::CreateTopic.to_string(), "CreateTopic");
        assert_eq!(AdminOperation::DeleteTopic.to_string(), "DeleteTopic");
        assert_eq!(AdminOperation::ListTopics.to_string(), "ListTopics");
        assert_eq!(AdminOperation::ResetOffsets.to_string(), "ResetOffsets");
    }

    #[test]
    fn test_admin_client_new() {
        let client = AdminClient::new("test-cluster");
        assert!(client.list_topics().is_empty());
        assert!(client.list_consumer_groups().is_empty());
        assert_eq!(client.describe_cluster().cluster_id, "test-cluster");
    }

    #[test]
    fn test_create_topic() {
        let mut client = AdminClient::new("test");
        let result = client.create_topic("orders", 6);
        assert!(result.success);
        assert!(client.topic_exists("orders"));
        let desc = client.describe_topic("orders").unwrap();
        assert_eq!(desc.partition_count(), 6);
    }

    #[test]
    fn test_create_topic_duplicate() {
        let mut client = AdminClient::new("test");
        client.create_topic("orders", 3);
        let result = client.create_topic("orders", 3);
        assert!(!result.success);
    }

    #[test]
    fn test_delete_topic() {
        let mut client = AdminClient::new("test");
        client.create_topic("orders", 3);
        let result = client.delete_topic("orders");
        assert!(result.success);
        assert!(!client.topic_exists("orders"));
    }

    #[test]
    fn test_delete_topic_not_found() {
        let mut client = AdminClient::new("test");
        let result = client.delete_topic("nonexistent");
        assert!(!result.success);
    }

    #[test]
    fn test_list_topics() {
        let mut client = AdminClient::new("test");
        client.create_topic("a", 1);
        client.create_topic("b", 1);
        client.create_topic("c", 1);
        let topics = client.list_topics();
        assert_eq!(topics.len(), 3);
    }

    #[test]
    fn test_alter_topic_config() {
        let mut client = AdminClient::new("test");
        client.create_topic("orders", 3);
        let result = client.alter_topic_config("orders", "retention.ms", "86400000");
        assert!(result.success);
        let desc = client.describe_topic("orders").unwrap();
        assert_eq!(desc.config.get("retention.ms"), Some(&"86400000".to_string()));
    }

    #[test]
    fn test_alter_topic_config_not_found() {
        let mut client = AdminClient::new("test");
        let result = client.alter_topic_config("missing", "k", "v");
        assert!(!result.success);
    }

    #[test]
    fn test_add_partitions() {
        let mut client = AdminClient::new("test");
        client.create_topic("orders", 3);
        let result = client.add_partitions("orders", 3);
        assert!(result.success);
        let desc = client.describe_topic("orders").unwrap();
        assert_eq!(desc.partition_count(), 6);
    }

    #[test]
    fn test_add_partitions_not_found() {
        let mut client = AdminClient::new("test");
        let result = client.add_partitions("missing", 3);
        assert!(!result.success);
    }

    #[test]
    fn test_consumer_group_crud() {
        let mut client = AdminClient::new("test");
        let group = ConsumerGroupDescription::new("my-group")
            .with_state(ConsumerGroupState::Stable)
            .add_member(MemberDescription::new("m1", "c1").assign("orders", 0));
        client.register_consumer_group(group);

        assert_eq!(client.list_consumer_groups().len(), 1);
        let desc = client.describe_consumer_group("my-group").unwrap();
        assert_eq!(desc.member_count(), 1);
        assert!(desc.is_active());

        let result = client.delete_consumer_group("my-group");
        assert!(result.success);
        assert!(client.list_consumer_groups().is_empty());
    }

    #[test]
    fn test_delete_consumer_group_not_found() {
        let mut client = AdminClient::new("test");
        let result = client.delete_consumer_group("unknown");
        assert!(!result.success);
    }

    #[test]
    fn test_cluster_info() {
        let mut client = AdminClient::new("prod-cluster");
        client.add_node(NodeInfo::new("node-1", "10.0.0.1", 9092).with_rack("us-east-1a"));
        client.add_node(NodeInfo::new("node-2", "10.0.0.2", 9092).with_rack("us-east-1b"));

        let cluster = client.describe_cluster();
        assert_eq!(cluster.node_count(), 2);
        assert!(!cluster.is_healthy()); // no controller
    }

    #[test]
    fn test_cluster_healthy() {
        let cluster = ClusterInfo::new("c1")
            .add_node(NodeInfo::new("n1", "localhost", 9092))
            .with_controller("n1");
        assert!(cluster.is_healthy());
    }

    #[test]
    fn test_node_info() {
        let node = NodeInfo::new("n1", "10.0.0.1", 9092).with_rack("rack-a");
        assert_eq!(node.address(), "10.0.0.1:9092");
        assert_eq!(node.rack, Some("rack-a".to_string()));
        assert!(node.is_alive);
    }

    #[test]
    fn test_partition_info() {
        let partition = PartitionInfo::new(0)
            .with_leader("node-1")
            .with_offsets(0, 1000);
        assert_eq!(partition.size(), 1000);
        assert!(!partition.is_under_replicated());
    }

    #[test]
    fn test_partition_under_replicated() {
        let mut partition = PartitionInfo::new(0);
        partition.replicas = vec!["n1".into(), "n2".into(), "n3".into()];
        partition.in_sync_replicas = vec!["n1".into(), "n2".into()];
        assert!(partition.is_under_replicated());
    }

    #[test]
    fn test_topic_description() {
        let desc = TopicDescription::new("orders")
            .add_partition(PartitionInfo::new(0).with_offsets(0, 500))
            .add_partition(PartitionInfo::new(1).with_offsets(0, 300))
            .with_config("retention.ms", "86400000");
        assert_eq!(desc.partition_count(), 2);
        assert_eq!(desc.total_events(), 800);
        assert!(!desc.is_internal);
    }

    #[test]
    fn test_topic_description_internal() {
        let desc = TopicDescription::new("__consumer_offsets").internal();
        assert!(desc.is_internal);
    }

    #[test]
    fn test_consumer_group_description() {
        let desc = ConsumerGroupDescription::new("group-1")
            .with_state(ConsumerGroupState::Stable)
            .with_coordinator("node-1")
            .add_member(
                MemberDescription::new("m1", "c1")
                    .with_host("10.0.0.5")
                    .assign("orders", 0)
                    .assign("orders", 1)
            );
        assert_eq!(desc.member_count(), 1);
        assert_eq!(desc.total_assigned_partitions(), 2);
        assert!(desc.is_active());
    }

    #[test]
    fn test_consumer_group_state_display() {
        assert_eq!(ConsumerGroupState::Empty.to_string(), "Empty");
        assert_eq!(ConsumerGroupState::Stable.to_string(), "Stable");
        assert_eq!(ConsumerGroupState::Dead.to_string(), "Dead");
    }

    #[test]
    fn test_offset_reset_strategy_display() {
        assert_eq!(OffsetResetStrategy::Earliest.to_string(), "earliest");
        assert_eq!(OffsetResetStrategy::Latest.to_string(), "latest");
        assert_eq!(OffsetResetStrategy::ToOffset(100).to_string(), "offset:100");
        assert_eq!(OffsetResetStrategy::ShiftBy(-10).to_string(), "shift:-10");
    }

    #[test]
    fn test_admin_result() {
        let ok = AdminResult::ok(AdminOperation::CreateTopic, "created");
        assert!(ok.success);
        let err = AdminResult::err(AdminOperation::DeleteTopic, "not found");
        assert!(!err.success);
        let with_dur = ok.with_duration(Duration::from_millis(50));
        assert_eq!(with_dur.duration, Duration::from_millis(50));
    }

    #[test]
    fn test_operation_log() {
        let mut client = AdminClient::new("test");
        client.create_topic("a", 1);
        client.create_topic("a", 1); // duplicate
        client.delete_topic("a");
        assert_eq!(client.operation_count(), 3);
        let log = client.operation_log();
        assert!(log[0].success);
        assert!(!log[1].success);
        assert!(log[2].success);
    }

    #[test]
    fn test_admin_stats() {
        let mut client = AdminClient::new("test");
        client.create_topic("t1", 3);
        client.create_topic("t2", 3);
        client.delete_topic("missing");
        client.register_consumer_group(ConsumerGroupDescription::new("g1"));
        client.add_node(NodeInfo::new("n1", "localhost", 9092));

        let stats = client.stats();
        assert_eq!(stats.topic_count, 2);
        assert_eq!(stats.consumer_group_count, 1);
        assert_eq!(stats.node_count, 1);
        assert_eq!(stats.successful_operations, 2);
        assert_eq!(stats.failed_operations, 1);
    }

    #[test]
    fn test_admin_stats_success_rate() {
        let stats = AdminClientStats {
            topic_count: 0,
            consumer_group_count: 0,
            node_count: 0,
            total_operations: 10,
            successful_operations: 8,
            failed_operations: 2,
        };
        assert_eq!(stats.success_rate(), 0.8);
    }

    #[test]
    fn test_admin_stats_empty_success_rate() {
        let stats = AdminClientStats {
            topic_count: 0,
            consumer_group_count: 0,
            node_count: 0,
            total_operations: 0,
            successful_operations: 0,
            failed_operations: 0,
        };
        assert_eq!(stats.success_rate(), 1.0);
    }

    #[test]
    fn test_cluster_total_partitions_tracking() {
        let mut client = AdminClient::new("test");
        client.create_topic("a", 3);
        client.create_topic("b", 6);
        assert_eq!(client.describe_cluster().total_partitions, 9);
        client.add_partitions("a", 3);
        assert_eq!(client.describe_cluster().total_partitions, 12);
        client.delete_topic("a");
        assert_eq!(client.describe_cluster().total_partitions, 6);
    }
}
