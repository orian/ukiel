//! Immutable pipeline definitions. Credentials and worker activation are not
//! catalog definition fields; workers select names explicitly at startup.
use crate::{HypertableId, PipelineId, StreamTableId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    Kafka,
    Parquet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Endpoint {
    Kafka(StreamTableId),
    Parquet(HypertableId),
}

impl Endpoint {
    pub fn kind(self) -> EndpointKind {
        match self {
            Self::Kafka(_) => EndpointKind::Kafka,
            Self::Parquet(_) => EndpointKind::Parquet,
        }
    }
    pub fn id(self) -> i64 {
        match self {
            Self::Kafka(id) => id.0,
            Self::Parquet(id) => id.0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorPolicy {
    #[default]
    Stall,
    Skip,
}

/// V1 targets are Parquet; Kafka egress delivery modes are outside v1.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    #[default]
    ExactlyOnce,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamTableDefinition {
    pub name: String,
    pub topic: String,
    pub format: String,
    pub schema: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamTable {
    pub id: StreamTableId,
    pub definition: StreamTableDefinition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineDefinition {
    pub name: String,
    pub source: Endpoint,
    pub target: HypertableId,
    pub sql: String,
    pub delivery: Delivery,
    pub error_policy: ErrorPolicy,
    pub event_time_column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: PipelineId,
    pub definition: PipelineDefinition,
}
