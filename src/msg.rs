use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrewriteRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrewriteResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitRequest {
    pub is_primary: bool,
}

pub struct CommitResponse {}
