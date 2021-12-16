use madsim::Request;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("TimestampResponse")]
pub struct TimestampRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampResponse {
    pub ts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<Option<Vec<u8>>, GetError>")]
pub struct GetRequest {
    pub start_ts: u64,
    pub key: Vec<u8>,
}

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum GetError {
    #[error("key is locked by timestamp {ts}")]
    IsLocked { ts: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<(), PrewriteError>")]
pub struct PrewriteRequest {
    pub start_ts: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub primary_key: Vec<u8>,
}

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum PrewriteError {
    #[error("write conflict with timestamp {ts}")]
    WriteConflict { ts: u64 },
    #[error("key is locked by timestamp {ts}")]
    IsLocked { ts: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("Result<(), CommitError>")]
pub struct CommitRequest {
    pub is_primary: bool,
    pub key: Vec<u8>,
    pub start_ts: u64,
    pub commit_ts: u64,
}

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum CommitError {}
