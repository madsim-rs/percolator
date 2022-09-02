use std::collections::BTreeMap;
use std::io::Result;
use std::net::SocketAddr;

use madsim::net::Endpoint;

use crate::msg::*;

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
// |  retry time  |  backoff time  |
// |--------------|----------------|
// |      1       |       100      |
// |      2       |       200      |
// |      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    ep: Endpoint,
    tso_addr: SocketAddr,
    txn_addr: SocketAddr,
    start_ts: Option<u64>,
    write_set: BTreeMap<Key, Value>,
}

type Key = Vec<u8>;
type Value = Vec<u8>;

impl Client {
    /// Creates a new Client.
    pub async fn new(tso_addr: SocketAddr, txn_addr: SocketAddr) -> Result<Client> {
        Ok(Client {
            ep: Endpoint::bind("0.0.0.0:0").await?,
            tso_addr,
            txn_addr,
            start_ts: None,
            write_set: BTreeMap::new(),
        })
    }

    /// Gets a timestamp from a TSO.
    pub async fn get_timestamp(&self) -> Result<u64> {
        let rsp = self.ep.call(self.tso_addr, TimestampRequest {}).await?;
        Ok(rsp.ts)
    }

    /// Begins a new transaction.
    pub async fn begin(&mut self) {
        assert!(self.start_ts.is_none(), "transaction already begin");
        let start_ts = self.get_timestamp().await.unwrap();
        self.start_ts = Some(start_ts);
        self.write_set.clear();
    }

    /// Gets the value for a given key.
    pub async fn get(&self, key: &[u8]) -> Result<Value> {
        let req = GetRequest {
            start_ts: self.start_ts.expect("no transaction"),
            key: key.into(),
        };
        let rsp = self.ep.call(self.txn_addr, req).await?;
        Ok(rsp.unwrap().unwrap_or_default())
    }

    /// Sets keys in a buffer until commit time.
    pub async fn set(&mut self, key: &[u8], value: &[u8]) {
        self.write_set.insert(key.into(), value.into());
    }

    /// Commits a transaction.
    pub async fn commit(&self) -> Result<bool> {
        if self.write_set.is_empty() {
            // read-only transaction
            return Ok(true);
        }
        let start_ts = self.start_ts.expect("no transaction");

        // Get commit timestamp
        let rsp = self.ep.call(self.tso_addr, TimestampRequest {}).await?;
        let commit_ts = rsp.ts;

        // PreWrite phase
        // first key is primary
        let primary_key = self.write_set.keys().next().unwrap();
        for (key, value) in &self.write_set {
            let req = PrewriteRequest {
                start_ts,
                key: key.clone(),
                value: value.clone(),
                primary_key: primary_key.clone(),
            };
            let rsp = self.ep.call(self.txn_addr, req).await?;
            if rsp.is_err() {
                return Ok(false);
            }
        }

        // Commit phase
        for (key, value) in &self.write_set {
            let req = CommitRequest {
                start_ts,
                commit_ts,
                key: key.clone(),
                is_primary: key == primary_key,
            };
            let rsp = self.ep.call(self.txn_addr, req).await?;
            if rsp.is_err() {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
