use std::collections::BTreeMap;
use std::io::Result;
use std::net::SocketAddr;
use std::time::Duration;

use madsim::net::rpc::Request;
use madsim::net::Endpoint;

use crate::msg::*;

// BACKOFF_TIME is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
// |  retry time  |  backoff time  |
// |--------------|----------------|
// |      1       |     100 ms     |
// |      2       |     200 ms     |
// |      3       |     400 ms     |
const BACKOFF_TIME: Duration = Duration::from_millis(100);
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
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
        let req = || TimestampRequest {};
        let rsp = self.call_with_retry(self.tso_addr, req).await?;
        tracing::info!(ts = rsp.ts, "get_timestamp");
        Ok(rsp.ts)
    }

    /// Begins a new transaction.
    pub async fn begin(&mut self) {
        tracing::info!("begin");
        assert!(self.start_ts.is_none(), "transaction already begin");
        let start_ts = self.get_timestamp().await.unwrap();
        self.start_ts = Some(start_ts);
        self.write_set.clear();
    }

    /// Gets the value for a given key.
    pub async fn get(&self, key: &[u8]) -> Result<Value> {
        let req = || GetRequest {
            start_ts: self.start_ts.expect("no transaction"),
            key: key.into(),
        };
        loop {
            let (lock_ts, primary) = match self.call_with_retry(self.txn_addr, req).await? {
                Ok(value) => {
                    let value = value.unwrap_or_default();
                    tracing::info!(
                        key = ?String::from_utf8_lossy(key),
                        value = ?String::from_utf8_lossy(&value),
                        "get"
                    );
                    return Ok(value);
                }
                Err(GetError::IsLocked { ts, primary }) => (ts, primary),
            };
            madsim::time::sleep(BACKOFF_TIME).await;
            let req = || CheckRequest {
                key: primary.clone(),
                lock_ts,
            };
            match self.call_with_retry(self.txn_addr, req).await? {
                Some(commit_ts) => {
                    tracing::debug!(key = ?String::from_utf8_lossy(key), lock_ts, "recovery commit");
                    let req = || CommitRequest {
                        is_primary: key == primary,
                        key: key.into(),
                        start_ts: lock_ts,
                        commit_ts,
                    };
                    self.call_with_retry(self.txn_addr, req).await?.unwrap();
                }
                None => {
                    tracing::debug!(key = ?String::from_utf8_lossy(key), lock_ts, "recovery rollback");
                    let req = || RollbackRequest {
                        key: key.into(),
                        start_ts: lock_ts,
                    };
                    self.call_with_retry(self.txn_addr, req).await?.unwrap();
                }
            }
        }
    }

    /// Sets keys in a buffer until commit time.
    pub async fn set(&mut self, key: &[u8], value: &[u8]) {
        tracing::info!(
            key = ?String::from_utf8_lossy(key),
            value = ?String::from_utf8_lossy(value),
            "set"
        );
        self.write_set.insert(key.into(), value.into());
    }

    /// Commits a transaction.
    pub async fn commit(&self) -> Result<bool> {
        tracing::info!("commit");
        if self.write_set.is_empty() {
            // read-only transaction
            return Ok(true);
        }
        let start_ts = self.start_ts.expect("no transaction");

        // Get commit timestamp
        let req = || TimestampRequest {};
        let rsp = self.call_with_retry(self.tso_addr, req).await?;
        let commit_ts = rsp.ts;

        // PreWrite phase
        // first key is primary
        let primary_key = self.write_set.keys().next().unwrap();
        for (key, value) in &self.write_set {
            let req = || PrewriteRequest {
                start_ts,
                key: key.clone(),
                value: value.clone(),
                primary_key: primary_key.clone(),
            };
            let rsp = self.call_with_retry(self.txn_addr, req).await?;
            if rsp.is_err() {
                return Ok(false);
            }
        }

        // Commit phase
        let mut committed = false;
        for key in self.write_set.keys() {
            let req = || CommitRequest {
                start_ts,
                commit_ts,
                key: key.clone(),
                is_primary: key == primary_key,
            };
            match self.call_with_retry(self.txn_addr, req).await {
                Ok(Ok(())) => committed = true,
                Err(e) if !committed => return Err(e),
                Err(_) | Ok(Err(_)) => return Ok(true),
            }
        }

        Ok(true)
    }

    async fn call_with_retry<F, R>(&self, dst: SocketAddr, mut request: F) -> Result<R::Response>
    where
        F: FnMut() -> R,
        R: Request,
    {
        let mut timeout = BACKOFF_TIME;
        let mut last_err = None;
        for _ in 0..RETRY_TIMES {
            match self.ep.call_timeout(dst, request(), timeout).await {
                Ok(rsp) => return Ok(rsp),
                Err(e) => last_err = Some(e),
            }
            timeout *= 2;
        }
        Err(last_err.unwrap())
    }
}
