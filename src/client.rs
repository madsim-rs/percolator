use std::io::Result;
use std::net::SocketAddr;

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
    // Your definitions here.
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_addr: SocketAddr, txn_addr: SocketAddr) -> Client {
        // Your code here.
        Client {}
    }

    /// Gets a timestamp from a TSO.
    pub async fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        unimplemented!()
    }

    /// Begins a new transaction.
    pub async fn begin(&mut self) {
        // Your code here.
        unimplemented!()
    }

    /// Gets the value for a given key.
    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        // Your code here.
        unimplemented!()
    }

    /// Sets keys in a buffer until commit time.
    pub async fn set(&mut self, key: &[u8], value: &[u8]) {
        // Your code here.
        unimplemented!()
    }

    /// Commits a transaction.
    pub async fn commit(&self) -> Result<bool> {
        // Your code here.
        unimplemented!()
    }
}
