use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::msg::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone)]
pub struct TimestampOracle {
    next_ts: Arc<AtomicU64>,
}

impl Default for TimestampOracle {
    fn default() -> Self {
        Self::new()
    }
}

#[madsim::service]
impl TimestampOracle {
    pub fn new() -> Self {
        let tso = TimestampOracle {
            next_ts: Default::default(),
        };
        tso.add_rpc_handler();
        tso
    }

    // example get_timestamp RPC handler.
    #[rpc]
    async fn get_timestamp(&self, _: TimestampRequest) -> TimestampResponse {
        let ts = self.next_ts.fetch_add(1, Ordering::SeqCst);
        TimestampResponse { ts }
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

impl Value {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Vector(bytes) => bytes,
            _ => panic!("expect vector"),
        }
    }

    fn as_ts(&self) -> u64 {
        match self {
            Self::Timestamp(ts) => *ts,
            _ => panic!("expect timestamp"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        let map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };
        let start = (key.clone(), ts_start_inclusive.unwrap_or(0));
        let end = (key, ts_end_inclusive.unwrap_or(u64::MAX));
        map.range(start..=end).next_back()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        let map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        let map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.remove(&(key, commit_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone)]
pub struct MemoryStorage {
    table: Arc<Mutex<KvTable>>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[madsim::service]
impl MemoryStorage {
    pub fn new() -> Self {
        let s = MemoryStorage {
            table: Default::default(),
        };
        s.add_rpc_handler();
        s
    }

    #[rpc]
    async fn get(&self, req: GetRequest) -> Result<Option<Vec<u8>>, GetError> {
        let table = self.table.lock().unwrap();
        let lock = table.read(req.key.clone(), Column::Lock, None, Some(req.start_ts));
        if let Some((&(_, ts), _)) = lock {
            return Err(GetError::IsLocked { ts });
        }
        let ts = match table.read(req.key.clone(), Column::Write, None, Some(req.start_ts)) {
            Some((_, v)) => v.as_ts(),
            None => return Ok(None),
        };
        let value = table
            .read(req.key, Column::Data, Some(ts), Some(ts))
            .unwrap()
            .1
            .as_bytes();
        Ok(Some(value.to_vec()))
    }

    #[rpc]
    async fn prewrite(&self, req: PrewriteRequest) -> Result<(), PrewriteError> {
        let mut table = self.table.lock().unwrap();
        let write = table.read(req.key.clone(), Column::Write, Some(req.start_ts), None);
        if let Some((&(_, ts), _)) = write {
            return Err(PrewriteError::WriteConflict { ts });
        }
        let lock = table.read(req.key.clone(), Column::Lock, None, None);
        if let Some((&(_, ts), _)) = lock {
            return Err(PrewriteError::IsLocked { ts });
        }
        table.write(
            req.key.clone(),
            Column::Data,
            req.start_ts,
            Value::Vector(req.value),
        );
        table.write(
            req.key.clone(),
            Column::Lock,
            req.start_ts,
            Value::Vector(req.primary_key),
        );
        Ok(())
    }

    #[rpc]
    async fn commit(&self, req: CommitRequest) -> Result<(), CommitError> {
        let mut table = self.table.lock().unwrap();
        table.write(
            req.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        table.erase(req.key.clone(), Column::Lock, req.start_ts);
        Ok(())
    }

    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }
}
