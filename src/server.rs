use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use itertools::Itertools;

use crate::msg::*;

#[derive(Default, Clone)]
pub struct TimestampOracle {
    next_ts: Arc<AtomicU64>,
}

#[madsim::service]
impl TimestampOracle {
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
    /// Reads the latest key-value record from a specified column
    /// in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_range: impl RangeBounds<u64>,
    ) -> Option<(u64, &Value)> {
        let map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };
        let start = (
            key.clone(),
            match ts_range.start_bound() {
                Bound::Included(ts) => *ts,
                Bound::Excluded(ts) => *ts + 1,
                Bound::Unbounded => 0,
            },
        );
        let end = (
            key,
            match ts_range.end_bound() {
                Bound::Included(ts) => *ts,
                Bound::Excluded(ts) => *ts - 1,
                Bound::Unbounded => u64::MAX,
            },
        );
        map.range(start..=end)
            .next_back()
            .map(|((_, ts), v)| (*ts, v))
    }

    /// Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        let map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.insert((key, ts), value);
    }

    /// Erases a record from a specified column in MemoryStorage.
    #[inline]
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        let map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.remove(&(key, commit_ts));
    }

    /// Finds the write record pointing to the specific timestamp.
    /// Returns the commit timestamp.
    #[inline]
    fn find_write(&self, key: Vec<u8>, start_ts: u64) -> Option<u64> {
        self.write
            .range((key.clone(), 0)..=(key, u64::MAX))
            .find(|(_, v)| v.as_ts() == start_ts)
            .map(|((_, ts), _)| *ts)
    }
}

impl Display for KvTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = BTreeMap::<&[u8], BTreeMap<u64, (_, _, _)>>::new();
        for ((key, ts), value) in &self.data {
            map.entry(key).or_default().entry(*ts).or_default().0 = Some(value);
        }
        for ((key, ts), value) in &self.lock {
            map.entry(key).or_default().entry(*ts).or_default().1 = Some(value);
        }
        for ((key, ts), value) in &self.write {
            map.entry(key).or_default().entry(*ts).or_default().2 = Some(value);
        }

        let mut table = comfy_table::Table::new();
        table.set_header(vec!["Key", "Data", "Lock", "Write"]);
        for (key, map) in map {
            let value_to_string = |ts: u64, v: Option<&Value>| match v {
                Some(Value::Timestamp(t)) => format!("{ts}: data@{t}"),
                Some(Value::Vector(v)) => format!("{ts}: {}", String::from_utf8_lossy(v)),
                None => format!(""),
            };
            table.add_row(vec![
                String::from_utf8_lossy(key).to_string(),
                map.iter()
                    .rev()
                    .map(|(ts, (v, _, _))| value_to_string(*ts, *v))
                    .join("\n"),
                map.iter()
                    .rev()
                    .map(|(ts, (_, v, _))| value_to_string(*ts, *v))
                    .join("\n"),
                map.iter()
                    .rev()
                    .map(|(ts, (_, _, v))| value_to_string(*ts, *v))
                    .join("\n"),
            ]);
        }
        write!(f, "{table}")
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Default, Clone)]
pub struct MemoryStorage {
    table: Arc<Mutex<KvTable>>,
}

#[madsim::service]
impl MemoryStorage {
    #[rpc]
    fn get(&self, req: GetRequest) -> Result<Option<Vec<u8>>, GetError> {
        let table = self.table.lock().unwrap();
        if let Some((ts, primary)) = table.read(req.key.clone(), Column::Lock, ..=req.start_ts) {
            let primary = primary.as_bytes().to_vec();
            return Err(GetError::IsLocked { ts, primary });
        }
        let ts = match table.read(req.key.clone(), Column::Write, ..=req.start_ts) {
            Some((_, v)) => v.as_ts(),
            None => return Ok(None),
        };
        let value = table
            .read(req.key, Column::Data, ts..=ts)
            .unwrap()
            .1
            .as_bytes();
        Ok(Some(value.to_vec()))
    }

    #[rpc]
    fn prewrite(&self, req: PrewriteRequest) -> Result<(), PrewriteError> {
        let mut table = self.table.lock().unwrap();
        if let Some((ts, _)) = table.read(req.key.clone(), Column::Write, req.start_ts..) {
            return Err(PrewriteError::WriteConflict { ts });
        }
        if let Some((ts, _)) = table.read(req.key.clone(), Column::Lock, ..) {
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
        tracing::debug!("prewrite\n{}", table);
        Ok(())
    }

    #[rpc]
    fn commit(&self, req: CommitRequest) -> Result<(), CommitError> {
        let mut table = self.table.lock().unwrap();
        table.write(
            req.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        table.erase(req.key.clone(), Column::Lock, req.start_ts);
        tracing::debug!("commit\n{}", table);
        Ok(())
    }

    #[rpc]
    fn check(&self, req: CheckRequest) -> Option<u64> {
        let table = self.table.lock().unwrap();
        table.find_write(req.key, req.lock_ts)
    }

    #[rpc]
    fn rollback(&self, req: RollbackRequest) -> Result<(), RollbackError> {
        let mut table = self.table.lock().unwrap();
        table.erase(req.key, Column::Lock, req.start_ts);
        tracing::debug!("rollback\n{}", table);
        Ok(())
    }
}
