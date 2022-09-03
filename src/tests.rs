use madsim::{
    net::rpc::Request,
    runtime::{Handle, NodeHandle},
    task, time,
};
use spin::Mutex;
use std::io;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use crate::client::Client;
use crate::msg;
use crate::server::{MemoryStorage, TimestampOracle};

struct Tester {
    clients: Vec<TestClient>,
    hooks: Arc<CommitHooks>,
}

#[derive(Debug, Default)]
struct CommitHooks {
    drop_req: AtomicBool,
    drop_resp: AtomicBool,
    fail_primary: AtomicBool,
}

impl CommitHooks {
    fn hook_req(&self, req: &msg::CommitRequest) -> bool {
        if self.drop_req.load(Ordering::Relaxed) {
            if !req.is_primary || self.fail_primary.load(Ordering::Relaxed) {
                tracing::debug!("drop a commit request");
                return false;
            }
        }
        true
    }

    fn hook_rsp(&self, _: &<msg::CommitRequest as Request>::Response) -> bool {
        if self.drop_resp.load(Ordering::Relaxed) {
            tracing::debug!("drop a commit response");
            return false;
        }
        true
    }
}

impl Tester {
    async fn new(num_client: usize) -> Self {
        let handle = Handle::current();

        let tso_addr = "10.0.1.1:1".parse::<SocketAddr>().unwrap();
        let txn_addr = "10.0.1.2:1".parse::<SocketAddr>().unwrap();

        handle
            .create_node()
            .name("tso")
            .ip(tso_addr.ip())
            .init(move || TimestampOracle::default().serve(tso_addr))
            .build();
        handle
            .create_node()
            .name("txn")
            .ip(txn_addr.ip())
            .init(move || MemoryStorage::default().serve(txn_addr))
            .build();

        let net = madsim::net::NetSim::current();
        let hooks = Arc::new(CommitHooks::default());
        let mut clients = vec![];
        for i in 1..=num_client {
            let node = handle
                .create_node()
                .name(format!("client-{i}"))
                .ip([10, 0, 0, i as u8].into())
                .build();
            let client = Arc::new(Mutex::new(
                node.spawn(Client::new(tso_addr, txn_addr))
                    .await
                    .unwrap()
                    .expect("failed to create client"),
            ));
            let hooks1 = hooks.clone();
            let hooks2 = hooks.clone();
            net.hook_rpc_req(node.id(), move |req| hooks1.hook_req(req));
            net.hook_rpc_rsp(node.id(), move |rsp| hooks2.hook_rsp(rsp));
            clients.push(TestClient { node, client });
        }
        Tester { clients, hooks }
    }

    fn client(&self, i: usize) -> TestClient {
        self.clients[i].clone()
    }

    fn enable_client(&self, i: usize) {
        tracing::info!(i, "enable client");
        let net = madsim::net::NetSim::current();
        net.unclog_node(self.clients[i].node.id());
    }

    fn disable_client(&self, i: usize) {
        tracing::info!(i, "disable client");
        let net = madsim::net::NetSim::current();
        net.clog_node(self.clients[i].node.id());
    }

    fn drop_req(&self) {
        tracing::info!("set drop request");
        self.hooks.drop_req.store(true, Ordering::Relaxed);
    }

    fn drop_resp(&self) {
        tracing::info!("set drop response");
        self.hooks.drop_resp.store(true, Ordering::Relaxed);
    }

    fn fail_primary(&self) {
        tracing::info!("set fail primary");
        self.hooks.fail_primary.store(true, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct TestClient {
    node: NodeHandle,
    client: Arc<Mutex<Client>>,
}

impl TestClient {
    async fn get_timestamp(&self) -> io::Result<u64> {
        let client = self.client.clone();
        self.node
            .spawn(async move { client.lock().get_timestamp().await })
            .await
            .unwrap()
    }
    async fn begin(&mut self) {
        let client = self.client.clone();
        self.node
            .spawn(async move { client.lock().begin().await })
            .await
            .unwrap()
    }
    async fn get(&self, key: &[u8]) -> io::Result<Vec<u8>> {
        let client = self.client.clone();
        let key = key.to_vec();
        self.node
            .spawn(async move { client.lock().get(&key).await })
            .await
            .unwrap()
    }
    async fn set(&mut self, key: &[u8], value: &[u8]) {
        let client = self.client.clone();
        let key = key.to_vec();
        let value = value.to_vec();
        self.node
            .spawn(async move { client.lock().set(&key, &value).await })
            .await
            .unwrap()
    }
    async fn commit(&self) -> io::Result<bool> {
        let client = self.client.clone();
        self.node
            .spawn(async move { client.lock().commit().await })
            .await
            .unwrap()
    }
}

#[madsim::test]
async fn test_get_timestamp_under_unreliable_network() {
    let t = Tester::new(3).await;
    let mut children = vec![];

    for i in 0..3 {
        let client = t.client(i);
        t.disable_client(i);
        children.push(task::spawn(async move {
            let res = client.get_timestamp().await;
            if i == 2 {
                assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
            } else {
                assert!(res.is_ok());
            }
        }));
    }

    time::sleep(Duration::from_millis(100)).await;
    t.enable_client(0);
    time::sleep(Duration::from_millis(200)).await;
    t.enable_client(1);
    time::sleep(Duration::from_millis(400)).await;
    t.enable_client(2);

    for child in children {
        child.await.unwrap();
    }
}

// https://github.com/ept/hermitage/blob/master/sqlserver.md#predicate-many-preceders-pmp
#[madsim::test]
async fn test_predicate_many_preceders_read_predicates() {
    let t = Tester::new(3).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"1", b"10").await;
    client0.set(b"2", b"20").await;
    assert_eq!(client0.commit().await.unwrap(), true);

    let mut client1 = t.client(1);
    client1.begin().await;
    assert_eq!(client1.get(b"3").await.unwrap(), b"");

    let mut client2 = t.client(2);
    client2.begin().await;
    client2.set(b"3", b"30").await;
    assert_eq!(client2.commit().await.unwrap(), true);

    assert_eq!(client1.get(b"3").await.unwrap(), b"");
}

// https://github.com/ept/hermitage/blob/master/sqlserver.md#predicate-many-preceders-pmp
#[madsim::test]
async fn test_predicate_many_preceders_write_predicates() {
    let t = Tester::new(3).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"1", b"10").await;
    client0.set(b"2", b"20").await;
    assert_eq!(client0.commit().await.unwrap(), true);

    let mut client1 = t.client(1);
    client1.begin().await;

    let mut client2 = t.client(2);
    client2.begin().await;

    client1.set(b"1", b"20").await;
    client1.set(b"2", b"30").await;
    assert_eq!(client1.get(b"2").await.unwrap(), b"20");

    client2.set(b"2", b"40").await;
    assert_eq!(client1.commit().await.unwrap(), true);
    assert_eq!(client2.commit().await.unwrap(), false);
}

// https://github.com/ept/hermitage/blob/master/sqlserver.md#lost-update-p4
#[madsim::test]
async fn test_lost_update() {
    let t = Tester::new(3).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"1", b"10").await;
    client0.set(b"2", b"20").await;
    assert_eq!(client0.commit().await.unwrap(), true);

    let mut client1 = t.client(1);
    client1.begin().await;

    let mut client2 = t.client(2);
    client2.begin().await;

    assert_eq!(client1.get(b"1").await.unwrap(), b"10");
    assert_eq!(client2.get(b"1").await.unwrap(), b"10");

    client1.set(b"1", b"11").await;
    client2.set(b"1", b"11").await;
    assert_eq!(client1.commit().await.unwrap(), true);
    assert_eq!(client2.commit().await.unwrap(), false);
}

// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
#[madsim::test]
async fn test_read_skew_read_only() {
    let t = Tester::new(3).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"1", b"10").await;
    client0.set(b"2", b"20").await;
    assert_eq!(client0.commit().await.unwrap(), true);

    let mut client1 = t.client(1);
    client1.begin().await;

    let mut client2 = t.client(2);
    client2.begin().await;

    assert_eq!(client1.get(b"1").await.unwrap(), b"10");
    assert_eq!(client2.get(b"1").await.unwrap(), b"10");
    assert_eq!(client2.get(b"2").await.unwrap(), b"20");

    client2.set(b"1", b"12").await;
    client2.set(b"2", b"18").await;
    assert_eq!(client2.commit().await.unwrap(), true);

    assert_eq!(client1.get(b"2").await.unwrap(), b"20");
}

// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
#[madsim::test]
async fn test_read_skew_predicate_dependencies() {
    let t = Tester::new(3).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"1", b"10").await;
    client0.set(b"2", b"20").await;
    assert_eq!(client0.commit().await.unwrap(), true);

    let mut client1 = t.client(1);
    client1.begin().await;

    let mut client2 = t.client(2);
    client2.begin().await;

    assert_eq!(client1.get(b"1").await.unwrap(), b"10");
    assert_eq!(client1.get(b"2").await.unwrap(), b"20");

    client2.set(b"3", b"30").await;
    assert_eq!(client2.commit().await.unwrap(), true);

    assert_eq!(client1.get(b"3").await.unwrap(), b"");
}

// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
#[madsim::test]
async fn test_read_skew_write_predicate() {
    let t = Tester::new(3).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"1", b"10").await;
    client0.set(b"2", b"20").await;
    assert_eq!(client0.commit().await.unwrap(), true);

    let mut client1 = t.client(1);
    client1.begin().await;

    let mut client2 = t.client(2);
    client2.begin().await;

    assert_eq!(client1.get(b"1").await.unwrap(), b"10");
    assert_eq!(client2.get(b"1").await.unwrap(), b"10");
    assert_eq!(client2.get(b"2").await.unwrap(), b"20");

    client2.set(b"1", b"12").await;
    client2.set(b"2", b"18").await;
    assert_eq!(client2.commit().await.unwrap(), true);

    client1.set(b"2", b"30").await;
    assert_eq!(client1.commit().await.unwrap(), false);
}

// https://github.com/ept/hermitage/blob/master/sqlserver.md#write-skew-g2-item
#[madsim::test]
async fn test_write_skew() {
    let t = Tester::new(3).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"1", b"10").await;
    client0.set(b"2", b"20").await;
    assert_eq!(client0.commit().await.unwrap(), true);

    let mut client1 = t.client(1);
    client1.begin().await;

    let mut client2 = t.client(2);
    client2.begin().await;

    assert_eq!(client1.get(b"1").await.unwrap(), b"10");
    assert_eq!(client1.get(b"2").await.unwrap(), b"20");
    assert_eq!(client2.get(b"1").await.unwrap(), b"10");
    assert_eq!(client2.get(b"2").await.unwrap(), b"20");

    client1.set(b"1", b"11").await;
    client2.set(b"2", b"21").await;

    assert_eq!(client1.commit().await.unwrap(), true);
    assert_eq!(client2.commit().await.unwrap(), true);
}

// https://github.com/ept/hermitage/blob/master/sqlserver.md#anti-dependency-cycles-g2
#[madsim::test]
async fn test_anti_dependency_cycles() {
    let t = Tester::new(4).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"1", b"10").await;
    client0.set(b"2", b"20").await;
    assert_eq!(client0.commit().await.unwrap(), true);

    let mut client1 = t.client(1);
    client1.begin().await;

    let mut client2 = t.client(2);
    client2.begin().await;

    client1.set(b"3", b"30").await;
    client2.set(b"4", b"42").await;

    assert_eq!(client1.commit().await.unwrap(), true);
    assert_eq!(client2.commit().await.unwrap(), true);

    let mut client3 = t.client(3);
    client3.begin().await;
    assert_eq!(client3.get(b"3").await.unwrap(), b"30");
    assert_eq!(client3.get(b"4").await.unwrap(), b"42");
}

#[madsim::test]
async fn test_commit_primary_drop_secondary_requests() {
    let t = Tester::new(2).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"3", b"30").await;
    client0.set(b"4", b"40").await;
    client0.set(b"5", b"50").await;
    t.drop_req();
    assert_eq!(client0.commit().await.unwrap(), true);

    let mut client1 = t.client(1);
    client1.begin().await;
    assert_eq!(client1.get(b"3").await.unwrap(), b"30");
    assert_eq!(client1.get(b"4").await.unwrap(), b"40");
    assert_eq!(client1.get(b"5").await.unwrap(), b"50");
}

#[madsim::test]
async fn test_commit_primary_success() {
    let t = Tester::new(2).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"3", b"30").await;
    client0.set(b"4", b"40").await;
    client0.set(b"5", b"50").await;
    t.drop_req();
    assert_eq!(client0.commit().await.unwrap(), true);

    let mut client1 = t.client(1);
    client1.begin().await;
    assert_eq!(client1.get(b"3").await.unwrap(), b"30");
    assert_eq!(client1.get(b"4").await.unwrap(), b"40");
    assert_eq!(client1.get(b"5").await.unwrap(), b"50");
}

#[madsim::test]
async fn test_commit_primary_success_without_response() {
    let t = Tester::new(2).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"3", b"30").await;
    client0.set(b"4", b"40").await;
    client0.set(b"5", b"50").await;
    t.drop_resp();
    assert!(client0.commit().await.is_err());

    let mut client1 = t.client(1);
    client1.begin().await;
    assert_eq!(client1.get(b"3").await.unwrap(), b"30");
    assert_eq!(client1.get(b"4").await.unwrap(), b"40");
    assert_eq!(client1.get(b"5").await.unwrap(), b"50");
}

#[madsim::test]
async fn test_commit_primary_fail() {
    let t = Tester::new(2).await;

    let mut client0 = t.client(0);
    client0.begin().await;
    client0.set(b"3", b"30").await;
    client0.set(b"4", b"40").await;
    client0.set(b"5", b"50").await;
    t.drop_req();
    t.fail_primary();
    assert_eq!(client0.commit().await.unwrap(), false);

    let mut client1 = t.client(1);
    client1.begin().await;
    assert_eq!(client1.get(b"3").await.unwrap(), b"");
    assert_eq!(client1.get(b"4").await.unwrap(), b"");
    assert_eq!(client1.get(b"5").await.unwrap(), b"");
}
