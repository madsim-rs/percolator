use madsim::{runtime::Handle, task, time};
use std::net::SocketAddr;
use std::time::Duration;

use crate::client::Client;
use crate::server::{MemoryStorage, TimestampOracle};

struct Tester {
    handle: Handle,
    tso_addr: SocketAddr,
    txn_addr: SocketAddr,
    clients: Vec<Client>,
}

impl Tester {
    async fn new(num_client: usize) -> Self {
        let handle = Handle::current();

        let tso_addr = "10.0.0.1:1".parse::<SocketAddr>().unwrap();
        let txn_addr = "10.0.0.2:1".parse::<SocketAddr>().unwrap();

        handle
            .create_node()
            .ip(tso_addr.ip())
            .init(|| async {
                TimestampOracle::default()
                    .serve("0.0.0.0:1".parse().unwrap())
                    .await
            })
            .build();
        handle
            .create_node()
            .ip(txn_addr.ip())
            .init(|| async {
                MemoryStorage::default()
                    .serve("0.0.0.0:1".parse().unwrap())
                    .await
            })
            .build();

        let mut clients = vec![];
        for _ in 0..num_client {
            clients.push(Client::new(tso_addr, txn_addr).await.unwrap());
        }
        Tester {
            handle,
            tso_addr,
            txn_addr,
            clients,
        }
    }

    fn client(&self, i: usize) -> Client {
        self.clients[i].clone()
    }

    fn enable_client(&self, i: usize) {
        todo!()
    }

    fn disable_client(&self, i: usize) {
        todo!()
    }

    fn drop_req(&self) {
        todo!()
    }
    fn drop_resp(&self) {
        todo!()
    }
    fn fail_primary(&self) {
        todo!()
    }
}

#[madsim::test]
async fn test_get_timestamp_under_unreliable_network() {
    let mut t = Tester::new(3).await;
    let mut children = vec![];

    for i in 0..3 {
        let client = t.client(i);
        t.disable_client(i);
        children.push(task::spawn(async move {
            let res = client.get_timestamp();
            // FIXME
            // if i == 2 {
            //     assert_eq!(res, Err(Error::Timeout));
            // } else {
            //     assert!(res.is_ok());
            // }
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
    let mut t = Tester::new(3).await;

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
    let mut t = Tester::new(3).await;

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
    let mut t = Tester::new(3).await;

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
    let mut t = Tester::new(3).await;

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
    let mut t = Tester::new(3).await;

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
    let mut t = Tester::new(3).await;

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
    let mut t = Tester::new(3).await;

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
    let mut t = Tester::new(4).await;

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
