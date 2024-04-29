use dfut::{d_await, into_dfut, DFut, GlobalScheduler, Runtime, WorkerServerConfig};

#[derive(Debug, Clone)]
pub struct Worker {
    runtime: Runtime,
}

#[into_dfut]
impl Worker {
    pub async fn foo(&self, a: Vec<String>) -> String {
        a.join(" ").to_string()
    }

    pub async fn bar(&self, a: usize, b: DFut<String>) -> String {
        let b = d_await!(b);
        (b + " ").repeat(a).trim().to_string()
    }

    pub async fn foo_bar(&self, a: usize) -> String {
        let v = vec!["hello".to_string(), "world".to_string()];
        let sf = self.foo(v).await;
        let b: DFut<String> = self.bar(a, sf).await;
        d_await!(b)
    }
}

#[tokio::main]
async fn main() {
    let global_scheduler_address = "http://127.0.0.1:8120";

    tokio::spawn(GlobalScheduler::serve(global_scheduler_address));

    (1..=3).for_each(|i| {
        tokio::spawn(Worker::serve(WorkerServerConfig {
            local_server_address: format!("http://127.0.0.1:812{i}"),
            global_scheduler_address: global_scheduler_address.to_string(),
            ..Default::default()
        }));
    });

    let client = WorkerClient::new(&global_scheduler_address).await;

    let mut d_futs = Vec::new();

    for _ in 0..10 {
        let d_fut = client.foo_bar(2).await;
        d_futs.push(d_fut);
    }

    for d_fut in d_futs {
        let v = client.d_await(d_fut).await.unwrap();
        assert_eq!(v, "hello world hello world");
    }
}
