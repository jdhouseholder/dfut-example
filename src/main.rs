use std::time::Instant;

use dfut::{d_await, into_dfut, DFut, GlobalScheduler, Runtime, WorkerServerConfig};
use rand::seq::SliceRandom;

#[derive(Debug, Clone)]
pub struct Worker {
    runtime: Runtime,
}

#[into_dfut]
impl Worker {
    // Foo Bar.
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

    // Sort.
    pub async fn partition(&self, mut v: Vec<u64>) -> (Vec<u64>, u64, Vec<u64>) {
        let p = v.pop().unwrap();
        let mut l = Vec::new();
        let mut g = Vec::new();
        for e in v {
            if e > p {
                g.push(e);
            } else {
                l.push(e);
            }
        }

        (l, p, g)
    }

    pub async fn quick_sort(&self, mut v: Vec<u64>) -> Vec<u64> {
        if v.len() < 200_000 {
            v.sort();
            return v;
        }
        let (l, p, g) = d_await!(self.partition(v).await);
        let l = d_await!(self.quick_sort(l).await);
        let g = d_await!(self.quick_sort(g).await);
        let mut out = Vec::new();
        out.extend(l);
        out.push(p);
        out.extend(g);
        out
    }

    // Supervisor.
    pub async fn supervised_train(&self, hyperparam: f64, data: Vec<f64>) -> Vec<f64> {
        let mut v = Vec::new();
        for d in data {
            v.push(self.train(hyperparam, d).await);
        }

        let mut o = Vec::new();
        for f in v {
            o.push(d_await!(f));
        }

        o
    }

    pub async fn train(&self, hyperparam: f64, data: f64) -> f64 {
        hyperparam * data
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

    // Foo Bar.
    {
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

    // Sort.
    {
        for size in [200_000, 400_000, 800_000] {
            let mut v: Vec<u64> = (0..size).collect();
            v.shuffle(&mut rand::thread_rng());

            let start = Instant::now();
            v.sort();
            let elapsed = start.elapsed();
            let _sorted = v;
            println!("local: size={size} took={elapsed:?}");

            let mut v: Vec<u64> = (0..size).collect();
            v.shuffle(&mut rand::thread_rng());

            let start = Instant::now();
            let _sorted = client.d_await(client.quick_sort(v).await).await.unwrap();
            let elapsed = start.elapsed();
            println!("distributed: size={size} took={elapsed:?}");
        }
    }

    // Supervisor.
    {
        let data = vec![1., 2., 3.];
        let s1 = client.supervised_train(1., data.clone()).await;
        let s2 = client.supervised_train(2., data.clone()).await;

        let model1 = client.d_await(s1).await.unwrap();
        assert_eq!(model1, vec![1., 2., 3.]);

        let model2 = client.d_await(s2).await.unwrap();
        assert_eq!(model2, vec![2., 4., 6.]);
    }
}
