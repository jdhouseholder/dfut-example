use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use dfut::{d_await, into_dfut, DFut, DResult, GlobalScheduler, Runtime, WorkerServerConfig};
use rand::seq::SliceRandom;

static SUCCEED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone)]
pub struct Worker {
    runtime: Runtime,
}

#[into_dfut]
impl Worker {
    // Foo Bar.
    pub async fn foo(&self, a: Vec<String>) -> DResult<String> {
        Ok(a.join(" ").to_string())
    }

    pub async fn bar(&self, a: usize, b: DFut<String>) -> DResult<String> {
        let b = d_await!(b);
        Ok((b + " ").repeat(a).trim().to_string())
    }

    pub async fn foo_bar(&self, a: usize) -> DResult<String> {
        let v = vec!["hello".to_string(), "world".to_string()];
        let sf = self.foo(v).await?;
        let b: DFut<String> = self.bar(a, sf).await?;
        Ok(d_await!(b))
    }

    // Sort. Inspired by: https://docs.ray.io/en/latest/ray-core/patterns/nested-tasks.html.
    pub async fn partition(&self, mut v: Vec<u64>) -> DResult<(Vec<u64>, u64, Vec<u64>)> {
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

        Ok((l, p, g))
    }

    pub async fn quick_sort(&self, mut v: Vec<u64>) -> DResult<Vec<u64>> {
        if v.len() < 200_000 {
            v.sort();
            return Ok(v);
        }
        let (l, p, g) = d_await!(self.partition(v).await?);
        let l = d_await!(self.quick_sort(l).await?);
        let g = d_await!(self.quick_sort(g).await?);
        let mut out = Vec::new();
        out.extend(l);
        out.push(p);
        out.extend(g);
        Ok(out)
    }

    // Supervisor. Inspired by:
    // https://docs.ray.io/en/latest/ray-core/patterns/tree-of-actors.html.
    pub async fn supervised_train(&self, hyperparam: f64, data: Vec<f64>) -> DResult<Vec<f64>> {
        let mut v = Vec::new();
        for d in data {
            v.push(self.train(hyperparam, d).await?);
        }

        let mut o = Vec::new();
        for f in v {
            o.push(d_await!(f));
        }

        Ok(o)
    }

    pub async fn train(&self, hyperparam: f64, data: f64) -> DResult<f64> {
        Ok(hyperparam * data)
    }

    pub async fn reconstruction(&self, v: u64) -> DResult<u64> {
        // Since we don't retry from the driver and we don't retry on the
        // current worker, we have the parent retry. We need one level of
        // indirection.
        let v = d_await!(self.retried_f(v).await?);
        Ok(v)
    }

    pub async fn retried_f(&self, v: u64) -> DResult<u64> {
        let succ = SUCCEED.fetch_or(true, Ordering::SeqCst);
        if !succ {
            return Err(dfut::Error::System);
        }
        Ok(2 * v)
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .unwrap();

    let global_scheduler_address = "http://127.0.0.1:8120";

    tokio::spawn(GlobalScheduler::serve(
        global_scheduler_address,
        vec![],
        Duration::from_secs(5),
    ));

    (1..=9).for_each(|i| {
        tokio::spawn(Worker::serve(WorkerServerConfig {
            local_server_address: format!("http://127.0.0.1:812{i}"),
            global_scheduler_address: global_scheduler_address.to_string(),
            ..Default::default()
        }));
    });

    let root_client = WorkerRootClient::new(&global_scheduler_address).await;
    let client = root_client.new_client();

    // Foo Bar.
    {
        let mut d_futs = Vec::new();

        for _ in 0..10 {
            let d_fut = client.foo_bar(2).await.unwrap();
            d_futs.push(d_fut);
        }

        for d_fut in d_futs {
            let v = client.d_await(d_fut).await.unwrap();
            assert_eq!(v, "hello world hello world");
        }
    }

    // Sort.
    {
        for size in [
            200_000, 400_000, 800_000, 1_600_000, 3_200_000, 6_400_000, 12_800_000,
        ] {
            println!("shuffle with size={size}");
            let mut v: Vec<u64> = (0..size).collect();
            v.shuffle(&mut rand::thread_rng());

            let mut want = v.clone();
            let start = Instant::now();
            want.sort();
            let elapsed = start.elapsed();
            println!("local: took={elapsed:?}");

            let start = Instant::now();
            let f = client.quick_sort(v.clone()).await.unwrap();
            let got = client.d_await(f).await.unwrap();
            let elapsed = start.elapsed();
            println!("distributed: took={elapsed:?}");

            assert_eq!(got, want);
        }
    }

    // Supervisor.
    {
        let data = vec![1., 2., 3.];
        let s1 = client.supervised_train(1., data.clone()).await.unwrap();
        let s2 = client.supervised_train(2., data.clone()).await.unwrap();

        let model1 = client.d_await(s1).await.unwrap();
        assert_eq!(model1, vec![1., 2., 3.]);

        let model2 = client.d_await(s2).await.unwrap();
        assert_eq!(model2, vec![2., 4., 6.]);
    }

    // Reconstruction.
    {
        let x = 42;
        let f = client.reconstruction(x).await.unwrap();
        let y = client.d_await(f).await.unwrap();
        assert_eq!(y, 2 * x);
    }

    println!();
    println!("metrics");
    println!("{}", prometheus_handle.render());
}
