use dfut::{
    d_await, d_cancel, into_dfut, DFut, DResult, GlobalScheduler, GlobalSchedulerCfg, Runtime,
    WorkerServerConfig,
};

#[derive(Debug, Clone)]
pub struct Worker {
    runtime: Runtime,
}

#[into_dfut]
impl Worker {
    pub async fn all_reduce(&self, v: Vec<f64>) -> DResult<Vec<f64>> {
        let mut f = Vec::new();
        for chunk in v.chunks(1000) {
            let tmp = self.do_work(chunk.to_vec()).await?;
            f.push(self.reduce(tmp).await?);
        }

        let n = f.len();
        let f = self.shuffle(f).await?;

        let fs = self.runtime.share_n(&f, n as u64).await?;

        let mut out = Vec::new();
        for v in fs {
            out.push(d_await!(self.do_work2(v).await?));
        }
        d_cancel!(f);
        Ok(out)
    }

    pub async fn do_work(&self, v: Vec<f64>) -> DResult<Vec<f64>> {
        Ok(v)
    }

    pub async fn reduce(&self, v: DFut<Vec<f64>>) -> DResult<f64> {
        let mut sum = 0.;
        for v in d_await!(v) {
            sum += v;
        }
        Ok(sum)
    }

    pub async fn shuffle(&self, v: Vec<DFut<f64>>) -> DResult<f64> {
        let mut sum = 0.;
        for v in v {
            sum += d_await!(v);
        }
        Ok(sum)
    }

    pub async fn do_work2(&self, v: DFut<f64>) -> DResult<f64> {
        let v = d_await!(v);
        Ok(v)
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .unwrap();

    let global_scheduler_address = "http://127.0.0.1:8220";

    tokio::spawn(GlobalScheduler::serve_forever(GlobalSchedulerCfg {
        address: global_scheduler_address.to_string(),
        ..Default::default()
    }));

    (1..=9).for_each(|i| {
        tokio::spawn(Worker::serve(WorkerServerConfig {
            local_server_address: format!("http://127.0.0.1:812{i}"),
            global_scheduler_address: global_scheduler_address.to_string(),
            ..Default::default()
        }));
    });

    let root_client = WorkerRootClient::new(&global_scheduler_address, "unique-id").await;
    let client = root_client.new_client();

    let fut = client
        .all_reduce((0..100_000_000).map(|v| v as f64).collect())
        .await
        .unwrap();
    let _output = client.d_await(fut).await.unwrap();

    println!();
    println!("metrics");
    println!("{}", prometheus_handle.render());
}
