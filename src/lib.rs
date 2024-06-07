use dfut::{d_await, into_dfut, DFut, DResult, Runtime};

#[derive(Debug, Clone)]
pub struct NoOpWorker {
    runtime: Runtime,
}

#[into_dfut]
impl NoOpWorker {
    pub async fn nop_fanout(&self, n: u64, a: u64) -> DResult<()> {
        let mut d_futs = Vec::new();
        for _ in 0..n {
            let tmp_d_fut = self.nop(a).await?;
            d_futs.push(tmp_d_fut);
        }

        for d_fut in d_futs {
            d_await!(d_fut);
        }
        Ok(())
    }

    pub async fn nop(&self, a: u64) -> DResult<Vec<u8>> {
        Ok(vec![42u8; a as usize])
    }
}
