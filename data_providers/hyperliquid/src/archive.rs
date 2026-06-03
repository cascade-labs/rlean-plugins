use anyhow::{Context, Result};
use aws_sdk_s3::types::RequestPayer;
use lz4_flex::frame::FrameDecoder;
use std::future::Future;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};

#[derive(Debug, Clone)]
pub struct ArchiveBuckets {
    pub market: String,
    pub fills: String,
}

#[derive(Clone)]
pub struct S3ArchiveClient {
    cache_dir: Option<PathBuf>,
    buckets: ArchiveBuckets,
    request_payer: String,
    runtime: Arc<ArchiveRuntime>,
    s3: Arc<Mutex<Option<aws_sdk_s3::Client>>>,
}

impl S3ArchiveClient {
    pub fn new(
        cache_dir: Option<PathBuf>,
        buckets: ArchiveBuckets,
        request_payer: impl Into<String>,
    ) -> Self {
        Self {
            cache_dir,
            buckets,
            request_payer: request_payer.into(),
            runtime: Arc::new(ArchiveRuntime::new()),
            s3: Arc::new(Mutex::new(None)),
        }
    }

    pub fn buckets(&self) -> &ArchiveBuckets {
        &self.buckets
    }

    pub async fn market_text(&self, key: &str) -> Result<Option<String>> {
        self.object_text(&self.buckets.market, key).await
    }

    pub async fn fills_text(&self, key: &str) -> Result<Option<String>> {
        self.object_text(&self.buckets.fills, key).await
    }

    async fn object_text(&self, bucket: &str, key: &str) -> Result<Option<String>> {
        let bytes = if let Some(path) = self.cache_path(bucket, key).filter(|path| path.exists()) {
            std::fs::read(&path).with_context(|| {
                format!(
                    "failed to read cached Hyperliquid source archive {}",
                    path.display()
                )
            })?
        } else {
            let Some(bytes) = self.download_object(bucket, key).await? else {
                return Ok(None);
            };
            bytes
        };

        decode_lz4_text(&bytes)
            .with_context(|| format!("failed to decode archive s3://{bucket}/{key}"))
            .map(Some)
    }

    fn cache_path(&self, bucket: &str, key: &str) -> Option<PathBuf> {
        self.cache_dir
            .as_ref()
            .map(|cache_dir| cache_dir.join(bucket).join(key))
    }

    async fn download_object(&self, bucket: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let client = self.s3_client()?;
        let bucket_name = bucket.to_string();
        let key_name = key.to_string();

        let mut request = client
            .get_object()
            .bucket(bucket_name.clone())
            .key(key_name.clone());
        if self.request_payer.eq_ignore_ascii_case("requester") {
            request = request.request_payer(RequestPayer::Requester);
        }

        self.runtime.block_on(async move {
            let object = match request.send().await {
                Ok(object) => object,
                Err(error) => {
                    let text = error.to_string();
                    if text.contains("NoSuchKey")
                        || text.contains("NotFound")
                        || text.contains("404")
                    {
                        return Ok(None);
                    }
                    return Err(anyhow::anyhow!(
                        "failed to download s3://{bucket_name}/{key_name}: {error}"
                    ));
                }
            };

            let bytes = object
                .body
                .collect()
                .await
                .with_context(|| format!("failed to read s3://{bucket_name}/{key_name} body"))?
                .into_bytes()
                .to_vec();
            Ok(Some(bytes))
        })?
    }

    fn s3_client(&self) -> Result<aws_sdk_s3::Client> {
        let mut guard = self
            .s3
            .lock()
            .map_err(|_| anyhow::anyhow!("Hyperliquid S3 client lock poisoned"))?;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let config = self
            .runtime
            .block_on(async {
                aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await
            })
            .context("failed to load AWS SDK config for Hyperliquid archive")?;
        let client = aws_sdk_s3::Client::new(&config);
        *guard = Some(client.clone());
        Ok(client)
    }
}

struct ArchiveRuntime {
    inner: Option<tokio::runtime::Runtime>,
}

impl ArchiveRuntime {
    fn new() -> Self {
        Self {
            inner: Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("failed to create Hyperliquid archive Tokio runtime"),
            ),
        }
    }

    fn block_on<F>(&self, future: F) -> Result<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let runtime = self
            .inner
            .as_ref()
            .context("Hyperliquid archive runtime already shut down")?;
        let (sender, receiver) = mpsc::sync_channel(1);
        runtime.spawn(async move {
            let output = future.await;
            let _ = sender.send(output);
        });
        receiver
            .recv()
            .context("Hyperliquid archive runtime task failed before returning")
    }
}

impl Drop for ArchiveRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.inner.take() {
            runtime.shutdown_background();
        }
    }
}

fn decode_lz4_text(bytes: &[u8]) -> Result<String> {
    let mut decoder = FrameDecoder::new(bytes);
    let mut text = String::new();
    decoder.read_to_string(&mut text)?;
    Ok(text)
}

#[cfg(test)]
pub(crate) fn encode_lz4_text(text: &str) -> Result<Vec<u8>> {
    use std::io::Write;
    let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
    encoder.write_all(text.as_bytes())?;
    Ok(encoder.finish()?)
}

#[cfg(test)]
mod tests {
    use super::ArchiveRuntime;

    #[tokio::test]
    async fn archive_runtime_runs_inside_existing_tokio_runtime() {
        let runtime = ArchiveRuntime::new();
        let value = runtime.block_on(async { 42 }).unwrap();
        assert_eq!(value, 42);
    }
}
