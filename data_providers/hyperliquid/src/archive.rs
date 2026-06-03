use anyhow::{Context, Result};
use aws_sdk_s3::config::{Credentials, Region};
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

#[derive(Debug, Clone)]
pub struct ArchiveCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

#[derive(Clone)]
pub struct S3ArchiveClient {
    cache_dir: Option<PathBuf>,
    buckets: ArchiveBuckets,
    request_payer: String,
    region: String,
    credentials: Option<ArchiveCredentials>,
    runtime: Arc<ArchiveRuntime>,
    s3: Arc<Mutex<Option<aws_sdk_s3::Client>>>,
}

impl S3ArchiveClient {
    pub fn new(
        cache_dir: Option<PathBuf>,
        buckets: ArchiveBuckets,
        request_payer: impl Into<String>,
        region: impl Into<String>,
        credentials: Option<ArchiveCredentials>,
    ) -> Self {
        Self {
            cache_dir,
            buckets,
            request_payer: request_payer.into(),
            region: region.into(),
            credentials,
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
                    let text = format!("{error:?}");
                    if text.contains("NoSuchKey")
                        || text.contains("NotFound")
                        || text.contains("404")
                    {
                        return Ok(None);
                    }
                    return Err(anyhow::anyhow!(
                        "{}",
                        hyperliquid_s3_error_message(&bucket_name, &key_name, &text)
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

        let credentials = self.credentials.clone();
        let region = self.region.clone();
        let config = self
            .runtime
            .block_on(async move {
                let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(Region::new(region));
                if let Some(credentials) = credentials {
                    loader = loader.credentials_provider(Credentials::new(
                        credentials.access_key_id,
                        credentials.secret_access_key,
                        credentials.session_token,
                        None,
                        "rlean-hyperliquid-plugin-config",
                    ));
                }
                loader.load().await
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

fn hyperliquid_s3_error_message(bucket: &str, key: &str, error: &str) -> String {
    let credential_hint = "Hyperliquid archive S3 access uses requester-pays buckets and needs valid AWS credentials. Set them with `rlean config set hyperliquid.aws_access_key_id <access-key-id>` and `rlean config set hyperliquid.aws_secret_access_key <secret-access-key>`, or export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY before running rlean.";
    let lowered = error.to_ascii_lowercase();

    if lowered.contains("permanentredirect") || lowered.contains("x-amz-bucket-region") {
        let region = s3_bucket_region_from_error(error).unwrap_or("ap-northeast-1");
        return format!(
            "failed to download Hyperliquid archive s3://{bucket}/{key}. S3 reported a bucket-region mismatch. Set `rlean config set hyperliquid.aws_region {region}`. Underlying AWS SDK error: {error}"
        );
    }

    if lowered.contains("credentials")
        || lowered.contains("forbidden")
        || lowered.contains("accessdenied")
        || lowered.contains("access denied")
        || lowered.contains("signature")
    {
        return format!(
            "failed to download Hyperliquid archive s3://{bucket}/{key}. {credential_hint} Underlying AWS SDK error: {error}"
        );
    }

    if lowered.contains("dispatch failure") {
        return format!(
            "failed to download Hyperliquid archive s3://{bucket}/{key}. The AWS SDK could not dispatch the S3 request. Check network/DNS/TLS access and the configured Hyperliquid AWS settings. Underlying AWS SDK error: {error}"
        );
    }

    format!("failed to download Hyperliquid archive s3://{bucket}/{key}: {error}")
}

fn s3_bucket_region_from_error(error: &str) -> Option<&str> {
    let marker = "x-amz-bucket-region\": \"";
    let start = error.find(marker)? + marker.len();
    let rest = &error[start..];
    let end = rest.find('"')?;
    Some(&rest[..end])
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
    use super::{hyperliquid_s3_error_message, s3_bucket_region_from_error, ArchiveRuntime};

    #[tokio::test]
    async fn archive_runtime_runs_inside_existing_tokio_runtime() {
        let runtime = ArchiveRuntime::new();
        let value = runtime.block_on(async { 42 }).unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn s3_dispatch_failure_mentions_rlean_credential_config() {
        let message = hyperliquid_s3_error_message(
            "hl-mainnet-node-data",
            "node_fills_by_block/hourly/20250101/0.lz4",
            "dispatch failure",
        );
        assert!(message.contains("AWS SDK could not dispatch"));
        assert!(
            message.contains("s3://hl-mainnet-node-data/node_fills_by_block/hourly/20250101/0.lz4")
        );
    }

    #[test]
    fn permanent_redirect_mentions_region_config() {
        let error =
            r#"ServiceError headers: {"x-amz-bucket-region": "ap-northeast-1"} PermanentRedirect"#;
        let message = hyperliquid_s3_error_message(
            "hl-mainnet-node-data",
            "node_fills_by_block/hourly/20250101/0.lz4",
            error,
        );
        assert!(message.contains("rlean config set hyperliquid.aws_region ap-northeast-1"));
        assert_eq!(s3_bucket_region_from_error(error), Some("ap-northeast-1"));
    }
}
