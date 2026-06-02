use anyhow::{Context, Result};
use aws_sdk_s3::types::RequestPayer;
use lz4_flex::frame::FrameDecoder;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::OnceCell;

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
    s3: Arc<OnceCell<aws_sdk_s3::Client>>,
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
            s3: Arc::new(OnceCell::new()),
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
        let client = self
            .s3
            .get_or_try_init(|| async {
                let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                Ok::<_, anyhow::Error>(aws_sdk_s3::Client::new(&config))
            })
            .await?;

        let mut request = client.get_object().bucket(bucket).key(key);
        if self.request_payer.eq_ignore_ascii_case("requester") {
            request = request.request_payer(RequestPayer::Requester);
        }

        let object = match request.send().await {
            Ok(object) => object,
            Err(error) => {
                let text = error.to_string();
                if text.contains("NoSuchKey") || text.contains("NotFound") || text.contains("404") {
                    return Ok(None);
                }
                return Err(anyhow::anyhow!(
                    "failed to download s3://{bucket}/{key}: {error}"
                ));
            }
        };

        let bytes = object
            .body
            .collect()
            .await
            .with_context(|| format!("failed to read s3://{bucket}/{key} body"))?
            .into_bytes()
            .to_vec();
        Ok(Some(bytes))
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
