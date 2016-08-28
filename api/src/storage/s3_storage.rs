use std::error::Error;
use std::io::Read;

use rusoto::{DefaultCredentialsProvider, Region};
use rusoto::s3::{S3Client, ListObjectsRequest};
use rustc_serialize::hex::ToHex;

use {Node, Storage};

pub struct S3Storage {
    // region: String,
    bucket: String,
    prefix: String,
}

impl S3Storage {
    pub fn new() -> Self {
        S3Storage {
            bucket: "haumaru-test".to_string(),
            prefix: "test".to_string(),
        }
    }
}

impl Storage for S3Storage {
    fn send(&self, hash: &[u8], _ins: Box<Read>) -> Result<(), Box<Error>> {
        let hex = hash.to_hex();
        let key = format!("{}/{}/{}/{}",
                          self.prefix,
                          &hex[0..2],
                          &hex[2..4],
                          &hex[4..]);

        debug!("Using s3://{}/{}", self.bucket, key);

        use rusoto::EnvironmentProvider;

        // let provider = DefaultCredentialsProvider::new().expect("AWS Credentials");
        let client = S3Client::new(EnvironmentProvider {}, Region::UsEast1);

        let mut lor = ListObjectsRequest::default();
        lor.bucket = self.bucket.clone();
        lor.prefix = Some(key.clone());
        lor.max_keys = Some(5);
        debug!("Sending: {:?}", lor);

        let response = client.list_objects(&lor)?;
        if !response.contents.is_empty() {
            return Ok(());
        }

        info!("Uploading s3://{}/{}", self.bucket, key);

        Ok(())
    }
    fn retrieve(&self, _hash: &[u8]) -> Result<Option<Box<Read>>, Box<Error>> {
        use std::io::Cursor;
        Ok(Some(box Cursor::new(vec![])))
    }
    fn verify(&self, n: Node) -> Result<Option<Node>, Box<Error>> {
        Ok(Some(n))
    }
}
