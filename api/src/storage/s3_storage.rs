

use {Node, Storage};

use chrono::*;
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::sha2::Sha256;
use engine::EngineConfig;
use hasher::Hasher;
use hyper;
use hyper::Url;
use hyper::client::*;
use hyper::header::Headers;
use hyper::method::Method;
use regex::Regex;
use rustc_serialize::base64;
use rustc_serialize::base64::{CharacterSet, Newline, ToBase64};
use rustc_serialize::hex::ToHex;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::{Read, Write};
use storage::SendRequest;
// use retry::retry_forever;

pub struct S3Storage {
    // region: String,
    bucket: String,
    prefix: String,
    access_key: String,
    secret_key: String,
    client: Client,
}

fn new_client() -> Client {
    let mut client = Client::new();
    client.set_redirect_policy(RedirectPolicy::FollowNone);
    client
}

impl Clone for S3Storage {
    fn clone(&self) -> Self {
        S3Storage {
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            client: new_client(),
        }
    }
}

impl S3Storage {
    pub fn new(config: EngineConfig) -> Self {
        S3Storage {
            bucket: config.bucket().map(|s| s.to_string()).expect("S3 bucket"),
            prefix: config.prefix().map(|s| s.to_string()).unwrap_or(String::new()),
            access_key: env::var("AWS_ACCESS_KEY_ID")
                .expect("AWS_ACCESS_KEY_ID")
                .into(),
            secret_key: env::var("AWS_SECRET_ACCESS_KEY")
                .expect("AWS_SECRET_ACCESS_KEY")
                .into(),
            client: new_client(),
        }
    }
}

struct AwsSignature {
    access_key: String,
    secret_key: String,
    method: String,
    service: String,
    host: String,
    region: String,
    amzdate: String,
    datestamp: String,
    canonical_uri: String,
    canonical_querystring: String,
    payload_hash: String,
    headers: HashMap<String, String>,
}

impl AwsSignature {
    fn signed_headers(&self) -> Headers {

        let mut headers_to_sign = self.headers.clone();
        headers_to_sign.insert("Host".into(), self.host.clone());
        headers_to_sign.insert("X-Amz-Date".into(), self.amzdate.clone());

        let mut header_keys: Vec<&String> = headers_to_sign.keys().collect();
        header_keys.sort();

        let mut canonical_headers = String::new();
        let mut signed_headers = String::new();
        for hdr in header_keys {
            canonical_headers = format!("{}{}:{}\n",
                                        canonical_headers,
                                        hdr.to_lowercase(),
                                        headers_to_sign.get(hdr).expect("hdr"));
            if !signed_headers.is_empty() {
                signed_headers = format!("{};", signed_headers);
            }
            signed_headers = format!("{}{}", signed_headers, hdr.to_lowercase());
        }
        debug!("canonical_header={}", canonical_headers);
        debug!("signed_headers={}", signed_headers);

        // Step 1 - Create a canonical request
        // let canonical_headers = format!("host:{}\nx-amz-date:{}\n", self.host, self.amzdate);
        // let signed_headers = "host;x-amz-date";

        let canonical_request = format!("{}\n{}\n{}\n{}\n{}\n{}",
                                        self.method,
                                        self.canonical_uri,
                                        self.canonical_querystring,
                                        canonical_headers,
                                        signed_headers,
                                        self.payload_hash);
        debug!("canonical_request:\n{}", canonical_request);

        // Step 2 - Create the string to sign
        let algorithm = "AWS4-HMAC-SHA256";
        let credential_scope = format!("{}/{}/{}/aws4_request",
                                       self.datestamp,
                                       self.region,
                                       self.service);
        let string_to_sign = format!("{}\n{}\n{}\n{}",
                                     algorithm,
                                     self.amzdate,
                                     credential_scope,
                                     sha256(&canonical_request).to_hex());
        // debug!("string_to_sign:\n{}", string_to_sign);

        // Step 3 - Calculate the signature
        let signing_key = get_signature_key(self.secret_key.clone(),
                                            self.datestamp.clone(),
                                            self.region.clone(),
                                            self.service.clone());

        let signature = sign(signing_key, string_to_sign).to_hex();
        // debug!("signature: {}", signature);

        // Step 4 - Add signing information to the request
        let authorization_header = format!("{} Credential={}/{}, SignedHeaders={}, Signature={}",
                                           algorithm,
                                           self.access_key.clone(),
                                           credential_scope,
                                           signed_headers,
                                           signature);
        // debug!("authorization_header: {}", authorization_header);

        let mut headers = Headers::new();
        headers.set_raw("X-Amz-Date", vec![self.amzdate.as_bytes().to_vec()]);
        headers.set_raw("Authorization",
                        vec![authorization_header.as_bytes().to_vec()]);
        headers.set_raw("x-amz-content-sha256",
                        vec![self.payload_hash
                                 .as_bytes()
                                 .to_vec()]);
        for (k, v) in &self.headers {
            headers.set_raw(k.clone(), vec![v.as_bytes().to_vec()]);
        }
        headers
    }
}

#[test]
fn test_get_signature() {
    use env_logger;
    let _ = env_logger::init();

    let sig = AwsSignature {
        access_key: "SOME_RANDOM_ACCESS_KEY".to_string(),
        secret_key: "SOME_RANDOM_SECRET_KEY".to_string(),
        method: "GET".to_string(),
        service: "s3".to_string(),
        host: "haumaru-test2.s3.amazonaws.com".to_string(),
        region: "us-west-2".to_string(),
        amzdate: "20160830T022534Z".to_string(),
        datestamp: "20160830".to_string(),
        canonical_uri: "/".to_string(),
        canonical_querystring: "list-type=2".to_string(),
        payload_hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            .to_string(),
        headers: HashMap::new(),
    };

    let mut headers = Headers::new();
    headers.set_raw("X-Amz-Date", vec![b"20160830T022534Z".to_vec()]);
    headers.set_raw("Authorization", vec![b"AWS4-HMAC-SHA256 Credential=SOME_RANDOM_ACCESS_KEY/20160830/us-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=a4ee0b6421cb27fcf1ad70fe96bc5bfa8747288fe475ff15e2398cc75ef73269".to_vec()]);
    headers.set_raw("x-amz-content-sha256",
                    vec!["e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                             .as_bytes()
                             .to_vec()]);

    let calcd_headers = sig.signed_headers();
    assert_eq!(headers, calcd_headers);
}

#[test]
fn test_put_signature() {
    use env_logger;
    let _ = env_logger::init();

    let mut headers = HashMap::new();
    headers.insert("Content-MD5".to_string(),
                   "flOOoET+sEjJGP6Pv/kH6w==".to_string());

    let key = "k/d/aksdjnaduij";
    let sig = AwsSignature {
        access_key: "SOME_RANDOM_ACCESS_KEY".to_string(),
        secret_key: "SOME_RANDOM_SECRET_KEY".to_string(),
        method: "PUT".to_string(),
        service: "s3".to_string(),
        host: "haumaru-test2.s3.amazonaws.com".to_string(),
        region: "us-west-2".to_string(),
        amzdate: "20160830T022534Z".to_string(),
        datestamp: "20160830".to_string(),
        canonical_uri: format!("/{}", key),
        canonical_querystring: "".to_string(),
        payload_hash: "33da36a652b582c6a5c95d2aff38ff95831f12554a09b7eb611b3594556557dc"
            .to_string(),
        headers: headers,
    };

    let mut headers = Headers::new();
    headers.set_raw("X-Amz-Date", vec![b"20160830T022534Z".to_vec()]);
    headers.set_raw("Authorization", vec![b"AWS4-HMAC-SHA256 Credential=SOME_RANDOM_ACCESS_KEY/20160830/us-west-2/s3/aws4_request, SignedHeaders=content-md5;host;x-amz-date, Signature=0f35d736d74e9c5104f428272e9380fae79146b1da52a49915c0ee8dd99dbed8".to_vec()]);
    headers.set_raw("x-amz-content-sha256",
                    vec!["33da36a652b582c6a5c95d2aff38ff95831f12554a09b7eb611b3594556557dc"
                             .as_bytes()
                             .to_vec()]);
    headers.set_raw("Content-MD5", vec![b"flOOoET+sEjJGP6Pv/kH6w==".to_vec()]);

    let calcd_headers = sig.signed_headers();
    assert_eq!(headers, calcd_headers);
}

struct AmazonRequest {
    access_key: String,
    secret_key: String,
    service: String,
    method: Method,
    url: Url,
    region: String,
    payload_hash: String,
    headers: HashMap<String, String>,
}

impl AmazonRequest {
    fn new(access_key: &str, secret_key: &str, service: &str, method: Method, url: Url) -> Self {
        AmazonRequest {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            service: service.into(),
            method: method,
            url: url,
            region: "us-west-2".into(),
            payload_hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
            headers: HashMap::new(),
        }
    }
    fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
    fn with_payload_hash(mut self, sha256: &str) -> Self {
        self.payload_hash = sha256.into();
        self
    }
    fn send<'a>(self,
                client: &'a Client,
                dt: DateTime<UTC>,
                body: Option<Body<'a>>)
                -> Result<Response, String> {
        let amzdate = dt.format("%Y%m%dT%H%M%SZ").to_string();
        let datestamp = dt.format("%Y%m%d").to_string();

        let host = match self.url.host_str() {
            None => return Err(format!("No host part")),
            Some(h) => h.to_string(),
        };

        let canonical_uri = self.url.path().to_string();

        let canonical_querystring = match self.url.query() {
            None => "".to_string(),
            Some(s) => s.to_string(),
        };

        let sig = AwsSignature {
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            method: self.method.as_ref().into(),
            service: self.service.clone(),
            host: host.clone(),
            region: self.region.clone(),
            amzdate: amzdate.clone(),
            datestamp: datestamp.clone(),
            canonical_uri: canonical_uri.clone(),
            canonical_querystring: canonical_querystring.clone(),
            payload_hash: self.payload_hash.clone(),
            headers: self.headers.clone(),
        };
        let headers = sig.signed_headers();

        let mut res = client.request(self.method.clone(), self.url.clone())
            .headers(headers.clone());
        match body {
            None => (),
            Some(body) => res = res.body(body),
        }
        let res = res.send()
            .map_err(|e| {
                format!("AWS request failed: {}. URL: {:?}, Headers: {:?}",
                        e,
                        self.url,
                        headers)
            })?;
        debug!("{:?}", res);
        Ok(res)
    }
}

impl S3Storage {
    fn key_exists(&self, dt: DateTime<UTC>, key: &str) -> Result<bool, String> {
        let host = format!("{}.s3.amazonaws.com", self.bucket);
        let query = format!("list-type=2&prefix={}", key).replace("/", "%2F");
        let url_str = format!("https://{}?{}", host, query);

        let url = url_str.parse().expect("URL");
        let aws_req =
            AmazonRequest::new(&self.access_key, &self.secret_key, "s3", Method::Get, url);
        let mut result = aws_req.send(&self.client, dt.clone(), None)
            .map_err(|e| format!("Failed to check S3 key exists: {}", e))?;

        let mut response_body = String::new();
        result.read_to_string(&mut response_body).expect("read_to_string");
        debug!("List Result:\n{:?}", response_body);

        if result.status != hyper::Ok {
            return Err(format!("Failed to check key exists: {}. {}\n{}",
                               result.status,
                               url_str,
                               response_body)
                .into());
        }

        lazy_static! {
            static ref RE: Regex = Regex::new(".*<KeyCount>([\\d]+)</KeyCount>.*").unwrap();
        }
        let caps = RE.captures(&response_body).unwrap();

        Ok(if let Some(n) = caps.at(1) {
            n == "1"
        } else {
            false
        })
    }

    fn key_from_sha256(&self, hash: &str) -> String {
        format!("{}/{}/{}/{}", self.prefix, &hash[0..1], &hash[1..2], &hash)
    }
}

impl Storage for S3Storage {
    fn send(&self, req: &mut SendRequest) -> Result<(), Box<Error>> {
        let &mut SendRequest { ref md5, sha256: ref hash, node: ref _node, ref mut reader, size } = req;
        let hex = hash.to_hex();
        let key = self.key_from_sha256(&hex);

        debug!("Using s3://{}/{}", self.bucket, key);

        if self.key_exists(UTC::now(), &key)? {
            debug!("Storage already contains {}", key);
            return Ok(());
        }

        debug!("Uploading s3://{}/{} ({} bytes)", self.bucket, key, size);
        let dt: DateTime<UTC> = UTC::now();

        let host = format!("{}.s3.amazonaws.com", self.bucket);
        let url_str = format!("https://{}/{}", host, key);
        let url = url_str.parse().expect("URL");
        let aws_req =
            AmazonRequest::new(&self.access_key, &self.secret_key, "s3", Method::Put, url)
                .with_header("x-amz-storage-clas", "STANDARD_IA")
                .with_header("Content-MD5",
                             &md5.to_base64(base64::Config {
                                 char_set: CharacterSet::Standard,
                                 newline: Newline::LF,
                                 pad: true,
                                 line_length: None,
                             }))
                .with_payload_hash(&hash.to_hex());
        let mut result = aws_req.send(&self.client,
                  dt.clone(),
                  Some(Body::SizedBody(reader, size)))
            .map_err(|e| format!("Failed to upload key to S3: {}", e))?;

        if result.status != hyper::Ok {
            let mut response_body = String::new();
            result.read_to_string(&mut response_body).expect("read_to_string");
            warn!("Failed upload result body:\n{:?}", response_body);
            return Err(format!("Failed failed to upload key: {}. {}\n{}",
                               result.status,
                               url_str,
                               response_body)
                .into());
        }

        Ok(())
    }
    fn retrieve(&self, _hash: &[u8]) -> Result<Option<Box<Read>>, Box<Error>> {
        use std::io::Cursor;
        Ok(Some(box Cursor::new(vec![])))
    }
    fn verify(&self, n: Node) -> Result<(Node, bool), Box<Error>> {
        let hex = n.hash().as_ref().expect("hash").to_hex();
        let key = self.key_from_sha256(&hex);
        if self.key_exists(UTC::now(), &key)? {
            info!("{} OK", key);
            Ok((n, true))
        } else {
            Ok((n, false))
        }
    }
}

fn get_signature_key(key: String,
                     datestamp: String,
                     region_name: String,
                     service_name: String)
                     -> Vec<u8> {
    let k_date = sign(format!("AWS4{}", key).as_bytes().to_vec(), datestamp);
    let k_region = sign(k_date, region_name);
    let k_service = sign(k_region, service_name);
    sign(k_service, "aws4_request".to_string())
}

fn sha256(content: &str) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.write_all(content.as_bytes()).expect("hash write_all");
    let (_md5, sha256) = hasher.result();
    sha256
}

fn sign(key: Vec<u8>, msg: String) -> Vec<u8> {
    let mut hmac = Hmac::new(Sha256::new(), &key);
    hmac.input(msg.as_bytes());
    hmac.result().code().to_vec()
}
