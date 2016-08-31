use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::{Read, Write};

use rustc_serialize::base64::{CharacterSet, ToBase64, Newline};
use rustc_serialize::base64;
use rustc_serialize::hex::ToHex;
use chrono::*;
use crypto::hmac::Hmac;
use crypto::sha2::Sha256;
use crypto::mac::Mac;
use hyper::client::*;
use hyper::header::Headers;
use hyper::method::Method;
use hyper;

use {Node, Storage};
use hasher::Hasher;

pub struct S3Storage {
    // region: String,
    bucket: String,
    prefix: String,
}

impl S3Storage {
    pub fn new() -> Self {
        S3Storage {
            bucket: "haumaru-test2".to_string(),
            prefix: "test".to_string(),
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
    let mut calcd_headers = sig.signed_headers();

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

impl Storage for S3Storage {
    fn send(&self,
            md5: &[u8],
            hash: &[u8],
            size: u64,
            mut ins: Box<Read>)
            -> Result<(), Box<Error>> {
        let hex = hash.to_hex();
        let key = format!("{}/{}/{}/{}", self.prefix, &hex[0..1], &hex[1..2], hex);

        debug!("Using s3://{}/{}", self.bucket, key);

        let client = Client::new();

        {
            let dt: DateTime<UTC> = UTC::now();
            let amzdate = dt.format("%Y%m%dT%H%M%SZ").to_string();
            let datestamp = dt.format("%Y%m%d").to_string();

            let sig = AwsSignature {
                access_key: env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID").to_string(),
                secret_key: env::var("AWS_SECRET_ACCESS_KEY")
                    .expect("AWS_SECRET_ACCESS_KEY")
                    .to_string(),
                method: "GET".to_string(),
                service: "s3".to_string(),
                host: "haumaru-test2.s3.amazonaws.com".to_string(),
                region: "us-west-2".to_string(),
                amzdate: amzdate,
                datestamp: datestamp,
                canonical_uri: "/".to_string(),
                canonical_querystring: "list-type=2".to_string(),
                payload_hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_string(),
                headers: HashMap::new(),
            };
            let headers = sig.signed_headers();

            let request_url = format!("https://{}?{}", sig.host, sig.canonical_querystring);

            let mut res = client.get(&request_url).headers(headers).send().unwrap();

            // info!("{:?}", res);
            assert_eq!(hyper::Ok, res.status);
            let mut response_body = String::new();
            res.read_to_string(&mut response_body).expect("read_to_string");
            debug!("List Result:\n{:?}", response_body);

            assert_eq!(res.status, hyper::Ok);
        }

        info!("Uploading s3://{}/{}", self.bucket, key);

        {
            let mut headers = HashMap::new();
            headers.insert("x-amz-storage-class".to_string(), "STANDARD_IA".to_string());
            headers.insert("Content-MD5".to_string(),
                           md5.to_base64(base64::Config {
                               char_set: CharacterSet::Standard,
                               newline: Newline::LF,
                               pad: true,
                               line_length: None,
                           }));

            let dt: DateTime<UTC> = UTC::now();
            let amzdate = dt.format("%Y%m%dT%H%M%SZ").to_string();
            let datestamp = dt.format("%Y%m%d").to_string();

            let sig = AwsSignature {
                access_key: env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID").to_string(),
                secret_key: env::var("AWS_SECRET_ACCESS_KEY")
                    .expect("AWS_SECRET_ACCESS_KEY")
                    .to_string(),
                method: "PUT".to_string(),
                service: "s3".to_string(),
                host: "haumaru-test2.s3.amazonaws.com".to_string(),
                region: "us-west-2".to_string(),
                amzdate: amzdate,
                datestamp: datestamp,
                canonical_uri: format!("/{}", key),
                canonical_querystring: "".to_string(),
                payload_hash: hash.to_hex(),
                headers: headers,
            };
            let mut headers = sig.signed_headers();

            let request_url = format!("https://{}/{}", sig.host, key);

            let mut res = client.request(Method::Put, &request_url)
                .headers(headers)
                .body(Body::SizedBody(&mut ins, size))
                .send()
                .unwrap();

            debug!("{:?}", res);

            let mut response_body = String::new();
            res.read_to_string(&mut response_body).expect("read_to_string");
            debug!("Upload Result:\n{:?}", response_body);

            assert_eq!(res.status, hyper::Ok);
        }
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
