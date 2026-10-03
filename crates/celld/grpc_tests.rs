// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Protocol tests drive real HTTP/2 transport outside the actor execution domain.
#![allow(clippy::disallowed_methods)]

use super::*;
use futures_util::{stream, StreamExt};
use http_body_util::{BodyExt, Full, StreamBody};
use std::convert::Infallible;

fn message(flag: u8, payload: &[u8]) -> Bytes {
    let mut bytes = BytesMut::new();
    bytes.put_u8(flag);
    bytes.put_u32(payload.len() as u32);
    bytes.extend_from_slice(payload);
    bytes.freeze()
}

fn status(code: &'static str) -> HeaderMap {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static(code));
    trailers.insert(
        "grpc-message",
        HeaderValue::from_static("permission%20denied"),
    );
    trailers.append("details-bin", HeaderValue::from_static("AQI="));
    trailers.append("details-bin", HeaderValue::from_static("AwQ="));
    trailers
}

#[test]
fn content_types_preserve_codec_and_parameters_without_matching_other_protocols() {
    for value in [
        "application/grpc",
        "application/grpc+proto",
        "application/grpc+json; charset=utf-8",
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, value.parse().unwrap());
        headers.insert(TE, HeaderValue::from_static("trailers"));
        assert!(request_to_web(&mut headers));
        assert!(!headers.contains_key(TE));
        assert!(is_web_content_type(headers[CONTENT_TYPE].to_str().unwrap()));
        assert!(request_to_native(&mut headers));
        assert_eq!(headers[CONTENT_TYPE], value);
        assert_eq!(headers[TE], "trailers");
    }
    for value in [
        "application/grpc-web-text",
        "application/grpc-web-text+proto",
        "application/grpc-bogus",
        "application/json",
        "🌐",
    ] {
        assert!(!is_native_content_type(value));
        assert!(!is_web_content_type(value));
    }
}

#[test]
fn multibyte_content_types_never_panic_at_protocol_prefix_boundaries() {
    for prefix in ["application/grpc", "application/grpc-web"] {
        for position in 0..=prefix.len() {
            for character in ["é", "界", "🌐"] {
                let value = format!("{}{character}invalid", &prefix[..position]);
                assert!(!is_native_content_type(&value));
                assert!(!is_web_content_type(&value));
            }
        }
    }
}

#[tokio::test]
async fn arbitrary_chunk_boundaries_preserve_messages_and_trailers() {
    let mut data = BytesMut::new();
    data.extend_from_slice(&message(0, b"first"));
    data.extend_from_slice(&message(1, b"compressed bytes stay opaque"));
    data.extend_from_slice(&message(0, b""));
    let data = data.freeze();
    let trailers = status("7");
    let mut wire = BytesMut::from(data.as_ref());
    wire.extend_from_slice(&encode_trailers(&trailers).unwrap());
    let wire = wire.freeze();
    for split in 0..=wire.len() {
        let chunks = vec![wire.slice(..split), wire.slice(split..)];
        let input = StreamBody::new(stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<_, Infallible>(Frame::data(chunk))),
        ));
        let body = ToNative::new(input, false).collect().await.unwrap();
        assert_eq!(body.trailers().unwrap(), &trailers, "split {split}");
        assert_eq!(body.to_bytes(), data, "split {split}");
    }
    let input =
        StreamBody::new(stream::iter(wire.iter().map(|byte| {
            Ok::<_, Infallible>(Frame::data(Bytes::copy_from_slice(&[*byte])))
        })));
    let body = ToNative::new(input, false).collect().await.unwrap();
    assert_eq!(body.trailers().unwrap(), &trailers);
    assert_eq!(body.to_bytes(), data);
}

#[tokio::test]
async fn native_trailers_become_web_frame_and_http_status_is_preserved() {
    let data = message(0, b"hello");
    let trailers = status("7");
    let input = StreamBody::new(stream::iter(vec![
        Ok::<_, Infallible>(Frame::data(data.clone())),
        Ok(Frame::trailers(trailers.clone())),
    ]));
    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "application/grpc+proto")
        .header(CONTENT_LENGTH, "10")
        .body(input)
        .unwrap();
    let response = native_response_to_web(response);
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()[CONTENT_TYPE],
        "application/grpc-web+proto"
    );
    assert!(!response.headers().contains_key(CONTENT_LENGTH));
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..data.len()], data.as_ref());
    assert_eq!(
        &bytes[data.len()..],
        encode_trailers(&trailers).unwrap().as_ref()
    );
}

#[tokio::test]
async fn trailers_only_headers_and_missing_status_are_not_success() {
    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "application/grpc-web")
        .header("grpc-status", "16")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = web_response_to_native(response);
    assert_eq!(response.headers()["grpc-status"], "16");
    assert_eq!(response.headers()[CONTENT_TYPE], "application/grpc");
    assert!(response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .is_empty());
    let empty = ToNative::new(Full::new(Bytes::new()), false)
        .collect()
        .await
        .unwrap();
    assert_eq!(empty.trailers().unwrap()["grpc-status"], "2");
    let web = ToWeb::new(Full::new(Bytes::new()), false)
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let native = ToNative::new(Full::new(web), false)
        .collect()
        .await
        .unwrap();
    assert_eq!(native.trailers().unwrap()["grpc-status"], "2");
}

#[tokio::test]
async fn malformed_frames_fail_instead_of_becoming_successful_trailers() {
    let complete = message(0, b"message");
    for length in 1..complete.len() {
        assert!(ToNative::new(Full::new(complete.slice(..length)), false)
            .collect()
            .await
            .is_err());
    }
    let mut after_trailers = BytesMut::from(encode_trailers(&status("0")).unwrap().as_ref());
    after_trailers.extend_from_slice(&complete);
    let oversized = Bytes::from_static(&[0x80, 0, 1, 0, 1]);
    for wire in [
        after_trailers.freeze(),
        oversized,
        message(0x81, b"compressed trailer"),
        message(2, b"invalid flags"),
        message(0x80, b"bad header\r\n"),
        message(0x80, b"content-length: 1\r\n"),
    ] {
        assert!(ToNative::new(Full::new(wire), false)
            .collect()
            .await
            .is_err());
    }
}

struct TrackedBody {
    polls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for TrackedBody {
    fn drop(&mut self) {
        self.dropped
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Body for TrackedBody {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Frame<Bytes>>>> {
        self.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Poll::Ready(Some(Ok(Frame::data(message(0, b"chunk")))))
    }
}

#[tokio::test]
async fn wrappers_pull_on_demand_and_drop_the_source_on_cancellation() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    for native in [true, false] {
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let body = TrackedBody {
            polls: polls.clone(),
            dropped: dropped.clone(),
        };
        let mut body = if native {
            ToNative::new(body, false).boxed_unsync()
        } else {
            ToWeb::new(body, false).boxed_unsync()
        };
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert!(body.frame().await.unwrap().is_ok());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        drop(body);
        assert!(dropped.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn upstream_errors_remain_errors() {
    for native in [true, false] {
        let input = StreamBody::new(stream::iter(vec![Err::<Frame<Bytes>, _>(
            io::Error::other("connection reset"),
        )]));
        let result = if native {
            ToNative::new(input, false).collect().await
        } else {
            ToWeb::new(input, false).collect().await
        };
        assert!(result.unwrap_err().to_string().contains("connection reset"));
    }
}

#[tokio::test]
async fn http2_round_trip_streams_data_before_final_status() {
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let released = std::sync::Arc::new(std::sync::Mutex::new(Some(released)));
    let expected_request = message(0, b"request");
    let request_data = expected_request.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
            let released = released.lock().unwrap().take().unwrap();
            let expected_request = expected_request.clone();
            async move {
                assert_eq!(request.version(), hyper::Version::HTTP_2);
                assert_eq!(request.headers()[CONTENT_TYPE], "application/grpc+proto");
                assert_eq!(request.headers()[TE], "trailers");
                assert_eq!(
                    request.into_body().collect().await.unwrap().to_bytes(),
                    expected_request
                );
                let first = stream::once(async {
                    Ok::<_, Infallible>(Frame::data(message(0, b"first reply")))
                });
                let last = stream::once(async move {
                    released.await.unwrap();
                    Ok::<_, Infallible>(Frame::trailers(status("7")))
                });
                Ok::<_, Infallible>(
                    Response::builder()
                        .header(CONTENT_TYPE, "application/grpc+proto")
                        .body(StreamBody::new(first.chain(last)))
                        .unwrap(),
                )
            }
        });
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(socket), service)
            .await
            .unwrap();
    });
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/grpc-web+proto"),
        );
        assert!(request_to_native(&mut headers));
        let response = client
            .post(format!("http://{address}/hello.Greeter/SayHello"))
            .headers(headers)
            .body(request_data)
            .send()
            .await
            .unwrap();
        assert_eq!(response.version(), hyper::Version::HTTP_2);
        let response: Response<reqwest::Body> = response.into();
        let mut web = native_response_to_web(response).into_body();
        let first = web.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(first, message(0, b"first reply"));
        release.send(()).unwrap();
        let trailer = web.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(trailer, encode_trailers(&status("7")).unwrap());
        assert!(web.frame().await.is_none());
    })
    .await;
    server.abort();
    result.unwrap();
}
