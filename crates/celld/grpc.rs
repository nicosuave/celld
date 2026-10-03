// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Streaming translation at the Fetch boundary. Binary gRPC-web and gRPC
//! share message framing; only content types and response trailers differ.
//! See https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md.
//!
//! These bodies own their upstream body and never spawn a pump. Each consumer
//! poll pulls only the next needed frame; dropping the adapter drops upstream.

use bytes::{BufMut, Bytes, BytesMut};
use hyper::body::{Body, Frame};
use hyper::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE, TE};
use hyper::Response;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

// A trailer block is metadata, not a protobuf message. Bound its allocation
// independently of message size; data messages are forwarded without buffering.
const MAX_TRAILER_BYTES: usize = 64 * 1024;

fn translate_content_type(value: &str, from: &str, to: &str) -> Option<String> {
    let (media_type, parameters) = value.split_once(';').unwrap_or((value, ""));
    let media_type = media_type.trim();
    let suffix = media_type.get(from.len()..)?;
    if !media_type.get(..from.len())?.eq_ignore_ascii_case(from)
        || (!suffix.is_empty() && !suffix.starts_with('+'))
    {
        return None;
    }
    let parameters = if parameters.is_empty() {
        String::new()
    } else {
        format!(";{parameters}")
    };
    Some(format!("{to}{suffix}{parameters}"))
}

fn translate_headers(headers: &mut HeaderMap, from: &str, to: &str) -> bool {
    let Some(content_type) = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| translate_content_type(value, from, to))
        .and_then(|value| HeaderValue::from_str(&value).ok())
    else {
        return false;
    };
    headers.insert(CONTENT_TYPE, content_type);
    true
}

pub fn is_web_content_type(value: &str) -> bool {
    translate_content_type(value, "application/grpc-web", "application/grpc").is_some()
}

pub fn is_native_content_type(value: &str) -> bool {
    translate_content_type(value, "application/grpc", "application/grpc-web").is_some()
}

/// Translate a native request's headers before exposing it to a Worker.
/// The binary request body is already a valid gRPC-web body.
pub fn request_to_web(headers: &mut HeaderMap) -> bool {
    let translated = translate_headers(headers, "application/grpc", "application/grpc-web");
    if translated {
        headers.remove(TE);
    }
    translated
}

/// Translate a binary gRPC-web request before sending it over HTTP/2.
/// Text/base64 gRPC-web is deliberately not classified as binary.
pub fn request_to_native(headers: &mut HeaderMap) -> bool {
    let translated = translate_headers(headers, "application/grpc-web", "application/grpc");
    if translated {
        headers.insert(TE, HeaderValue::from_static("trailers"));
        headers.remove("connection");
        headers.remove("transfer-encoding");
    }
    translated
}

/// Decode a Worker's binary gRPC-web response into native HTTP/2 trailers.
pub fn web_response_to_native<B>(response: Response<B>) -> Response<ToNative<B>> {
    let (mut parts, body) = response.into_parts();
    translate_headers(
        &mut parts.headers,
        "application/grpc-web",
        "application/grpc",
    );
    parts.headers.remove(CONTENT_LENGTH);
    parts.headers.remove("transfer-encoding");
    parts.headers.remove("connection");
    let has_status = parts.headers.contains_key("grpc-status");
    Response::from_parts(parts, ToNative::new(body, has_status))
}

/// Encode a native upstream's HTTP trailers into a binary gRPC-web frame.
/// Use the HTTP body itself, not `into_data_stream`, which discards trailers.
pub fn native_response_to_web<B>(response: Response<B>) -> Response<ToWeb<B>> {
    let (mut parts, body) = response.into_parts();
    translate_headers(
        &mut parts.headers,
        "application/grpc",
        "application/grpc-web",
    );
    parts.headers.remove(CONTENT_LENGTH);
    parts.headers.remove("trailer");
    let has_status = parts.headers.contains_key("grpc-status");
    Response::from_parts(parts, ToWeb::new(body, has_status))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn missing_status() -> HeaderMap {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("2"));
    trailers.insert(
        "grpc-message",
        HeaderValue::from_static("upstream ended without grpc-status"),
    );
    trailers
}

fn encode_trailers(trailers: &HeaderMap) -> io::Result<Bytes> {
    let mut block = BytesMut::new();
    for (name, value) in trailers {
        if block.len() + name.as_str().len() + value.as_bytes().len() + 4 > MAX_TRAILER_BYTES {
            return Err(invalid("gRPC trailer block exceeds 64 KiB"));
        }
        block.extend_from_slice(name.as_str().as_bytes());
        block.extend_from_slice(b": ");
        block.extend_from_slice(value.as_bytes());
        block.extend_from_slice(b"\r\n");
    }
    let mut frame = BytesMut::with_capacity(block.len() + 5);
    frame.put_u8(0x80);
    frame.put_u32(block.len() as u32);
    frame.extend_from_slice(&block);
    Ok(frame.freeze())
}

fn decode_trailers(block: &[u8]) -> io::Result<HeaderMap> {
    let mut trailers = HeaderMap::new();
    for line in block.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let colon = line
            .iter()
            .position(|byte| *byte == b':')
            .ok_or_else(|| invalid("malformed gRPC-web trailer"))?;
        let name = HeaderName::from_bytes(&line[..colon])
            .map_err(|_| invalid("invalid gRPC-web trailer name"))?;
        if matches!(
            name.as_str(),
            "content-length" | "transfer-encoding" | "connection" | "trailer" | "te"
        ) {
            return Err(invalid("forbidden gRPC-web trailer name"));
        }
        let value = &line[colon + 1..];
        let value = value.strip_prefix(b" ").unwrap_or(value);
        let value = HeaderValue::from_bytes(value)
            .map_err(|_| invalid("invalid gRPC-web trailer value"))?;
        trailers.append(name, value);
    }
    Ok(trailers)
}

pub struct ToWeb<B> {
    inner: Pin<Box<B>>,
    has_status: bool,
    done: bool,
}

impl<B> ToWeb<B> {
    fn new(inner: B, has_status: bool) -> Self {
        Self {
            inner: Box::pin(inner),
            has_status,
            done: false,
        }
    }
}

impl<B> Body for ToWeb<B>
where
    B: Body<Data = Bytes>,
    B::Error: std::fmt::Display,
{
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Frame<Bytes>>>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_frame(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(error))) => {
                self.done = true;
                Poll::Ready(Some(Err(io::Error::other(error.to_string()))))
            }
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(data) => Poll::Ready(Some(Ok(Frame::data(data)))),
                Err(frame) => {
                    let Ok(mut trailers) = frame.into_trailers() else {
                        self.done = true;
                        return Poll::Ready(Some(Err(invalid("unsupported HTTP body frame"))));
                    };
                    if !trailers.contains_key("grpc-status") && !self.has_status {
                        trailers.extend(missing_status());
                    }
                    self.done = true;
                    Poll::Ready(Some(encode_trailers(&trailers).map(Frame::data)))
                }
            },
            Poll::Ready(None) => {
                self.done = true;
                if self.has_status {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(encode_trailers(&missing_status()).map(Frame::data)))
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

enum DecodeState {
    Header,
    Data(usize),
    Trailers(usize, BytesMut),
    End(HeaderMap),
    Done,
}

pub struct ToNative<B> {
    inner: Pin<Box<B>>,
    pending: Bytes,
    header: BytesMut,
    state: DecodeState,
    has_status: bool,
}

impl<B> ToNative<B> {
    fn new(inner: B, has_status: bool) -> Self {
        Self {
            inner: Box::pin(inner),
            pending: Bytes::new(),
            header: BytesMut::with_capacity(5),
            state: DecodeState::Header,
            has_status,
        }
    }
}

impl<B> Body for ToNative<B>
where
    B: Body<Data = Bytes>,
    B::Error: std::fmt::Display,
{
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Frame<Bytes>>>> {
        let this = self.get_mut();
        let mut upstream_polls = 0;
        loop {
            if matches!(this.state, DecodeState::Done) {
                return Poll::Ready(None);
            }
            if !this.pending.is_empty() {
                match &mut this.state {
                    DecodeState::Header => {
                        let count = (5 - this.header.len()).min(this.pending.len());
                        this.header.extend_from_slice(&this.pending.split_to(count));
                        if this.header.len() < 5 {
                            continue;
                        }
                        let flag = this.header[0];
                        let length =
                            u32::from_be_bytes(this.header[1..5].try_into().unwrap()) as usize;
                        match flag {
                            0 | 1 => {
                                this.state = if length == 0 {
                                    DecodeState::Header
                                } else {
                                    DecodeState::Data(length)
                                };
                                return Poll::Ready(Some(Ok(Frame::data(
                                    this.header.split().freeze(),
                                ))));
                            }
                            0x80 if length <= MAX_TRAILER_BYTES => {
                                this.header.clear();
                                this.state = if length == 0 {
                                    DecodeState::End(HeaderMap::new())
                                } else {
                                    DecodeState::Trailers(length, BytesMut::new())
                                };
                            }
                            _ => {
                                this.state = DecodeState::Done;
                                return Poll::Ready(Some(Err(invalid(
                                    "invalid or oversized gRPC-web frame",
                                ))));
                            }
                        }
                    }
                    DecodeState::Data(remaining) => {
                        let count = (*remaining).min(this.pending.len());
                        let data = this.pending.split_to(count);
                        *remaining -= count;
                        if *remaining == 0 {
                            this.state = DecodeState::Header;
                        }
                        return Poll::Ready(Some(Ok(Frame::data(data))));
                    }
                    DecodeState::Trailers(remaining, block) => {
                        let count = (*remaining).min(this.pending.len());
                        block.extend_from_slice(&this.pending.split_to(count));
                        *remaining -= count;
                        if *remaining == 0 {
                            match decode_trailers(block) {
                                Ok(trailers) => this.state = DecodeState::End(trailers),
                                Err(error) => {
                                    this.state = DecodeState::Done;
                                    return Poll::Ready(Some(Err(error)));
                                }
                            }
                        }
                    }
                    DecodeState::End(_) => {
                        this.state = DecodeState::Done;
                        return Poll::Ready(Some(Err(invalid("data after gRPC-web trailers"))));
                    }
                    DecodeState::Done => unreachable!(),
                }
                continue;
            }
            // Bound work even when upstream repeatedly yields empty chunks.
            if upstream_polls == 32 {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            upstream_polls += 1;
            match this.inner.as_mut().poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => this.pending = data,
                    Err(_) => {
                        this.state = DecodeState::Done;
                        return Poll::Ready(Some(Err(invalid(
                            "HTTP trailers in gRPC-web response",
                        ))));
                    }
                },
                Poll::Ready(Some(Err(error))) => {
                    this.state = DecodeState::Done;
                    return Poll::Ready(Some(Err(io::Error::other(error.to_string()))));
                }
                Poll::Ready(None) => {
                    let state = std::mem::replace(&mut this.state, DecodeState::Done);
                    return match state {
                        DecodeState::End(mut trailers) => {
                            if !trailers.contains_key("grpc-status") && !this.has_status {
                                trailers.extend(missing_status());
                            }
                            Poll::Ready(Some(Ok(Frame::trailers(trailers))))
                        }
                        DecodeState::Header if this.header.is_empty() => {
                            if this.has_status {
                                Poll::Ready(None)
                            } else {
                                Poll::Ready(Some(Ok(Frame::trailers(missing_status()))))
                            }
                        }
                        _ => Poll::Ready(Some(Err(invalid("truncated gRPC-web frame")))),
                    };
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self.state, DecodeState::Done)
    }
}

#[cfg(test)]
#[path = "grpc_tests.rs"]
mod tests;
