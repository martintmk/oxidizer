// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(clippy::missing_panics_doc, clippy::unwrap_used, reason = "benchmark code")]
#![expect(missing_docs, reason = "Benchmark code")]

use std::alloc::System;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use alloc_tracker::{Allocator, Session};
use benchmarking::time_sample_with_inputs;
use bytes::Bytes;
use bytesbuf::BytesView;
use bytesbuf::mem::testing::TransparentMemory;
use criterion::{Criterion, criterion_group, criterion_main};
use futures::executor::block_on;
use http::{Request, Uri};
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;
use http_extensions::{HttpBody, HttpBodyBuilder, HttpBodyOptions, HttpError, Result};
use tick::Clock;

#[global_allocator]
static ALLOCATOR: Allocator<System> = Allocator::system();

const REQUEST_BYTES: usize = 10_240;
const RESPONSE_BYTES: usize = 9_216;
const RESPONSE_FRAME_BYTES: usize = RESPONSE_BYTES / 3;

fn prepared_inputs<T>(iters: u64, mut setup: impl FnMut() -> T) -> Vec<T> {
    (0..iters).map(|_| setup()).collect()
}

fn response_body(builder: &HttpBodyBuilder) -> HttpBody {
    let frame = BytesView::from(Bytes::from_static(&[0_u8; RESPONSE_FRAME_BYTES]));
    builder.body(
        ThreeFrameBody::new([frame.clone(), frame.clone(), frame]),
        &HttpBodyOptions::default(),
    )
}

fn entry(c: &mut Criterion) {
    let session = Session::new();
    let memory = TransparentMemory::new();
    let body_builder = HttpBodyBuilder::with_custom_memory(memory.clone(), &Clock::new_frozen());
    let request_payload = BytesView::copied_from_slice(&[0_u8; REQUEST_BYTES], &memory);
    let uri = Uri::from_static("http://127.0.0.1/request-response");

    let mut group = c.benchmark_group("body_path_request");

    let operation = session.operation("request_construction");
    group.bench_function("request_construction", |b| {
        b.iter_custom(|iters| {
            let payloads = prepared_inputs(iters, || request_payload.clone());
            time_sample_with_inputs(
                payloads,
                |sample_iters| operation.measure_thread().iterations(sample_iters),
                |payload| Request::post(uri.clone()).body(body_builder.bytes(mem::take(payload))).unwrap(),
            )
        });
    });

    let operation = session.operation("request_body_framing");
    group.bench_function("request_body_framing", |b| {
        b.iter_custom(|iters| {
            let bodies = prepared_inputs(iters, || body_builder.bytes(request_payload.clone()));
            time_sample_with_inputs(
                bodies,
                |sample_iters| operation.measure_thread().iterations(sample_iters),
                |body| block_on(body.frame()).unwrap().unwrap().into_data().unwrap(),
            )
        });
    });

    group.finish();

    let mut group = c.benchmark_group("body_path_response");

    let operation = session.operation("hyper_frame_handoff");
    group.bench_function("hyper_frame_handoff", |b| {
        b.iter_custom(|iters| {
            let frames = prepared_inputs(iters, || Some(Frame::data(Bytes::from_static(&[0_u8; RESPONSE_FRAME_BYTES]))));
            time_sample_with_inputs(
                frames,
                |sample_iters| operation.measure_thread().iterations(sample_iters),
                |frame| frame.take().unwrap().map_data(BytesView::from),
            )
        });
    });

    let operation = session.operation("response_frame_consumption");
    group.bench_function("response_frame_consumption", |b| {
        b.iter_custom(|iters| {
            let bodies = prepared_inputs(iters, || response_body(&body_builder));
            time_sample_with_inputs(
                bodies,
                |sample_iters| operation.measure_thread().iterations(sample_iters),
                |body| {
                    let mut total = 0;
                    while let Some(frame) = block_on(body.frame()) {
                        total += frame.unwrap().into_data().unwrap().len();
                    }
                    total
                },
            )
        });
    });

    let operation = session.operation("complete_response_drain");
    group.bench_function("complete_response_drain", |b| {
        b.iter_custom(|iters| {
            let bodies = prepared_inputs(iters, || response_body(&body_builder));
            time_sample_with_inputs(
                bodies,
                |sample_iters| operation.measure_thread().iterations(sample_iters),
                |body| block_on(mem::replace(body, body_builder.empty()).into_bytes()).unwrap(),
            )
        });
    });

    group.finish();

    let mut group = c.benchmark_group("body_path_complete");

    let operation = session.operation("complete_request_response");
    group.bench_function("complete_request_response", |b| {
        b.iter_custom(|iters| {
            let inputs = prepared_inputs(iters, || CompletePathInput {
                request: Some(request_payload.clone()),
                response: Some([
                    Bytes::from_static(&[0_u8; RESPONSE_FRAME_BYTES]),
                    Bytes::from_static(&[0_u8; RESPONSE_FRAME_BYTES]),
                    Bytes::from_static(&[0_u8; RESPONSE_FRAME_BYTES]),
                ]),
            });
            time_sample_with_inputs(
                inputs,
                |sample_iters| operation.measure_thread().iterations(sample_iters),
                |input| {
                    let request = Request::post(uri.clone())
                        .body(body_builder.bytes(input.request.take().unwrap()))
                        .unwrap();
                    let mut request_body = request.into_body();
                    _ = block_on(request_body.frame()).unwrap().unwrap();

                    let frames = input.response.take().unwrap().map(BytesView::from);
                    let response = body_builder.body(ThreeFrameBody::new(frames), &HttpBodyOptions::default());
                    block_on(response.into_bytes()).unwrap()
                },
            )
        });
    });

    group.finish();
}

struct CompletePathInput {
    request: Option<BytesView>,
    response: Option<[Bytes; 3]>,
}

#[derive(Debug)]
struct ThreeFrameBody {
    frames: [Option<BytesView>; 3],
    next: usize,
}

impl ThreeFrameBody {
    fn new(frames: [BytesView; 3]) -> Self {
        let [first, second, third] = frames;
        Self {
            frames: [Some(first), Some(second), Some(third)],
            next: 0,
        }
    }
}

impl Body for ThreeFrameBody {
    type Data = BytesView;
    type Error = HttpError;

    fn poll_frame(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>>>> {
        let next = self.next;
        let frame = self.frames.get_mut(next).and_then(Option::take).map(Frame::data);
        self.next += usize::from(frame.is_some());
        Poll::Ready(frame.map(Ok))
    }

    fn is_end_stream(&self) -> bool {
        self.next == self.frames.len()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact((RESPONSE_BYTES - (self.next * RESPONSE_FRAME_BYTES)) as u64)
    }
}

criterion_group!(benches, entry);
criterion_main!(benches);
