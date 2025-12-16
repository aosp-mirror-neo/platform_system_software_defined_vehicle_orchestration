/*
 * Copyright (C) 2026 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//! Contains mocks used for custom modes message publication.

use futures::channel::mpsc;
use futures::Stream;
use sdv_mw_rs_comms::ByteBuffersStream;
use sdv_mw_rs_comms::InstantRead;
use sdv_mw_rs_comms::Publication;
use sdv_mw_rs_comms::Publish;
use sdv_mw_rs_comms::ReadHistory;
use sdv_mw_rs_comms::Subscribe;
use sdv_mw_rs_comms::SubscribeOptions;
use sdv_status_rs::{SdvResult, SdvStatus, SdvStatusCode};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

/// Struct used by the MockPublisher and MockPublication to interact with the pubsub sender.
/// The MockPublisher publishes the messages sent by the SetCustomState request (RPC), mocking a new
/// message (GlobalCustomStateChange) publishes in pubsub.
/// The MockPublication creates the receiver (setting up the sender for the MockPublisher) that will
/// be observed (subscribing to new messages), getting notified whenever a new message (GlobalCustomStateChange)
/// is available through pubsub.
pub struct MockMessageQueue {
    senders: Vec<mpsc::Sender<SdvResult<(u32, Vec<u8>)>>>,
}

impl Default for MockMessageQueue {
    /// Creates a new MockMessageQueue
    fn default() -> Self {
        Self { senders: Vec::new() }
    }
}

impl MockMessageQueue {
    /// Publish messages
    pub fn publish_messages(&mut self, messages: &[Box<[u8]>]) {
        let message_count = messages.len() as u32;
        let concat: Vec<u8> = messages.iter().flat_map(|arr| arr.iter().copied()).collect();

        self.senders.retain_mut(|s| {
            s.try_send(Ok((message_count, concat.clone()))).expect("Receiver is gone");
            true
        });
    }

    /// Drops all the senders so that the observed stream closes
    pub fn drop_stream(&mut self) {
        // Sender will be dropped when they are removed from the vec.
        self.senders.clear();
    }

    /// Observe messages
    pub fn observe(&mut self) -> Pin<Box<dyn Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send>> {
        let (sender, receiver) = mpsc::channel::<SdvResult<(u32, Vec<u8>)>>(10);
        self.senders.push(sender);
        Box::pin(receiver)
    }
}

/// Mocks the pubsub publisher.
pub struct MockPublisher {
    message_queue: Arc<Mutex<MockMessageQueue>>,
}

impl MockPublisher {
    /// Create a new instance of a MockPublisher
    pub fn new(message_queue: Arc<Mutex<MockMessageQueue>>) -> Self {
        Self { message_queue }
    }
}

impl Publish for MockPublisher {
    fn publish_messages(&self, messages: &[Box<[u8]>]) -> SdvResult<()> {
        self.message_queue.lock().unwrap().publish_messages(messages);
        Ok(())
    }
}

/// Mocks the pubsub observer.
#[derive(Clone)]
pub struct MockPublication {
    message_queue: Arc<Mutex<MockMessageQueue>>,
}

impl MockPublication {
    /// Creates a new instance of a MockPublication
    pub fn new(message_queue: Arc<Mutex<MockMessageQueue>>) -> Self {
        Self { message_queue }
    }
}

#[async_trait::async_trait]
impl Publication for MockPublication {
    async fn subscribe(&self, _options: &SubscribeOptions) -> SdvResult<Box<dyn Subscribe>> {
        unimplemented!()
    }

    async fn observe(&self, _options: &SubscribeOptions) -> SdvResult<ByteBuffersStream> {
        let stream = self.message_queue.lock().unwrap().observe();
        Ok(stream)
    }

    async fn create_history(&self, _options: &SubscribeOptions) -> SdvResult<Box<dyn ReadHistory>> {
        unimplemented!()
    }

    fn create_instant_read(&self) -> Box<dyn InstantRead> {
        unimplemented!()
    }
}

#[derive(Default)]
/// Mocks the pubsub observer that will fail whenever trying to observe.
pub struct FailureMockPublication {}

#[async_trait::async_trait]
impl Publication for FailureMockPublication {
    async fn subscribe(&self, _options: &SubscribeOptions) -> SdvResult<Box<dyn Subscribe>> {
        unimplemented!()
    }

    async fn observe(&self, _options: &SubscribeOptions) -> SdvResult<ByteBuffersStream> {
        Err(SdvStatus::new(SdvStatusCode::Unknown))
    }

    async fn create_history(&self, _options: &SubscribeOptions) -> SdvResult<Box<dyn ReadHistory>> {
        unimplemented!()
    }

    fn create_instant_read(&self) -> Box<dyn InstantRead> {
        unimplemented!()
    }
}
