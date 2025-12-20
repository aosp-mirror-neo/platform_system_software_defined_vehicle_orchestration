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
//! Contains mocks used for vehicle modes message publication.

use futures::channel::mpsc;
use futures::Stream;
use protobuf::Message;
use sdv_mw_rs_comms::codec;
use sdv_mw_rs_comms::ByteBuffersStream;
use sdv_mw_rs_comms::InstantRead;
use sdv_mw_rs_comms::Publication;
use sdv_mw_rs_comms::Publish;
use sdv_mw_rs_comms::ReadHistory;
use sdv_mw_rs_comms::Subscribe;
use sdv_mw_rs_comms::SubscribeOptions;
use sdv_status_rs::{SdvResult, SdvStatus, SdvStatusCode};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

/// Mocked Publisher used in test to wait until the observer has been registered before publishing messages.
pub struct MockPublisher<M: Message> {
    /// Publish
    pub publish_handle: Box<dyn Publish>,
    _data: PhantomData<M>,
    /// Receiver used to notify when an observer has been registered.
    pub receiver: Receiver<()>,
}

impl<M: Message> MockPublisher<M> {
    /// Creates a new MockPublisher
    pub fn new(publish_handle: Box<dyn Publish>, receiver: Receiver<()>) -> Self {
        Self { publish_handle, _data: PhantomData, receiver }
    }

    /// Publish a message
    pub async fn publish(&mut self, message: &M) -> SdvResult<()> {
        self.publish_batch([message]).await
    }

    /// Publish a batch of messages
    pub async fn publish_batch<'a>(
        &mut self,
        messages: impl IntoIterator<Item = &'a M>,
    ) -> SdvResult<()> {
        // We need to wait until the observer was registered to publish a message.
        // Otherwise the test will fail because there are no observers.
        // We do it with a channel because this happens in multiple threads.
        self.receiver.recv().await;
        let byte_buffers: SdvResult<Vec<Box<[u8]>>> =
            messages.into_iter().map(|msg| codec::encode(msg)).collect();
        if let Ok(_byte_buffers) = byte_buffers {
            let res = self.publish_handle.publish_messages(&_byte_buffers);
            assert!(res.is_ok());
        }
        Ok(())
    }
}

/// Mock for the MessageQueue
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

    /// Observer messages
    pub fn observe(&mut self) -> Pin<Box<dyn Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send>> {
        let (sender, receiver) = mpsc::channel::<SdvResult<(u32, Vec<u8>)>>(10);
        self.senders.push(sender);
        Box::pin(receiver)
    }
}

/// Mocks the pubsub publisher.
pub struct MockPublish {
    message_queue: Arc<Mutex<MockMessageQueue>>,
}

impl MockPublish {
    /// Creates a new MockPublish
    pub fn new(message_queue: Arc<Mutex<MockMessageQueue>>) -> Self {
        Self { message_queue }
    }
}

impl Publish for MockPublish {
    fn publish_messages(&self, messages: &[Box<[u8]>]) -> SdvResult<()> {
        self.message_queue.lock().unwrap().publish_messages(messages);
        Ok(())
    }
}

/// Mocks the pubsub observer.
#[derive(Clone)]
pub struct MockPublication {
    message_queue: Arc<Mutex<MockMessageQueue>>,
    sender: Sender<()>,
}

impl MockPublication {
    /// Creates a new MockPublication
    pub fn new(message_queue: Arc<Mutex<MockMessageQueue>>, sender: Sender<()>) -> Self {
        Self { message_queue, sender }
    }
}

#[async_trait::async_trait]
impl Publication for MockPublication {
    async fn subscribe(&self, _options: &SubscribeOptions) -> SdvResult<Box<dyn Subscribe>> {
        unimplemented!()
    }

    async fn observe(&self, _options: &SubscribeOptions) -> SdvResult<ByteBuffersStream> {
        let stream = self.message_queue.lock().unwrap().observe();
        // Notify that an observer has been added.
        // We don't care about result as we don't need to publish messages in all tests
        let _ = self.sender.send(()).await;
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
