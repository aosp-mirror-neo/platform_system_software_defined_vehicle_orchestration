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
//! Utility for connecting to binder with reconnect on death notification.

use binder::{DeathRecipient, IBinder};
use log::{debug, info, warn};
use mockall::automock;
use std::sync::{Arc, Mutex, MutexGuard};

// Type alias for the reconnect listener callback.
// Needed by mockall to enable automock for the set_reconnect_listener fn.
type ReconnectListener<T> = Box<dyn Fn(&binder::Strong<T>) + Send + Sync>;

/// Provides a mechanism to get notified when a binder connection has died, informing it through the reconnection listener.
#[automock]
pub trait BinderConnectionManager<T>: Send + Sync
where
    T: binder::FromIBinder + ?Sized,
{
    /// Returns the active binder connection.
    fn get_binder(&self) -> &Arc<Mutex<binder::Strong<T>>>;

    /// Set callback for binder reconnection.
    fn set_reconnect_listener(&self, listener: ReconnectListener<T>);
}

/// Connection to binder that reconnects on receiving death notification.
pub struct PersistentBinderConnection<T>
where
    T: binder::FromIBinder + ?Sized,
{
    binder: Arc<Mutex<binder::Strong<T>>>,
    binder_fetcher: Box<dyn Fn() -> binder::Strong<T> + Send + Sync + 'static>,
    death_recipient: Arc<Mutex<Option<DeathRecipient>>>,
    reconnect_listener: Arc<Mutex<Option<ReconnectListener<T>>>>,
}

impl<T> PersistentBinderConnection<T>
where
    T: binder::FromIBinder + ?Sized,
{
    /// Create new persistent binder connection.
    pub fn new(
        binder_fetcher: impl Fn() -> binder::Strong<T> + Send + Sync + 'static,
    ) -> Arc<Self> {
        let binder = Arc::new(Mutex::new(binder_fetcher()));
        let binder_guard = binder.lock().unwrap();

        let connection = Arc::new(Self {
            binder: binder.clone(),
            binder_fetcher: Box::new(binder_fetcher),
            death_recipient: Arc::new(Mutex::new(None)),
            reconnect_listener: Arc::new(Mutex::new(None)),
        });

        connection.link_to_death(&binder_guard);
        connection
    }

    fn link_to_death(self: &Arc<Self>, binder_guard: &MutexGuard<'_, binder::Strong<T>>) {
        let mut death_recipient = self.create_death_recipient();
        if binder_guard.as_binder().link_to_death(&mut death_recipient).is_err() {
            warn!("Unable to link to death");
        } else {
            info!("Linked to death");
        }
        *self.death_recipient.lock().unwrap() = Some(death_recipient);
    }

    fn create_death_recipient(self: &Arc<Self>) -> DeathRecipient {
        debug!("Creating death callback");
        let connection_weak = Arc::downgrade(self);

        let callback = move || {
            debug!("Death callback called");
            let Some(connection) = connection_weak.upgrade() else {
                debug!("Unable to get connection in death watcher callback");
                return;
            };
            let mut binder_guard = connection.binder.lock().unwrap();

            let new_binder = (connection.binder_fetcher)();

            let listener = connection.reconnect_listener.lock().unwrap();
            listener.as_ref().inspect(|listener| listener(&new_binder));

            *binder_guard = new_binder;
            connection.link_to_death(&binder_guard);

            debug!("Death callback exit")
        };

        DeathRecipient::new(callback)
    }
}

impl<T> BinderConnectionManager<T> for PersistentBinderConnection<T>
where
    T: binder::FromIBinder + ?Sized,
{
    fn get_binder(&self) -> &Arc<Mutex<binder::Strong<T>>> {
        &self.binder
    }

    fn set_reconnect_listener(&self, listener: ReconnectListener<T>) {
        *self.reconnect_listener.lock().unwrap() = Some(listener);
    }
}
