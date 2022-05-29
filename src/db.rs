use dashmap::{mapref::entry::Entry as MapEntry, DashMap};
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::time::{delay_queue::Key, DelayQueue};

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{cmd::SetBehaviour, proto::Value};

#[derive(Clone)]
pub struct Db {
    inner: Arc<DbInner>,
}

struct DbInner {
    /// The key-value data store.
    entries: DashMap<String, Entry>,
    /// Notifies the expiration task.
    background_task: mpsc::UnboundedSender<ExpirationUpdate>,
}

#[derive(Debug)]
enum ExpirationUpdate {
    Remove {
        key: Key,
    },
    Reset {
        key: Key,
        timeout: Duration,
    },
    Insert {
        value: String,
        timeout: Duration,
        return_key: oneshot::Sender<Key>,
    },
}

struct Entry {
    /// TODO: Consider storing the bytes instead
    value: Value,
    expires_at: Option<Instant>,
    expiration_key: Option<Key>,
}

async fn expiration_task(
    mut queue: DelayQueue<String>,
    mut rx: mpsc::UnboundedReceiver<ExpirationUpdate>,
    db: Db,
) {
    let mut has_items = !queue.is_empty();

    loop {
        tokio::select! {
            // Only poll this branch if the queue has items
            // else we would instantly always resolve to None
            // and block
            Some(item) = queue.next(), if has_items => {
                db.remove_raw(item.get_ref());
                has_items = !queue.is_empty();
            },
            Some(update) = rx.recv() => {
                match update {
                    ExpirationUpdate::Remove { key } => {
                        queue.remove(&key);
                    },
                    ExpirationUpdate::Reset { key, timeout } => {
                        queue.reset(&key, timeout);
                    },
                    ExpirationUpdate::Insert { value, timeout, return_key } => {
                        let key = queue.insert(value, timeout);
                        return_key.send(key).unwrap();
                        has_items = true;
                    }
                }
            }
        }
    }
}

impl Db {
    pub fn new() -> Self {
        let (background_task, background_receive) = mpsc::unbounded_channel();

        let inner = Arc::new(DbInner {
            entries: DashMap::new(),
            background_task,
        });
        let db = Self { inner };

        let expirations = DelayQueue::new();

        tokio::spawn(expiration_task(expirations, background_receive, db.clone()));

        db
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.inner.entries.get(key).map(|entry| entry.value.clone())
    }

    pub async fn set(
        &self,
        key: String,
        value: Value,
        expire: Option<Duration>,
        behaviour: SetBehaviour,
        keep_ttl: bool,
    ) -> Option<Value> {
        let map_entry = self.inner.entries.entry(key);
        let should_insert = match behaviour {
            SetBehaviour::Force => true,
            SetBehaviour::OnlyIfExists => matches!(map_entry, MapEntry::Occupied(_)),
            SetBehaviour::OnlyIfNotExists => matches!(map_entry, MapEntry::Vacant(_)),
        };

        if should_insert {
            match map_entry {
                MapEntry::Occupied(mut occupied_entry) => {
                    let old = occupied_entry.get_mut();

                    let prev = std::mem::replace(&mut old.value, value);

                    if !keep_ttl {
                        if let Some(expiration) = expire {
                            old.expires_at = Some(Instant::now() + expiration);

                            self.inner
                                .background_task
                                .send(ExpirationUpdate::Reset {
                                    key: old.expiration_key.unwrap(),
                                    timeout: expiration,
                                })
                                .unwrap();
                        }
                    }

                    Some(prev)
                }
                MapEntry::Vacant(vacant_entry) => {
                    let entry = if let Some(expiration) = expire {
                        let (tx, rx) = oneshot::channel();
                        self.inner
                            .background_task
                            .send(ExpirationUpdate::Insert {
                                value: vacant_entry.key().clone(),
                                timeout: expiration,
                                return_key: tx,
                            })
                            .unwrap();
                        let expiration_key = rx.await.unwrap();

                        Entry {
                            value,
                            expires_at: Some(Instant::now() + expiration),
                            expiration_key: Some(expiration_key),
                        }
                    } else {
                        Entry {
                            value,
                            expires_at: None,
                            expiration_key: None,
                        }
                    };

                    vacant_entry.insert(entry);

                    Some(Value::NullString)
                }
            }
        } else {
            None
        }
    }

    pub fn remove(&self, keys: Vec<String>) -> usize {
        let mut count = 0;

        for key in keys {
            if let Some((_, entry)) = self.inner.entries.remove(&key) {
                count += 1;

                if let Some(expiration_key) = entry.expiration_key {
                    self.inner
                        .background_task
                        .send(ExpirationUpdate::Remove {
                            key: expiration_key,
                        })
                        .unwrap();
                }
            };
        }

        count
    }

    pub fn remove_raw(&self, key: &str) {
        self.inner.entries.remove(key);
    }

    pub fn ttl(&self, key: &str) -> i64 {
        if let Some(value) = self.inner.entries.get(key) {
            if let Some(expiration) = value.expires_at {
                let remaining = expiration.checked_duration_since(Instant::now());

                if let Some(remaining) = remaining {
                    remaining.as_secs() as i64
                } else {
                    // About to get yeeted
                    -2
                }
            } else {
                -1
            }
        } else {
            -2
        }
    }

    pub fn pttl(&self, key: &str) -> i64 {
        if let Some(value) = self.inner.entries.get(key) {
            if let Some(expiration) = value.expires_at {
                let remaining = expiration.checked_duration_since(Instant::now());

                if let Some(remaining) = remaining {
                    remaining.as_millis() as i64
                } else {
                    // About to get yeeted
                    -2
                }
            } else {
                -1
            }
        } else {
            -2
        }
    }
}
