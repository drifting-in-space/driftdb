use crate::websocket::WebSocketConnection;
use driftdb::{types::ReplicaInstruction, MessageFromDatabase, Store};
use std::{
    rc::Rc,
    sync::{RwLock, RwLockReadGuard},
};
use yew::{hook, use_force_update, use_memo};

pub struct SyncStore {
    store: Rc<RwLock<Store>>,
    _socket: WebSocketConnection<MessageFromDatabase, ()>,
}

impl SyncStore {
    pub fn new<F>(url: &str, callback: F) -> Self
    where
        F: Fn() -> () + 'static,
    {
        let store = Rc::new(RwLock::new(Store::default()));

        let socket = {
            let store = store.clone();
            WebSocketConnection::new(url, move |message| {
                match message {
                    MessageFromDatabase::ReplicaInstruction(
                        ReplicaInstruction::StoreInstruction(store_instruction),
                    ) => {
                        store.write().unwrap().apply(&store_instruction);

                        callback();
                    }

                    MessageFromDatabase::ReplicaInstruction(
                        ReplicaInstruction::InitInstruction(new_store),
                    ) => {
                        *store.write().unwrap() = new_store;

                        callback();
                    }

                    _ => {}
                }
            })
            .unwrap()
        };

        SyncStore { store, _socket: socket }
    }

    pub fn store(&self) -> RwLockReadGuard<Store> {
        self.store.read().unwrap()
    }
}

#[hook]
pub fn use_sync_store(url: &str) -> Rc<SyncStore> {
    let force_update = use_force_update();
    let sync_store = use_memo(
        move |_| SyncStore::new(url, move || force_update.force_update()),
        (),
    );

    sync_store
}
