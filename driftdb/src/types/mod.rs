use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Display;
use crate::{StoreInstruction, Store};

pub mod key_seq_pair;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Default, Deserialize, Hash)]
pub struct Key(String);

impl Key {
    pub fn new(s: String) -> Self {
        Key(s)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Key(s.to_string())
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default, PartialOrd, Ord, Hash,
)]
pub struct SequenceNumber(pub u64);

impl SequenceNumber {
    pub fn next(&self) -> Self {
        SequenceNumber(self.0 + 1)
    }
}

impl Display for SequenceNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Action {
    /// Broadcast to relavent clients without altering the stream.
    Relay,

    /// Append to the stream.
    Append,

    /// Replace the entire stream.
    Replace,

    /// Replace the entire stream up to the given sequence number.
    /// If the stream has already been rolled up to an equal or greater
    /// sequence number, this is ignored.
    Compact { seq: SequenceNumber },
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageToDatabase {
    Push {
        /// Key to push to.
        key: Key,

        /// Value to push.
        value: Value,

        /// Describes the action that this should have on the state.
        action: Action,
    },
    Get {
        /// Key to get.
        key: Key,
        /// Sequence number to start from.
        #[serde(default)]
        seq: SequenceNumber,
    },
    Ping {
        nonce: Option<u64>,
    },
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct SequenceValue {
    pub value: Value,
    pub seq: SequenceNumber,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub enum ReplicaInstruction {
    StoreInstruction(StoreInstruction),
    InitInstruction(Store),
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageFromDatabase {
    Push {
        key: Key,
        value: Value,
        seq: SequenceNumber,
    },
    Init {
        key: Key,
        data: Vec<SequenceValue>,
    },
    Error {
        message: String,
    },
    StreamSize {
        key: Key,
        size: usize,
    },
    Pong {
        nonce: Option<u64>,
    },
    ReplicaInstruction(ReplicaInstruction),
}
