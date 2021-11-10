use serde::de::Unexpected;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::fmt::{Debug, Display, Formatter};
use std::num::NonZeroU64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMessage {
    pub id: String,
    pub sequence: Sequence,
    pub payload: Value,
}

pub type Sequence = Option<SequenceId>;

pub type SequenceId = NonZeroU64;

pub type RequestSequence = Option<RequestSequenceId>;

#[derive(Debug, Copy, Clone)]
pub enum RequestSequenceId {
    Id(SequenceId),
    Last,
    First,
}

impl Serialize for RequestSequenceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            RequestSequenceId::Id(s) => serializer.serialize_u64(s.get()),
            RequestSequenceId::Last => serializer.serialize_str("last"),
            RequestSequenceId::First => serializer.serialize_str("first"),
        }
    }
}

impl<'de> Deserialize<'de> for RequestSequenceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SequenceVisitor;

        impl<'de> serde::de::Visitor<'de> for SequenceVisitor {
            type Value = RequestSequenceId;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("a non zero positive value or \"last\"")
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if let Some(id) = SequenceId::new(v) {
                    return Ok(RequestSequenceId::Id(id));
                }

                Err(E::invalid_value(Unexpected::Unsigned(v), &"positive value"))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v == RequestSequenceId::Last.to_string() {
                    return Ok(RequestSequenceId::Last);
                }
                if v == RequestSequenceId::First.to_string() {
                    return Ok(RequestSequenceId::First);
                }

                if let Ok(id) = v.parse::<u64>() {
                    return self.visit_u64(id);
                }

                Err(E::invalid_value(Unexpected::Str(v), &"last"))
            }
        }

        deserializer.deserialize_str(SequenceVisitor)
    }
}

impl Display for RequestSequenceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestSequenceId::Id(s) => write!(f, "{}", s),
            RequestSequenceId::Last => write!(f, "last"),
            RequestSequenceId::First => write!(f, "first"),
        }
    }
}

impl UniqId for EventMessage {
    fn get_id(&self) -> &str {
        &self.id
    }
    fn get_sequence(&self) -> Sequence {
        self.sequence
    }

    fn set_sequence(&mut self, sequence: SequenceId) -> Sequence {
        self.sequence.replace(sequence)
    }
}

pub trait UniqId {
    fn get_id(&self) -> &str;
    fn get_sequence(&self) -> Sequence;
    fn set_sequence(&mut self, sequence: SequenceId) -> Sequence;
}
