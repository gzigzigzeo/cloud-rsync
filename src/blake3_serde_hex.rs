use serde::{
    de::{self, Visitor},
    Deserializer, Serializer,
};
use std::fmt;

pub fn serialize<S>(value: &blake3::Hash, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let hex = value.to_hex();
    let hex_str = hex.as_str();
    serializer.serialize_str(hex_str)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<blake3::Hash, D::Error>
where
    D: Deserializer<'de>,
{
    struct HashValueVisitor;

    impl<'de> Visitor<'de> for HashValueVisitor {
        type Value = blake3::Hash;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("Hash as string or array of integers")
        }

        fn visit_str<E>(self, v: &str) -> Result<blake3::Hash, E>
        where
            E: de::Error,
        {
            // Implementing From needs wrapper
            match blake3::Hash::from_hex(v) {
                Ok(hash) => Ok(hash),
                Err(e) => return Err(de::Error::custom(e)),
            }
        }
    }

    deserializer.deserialize_any(HashValueVisitor)
}
