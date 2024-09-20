use anyhow::Context;
use leveldb::database::Database;
use leveldb::kv::KV;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Deleted {
    No,
    Soft,
    Hard,
    Init,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Record {
    deleted: Deleted,
    hash: String,
    read_volumes: Vec<String>,
}

impl Record {
    pub(crate) fn new(deleted: Deleted, hash: String, read_volumes: Vec<String>) -> Self {
        Self {
            deleted,
            hash,
            read_volumes,
        }
    }

    pub(crate) fn deleted(&self) -> Deleted {
        self.deleted
    }

    pub(crate) fn hash(&self) -> &str {
        &self.hash
    }

    pub(crate) fn read_volumes(&self) -> &Vec<String> {
        &self.read_volumes
    }

    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| anyhow::anyhow!("Serialization error: {}", e))
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        bincode::deserialize(bytes).map_err(|e| anyhow::anyhow!("Deserialization error: {}", e))
    }
}

impl Default for Record {
    fn default() -> Self {
        Self {
            deleted: Deleted::Init,
            hash: String::new(),
            read_volumes: Vec::new(),
        }
    }
}

pub(crate) type LevelDbKey = i32;

pub(crate) fn leveldb_key_from_str(key: &str) -> LevelDbKey {
    // TODO make sure i32 is always positive and use only the lower 31 bits of the hash
    let leveldb_key: i32 = (gxhash::gxhash32(key.as_bytes(), 0) & 0x7FFFFFFF) as i32;
    leveldb_key
}

pub(crate) struct LevelDb {
    leveldb: Database<LevelDbKey>,
}

impl LevelDb {
    pub(crate) fn new(ldb_path: &std::path::Path) -> anyhow::Result<Self> {
        let mut leveldb_options = leveldb::options::Options::new();
        leveldb_options.create_if_missing = true;

        let leveldb = leveldb::database::Database::open(ldb_path, leveldb_options)
            .with_context(|| format!("Failed to open LevelDB at path: {}", ldb_path.display()))?;

        Ok(Self { leveldb })
    }

    pub(crate) async fn put_record(&self, key: &str, record: Record) -> anyhow::Result<()> {
        let leveldb_key = leveldb_key_from_str(key);
        let write_options = leveldb::options::WriteOptions::new();
        self.leveldb
            .put(write_options, leveldb_key, &record.to_bytes()?)
            .with_context(|| {
                format!(
                    "Failed to put record for key {} and leveldb_key {}",
                    key, leveldb_key
                )
            })?;
        Ok(())
    }

    pub(crate) async fn get_record(&self, key: &str) -> anyhow::Result<Option<Record>> {
        let read_options = leveldb::options::ReadOptions::new();
        let leveldb_key = leveldb_key_from_str(key);

        let record = self
            .leveldb
            .get(read_options, leveldb_key)
            .with_context(|| format!("Failed to get key {} from LevelDB", key))?;

        if let Some(record) = record {
            Ok(Some(Record::from_bytes(&record)?))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn get_record_or_default(&self, key: &str) -> anyhow::Result<Record> {
        let record = self.get_record(key).await?;
        Ok(record.unwrap_or(Record::default()))
    }
}

pub fn get_remote_path(key: &str) -> String {
    let md5_key = md5::compute(key);
    let b64_key = base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE, key);

    format!("/{:02x}/{:02x}/{}", md5_key[0], md5_key[1], b64_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_to_and_from_bytes() -> anyhow::Result<()> {
        let record = Record {
            deleted: Deleted::Hard,
            hash: "1234567890".to_string(),
            read_volumes: vec!["vol1".to_string(), "vol2".to_string()],
        };
        let bytes = record.to_bytes()?;
        let deserialized_record = Record::from_bytes(&bytes)?;
        assert_eq!(record, deserialized_record);

        Ok(())
    }

    #[test]
    fn test_record_from_slice_bytes() -> anyhow::Result<()> {
        let bytes = [
            2, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 49, 50, 51, 52, 53, 54, 55, 56, 57, 48, 2, 0, 0,
            0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 118, 111, 108, 49, 4, 0, 0, 0, 0, 0, 0, 0, 118,
            111, 108, 50,
        ];
        let record = Record::from_bytes(&bytes)?;

        let expected_record = Record {
            deleted: Deleted::Hard,
            hash: "1234567890".to_string(),
            read_volumes: vec!["vol1".to_string(), "vol2".to_string()],
        };

        assert_eq!(record, expected_record);

        Ok(())
    }

    #[test]
    fn test_record_default() -> anyhow::Result<()> {
        let record = Record::default();
        let expected_record = Record {
            deleted: Deleted::Init,
            hash: String::new(),
            read_volumes: Vec::new(),
        };
        assert_eq!(record, expected_record);

        Ok(())
    }

    #[test]
    fn test_record_with_empty_read_volumes() -> anyhow::Result<()> {
        let record = Record {
            deleted: Deleted::Hard,
            hash: "1234567890".to_string(),
            read_volumes: Vec::new(),
        };
        let bytes = record.to_bytes()?;
        let deserialized_record = Record::from_bytes(&bytes)?;
        assert_eq!(record, deserialized_record);

        Ok(())
    }

    #[test]
    fn test_get_remote_path() {
        let tests = vec![
            ("hello", "/5d/41/aGVsbG8="),
            ("helloworld", "/fc/5e/aGVsbG93b3JsZA=="),
        ];

        for (key, expected_path) in tests {
            let path = get_remote_path(key);
            assert_eq!(path, expected_path);
        }
    }
}
