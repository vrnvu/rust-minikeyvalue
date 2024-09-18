use anyhow::Context;
use leveldb::database::Database;
use leveldb::kv::KV;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Deleted {
    No,
    Soft,
    Hard,
    Init, // TODO https://github.com/geohot/minikeyvalue/pull/48
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

impl TryFrom<Option<Vec<u8>>> for Record {
    type Error = anyhow::Error;

    fn try_from(value: Option<Vec<u8>>) -> anyhow::Result<Self> {
        match value {
            Some(data) => Self::from_bytes(&data),
            None => Ok(Record::default()),
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

    pub(crate) async fn get_record_or_default(&self, key: &str) -> anyhow::Result<Record> {
        let read_options = leveldb::options::ReadOptions::new();
        let leveldb_key = leveldb_key_from_str(key);

        let record = self
            .leveldb
            .get(read_options, leveldb_key)
            .with_context(|| format!("Failed to get key {} from LevelDB", key))?;

        record
            .try_into()
            .with_context(|| format!("Failed to deserialize record for key {}", key))
    }
}

pub fn get_remote_path(key: &str) -> String {
    let md5_key = md5::compute(key);
    let b64_key = base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE, key);

    format!("/{:02x}/{:02x}/{}", md5_key[0], md5_key[1], b64_key)
}

struct SortVol {
    score: Vec<u8>,
    volume: String,
}

/// `volumes`: Volumes to use for storing the data.
/// `replicas`: The number of replicas to create for the data. Default is 3.
/// `subvolumes`: The number of subvolumes, i.e., disks per machine. Default is 10.
pub fn get_volume(
    key: &str,
    volumes: Vec<String>,
    replicas: usize,
    subvolumes: u32,
) -> Vec<String> {
    // this is an intelligent way to pick the volume server for a file
    // stable in the volume server name (not position!)
    // and if more are added the correct portion will move (yay md5!)
    let mut sorted_volumes: Vec<SortVol> = volumes
        .into_iter()
        .map(|volume| {
            let mut hash_context = md5::Context::new();
            hash_context.consume(key);
            hash_context.consume(volume.as_bytes());
            let score = hash_context.compute();
            SortVol {
                score: score.to_vec(),
                volume: volume.clone(),
            }
        })
        .collect();

    sorted_volumes.sort_by(|a: &SortVol, b| a.score.cmp(&b.score).reverse());

    if replicas == 1 {
        return vec![sorted_volumes[0].volume.clone()];
    }

    sorted_volumes
        .into_iter()
        .take(replicas) // safe because sorted_volumes is sorted in descending order
        .map(|volume| {
            let subvolume_hash = (u32::from(volume.score[12]) << 24)
                + (u32::from(volume.score[13]) << 16)
                + (u32::from(volume.score[14]) << 8)
                + u32::from(volume.score[15]);
            format!("{}/sv{:02X}", volume.volume, subvolume_hash % subvolumes)
        })
        .collect::<Vec<String>>()
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
    fn test_record_try_from_none() -> anyhow::Result<()> {
        let record: Option<Vec<u8>> = None;
        let deserialized_record: Record = record.try_into()?;
        let expected_record = Record::default();
        assert_eq!(deserialized_record, expected_record);

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

    // TODO test get_volume and simplify the test cases so the logic is easier to understand
    #[test]
    fn test_get_volume() {
        let volumes = vec!["larry".to_string(), "moe".to_string(), "curly".to_string()];
        let tests = vec![
            ("hello", "larry"),
            ("helloworld", "curly"),
            ("world", "moe"),
            ("blah", "curly"),
            ("foo123", "moe"),
        ];

        for (key, expected_volume) in tests {
            let volume = get_volume(key, volumes.clone(), 1, 3);
            println!("{:?}", volume);
            let volume_path = volume.first().unwrap();
            assert_eq!(volume_path, expected_volume);
        }
    }

    #[test]
    fn test_get_volume_with_replicas() {
        let volumes = vec!["larry".to_string(), "moe".to_string(), "curly".to_string()];
        let tests = vec![
            ("hello", "larry/sv00"),
            ("helloworld", "curly/sv01"),
            ("world", "moe/sv02"),
            ("blah", "curly/sv01"),
            ("foo123", "moe/sv01"),
        ];

        for (key, expected_volume) in tests {
            let volume = get_volume(key, volumes.clone(), 3, 3);
            println!("{:?}", volume);
            let volume_path = volume.first().unwrap();
            assert_eq!(volume_path, expected_volume, "key: {}", key);
        }
    }

    #[test]
    fn test_get_volume_when_volumes_added_at_the_end() {
        let volumes = vec!["larry".to_string(), "moe".to_string(), "curly".to_string()];
        let tests = vec![
            ("hello", "larry"),
            ("helloworld", "curly"),
            ("world", "moe"),
            ("blah", "curly"),
            ("foo123", "moe"),
        ];

        for (key, expected_volume) in tests {
            let volume = get_volume(key, volumes.clone(), 1, 3);
            let volume_path = volume.first().unwrap();
            assert_eq!(volume_path, expected_volume);
        }

        // We added one volume `zzz`
        let volumes = vec![
            "larry".to_string(),
            "moe".to_string(),
            "curly".to_string(),
            "zzz".to_string(),
        ];
        let tests = vec![
            ("hello", "larry"),
            ("helloworld", "curly"),
            ("world", "moe"),
            ("blah", "curly"),
            ("foo123", "moe"),
            ("hash", "zzz"), // `hash` key should now be on `zzz` without touching the others
        ];
        for (key, expected_volume) in tests {
            let volume = get_volume(key, volumes.clone(), 1, 3);
            let volume_path = volume.first().unwrap();
            assert_eq!(volume_path, expected_volume);
        }
    }

    // TODO fix me
    // #[test]
    // fn test_get_volume_when_volumes_added_at_the_beginning() {
    //     let volumes = vec!["larry".to_string(), "moe".to_string(), "curly".to_string()];
    //     let tests = vec![
    //         ("hello", "larry"),
    //         ("helloworld", "curly"),
    //         ("world", "moe"),
    //         ("blah", "curly"),
    //         ("foo123", "moe"),
    //     ];

    //     for (key, expected_volume) in tests {
    //         let volume = get_volume(key, &volumes, 1, 3);
    //         let volume_path = volume.first().unwrap();
    //         assert_eq!(volume_path, expected_volume);
    //     }

    //     // We added one volume `aaa`
    //     let volumes = vec![
    //         "larry".to_string(),
    //         "moe".to_string(),
    //         "curly".to_string(),
    //         "aaa".to_string(),
    //     ];
    //     let tests = vec![
    //         ("hello", "larry"),
    //         ("helloworld", "curly"),
    //         ("world", "moe"),
    //         ("blah", "curly"),
    //         ("foo123", "moe"),
    //         ("9999899", "aaa"), // `hash` key should now be on `aaa` without touching the others
    //     ];
    //     for (key, expected_volume) in tests {
    //         let volume = get_volume(key, &volumes, 1, 3);
    //         let volume_path = volume.first().unwrap();
    //         assert_eq!(volume_path, expected_volume);
    //     }
    // }
}
