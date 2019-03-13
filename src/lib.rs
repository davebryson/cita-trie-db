///! RocksDB implementation for the cita-trie.
///!
///! #Example:
///!
///!```
///! // Open or create a database in the 'data' directory
///! let mut db = RocksDb::new("data");
///!
///! // Create the trie
///! let mut trie = PatriciaTrie::new(&mut db, RLPNodeCodec::default());
///!
///! // Insert stuff
///! trie.insert(b"hello-1", b"value-1").unwrap();
///! ...
///!```
///!
extern crate cita_trie;
extern crate rocksdb;

use cita_trie::db::DB;
use rocksdb::{Writable, DB as RDB};
use std::error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Wrapper for RocksDb errors that are all Strings
#[derive(Debug)]
pub struct RocksDbError(pub String);

impl From<String> for RocksDbError {
    fn from(err: String) -> RocksDbError {
        RocksDbError(err)
    }
}
impl Display for RocksDbError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "RocksDb error: {}", self.0)
    }
}
impl error::Error for RocksDbError {
    fn description(&self) -> &str {
        &self.0
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}

/// Handle to RocksDb
pub struct RocksDb {
    db: Arc<rocksdb::DB>,
}

impl RocksDb {
    /// Create or open a database at the give path.  Will panic on error
    pub fn new(dir: &str) -> Self {
        match RDB::open_default(dir) {
            Ok(db) => RocksDb { db: Arc::new(db) },
            Err(reason) => panic!(reason),
        }
    }
}

// Implemented to satisfy the DB Trait
impl fmt::Debug for RocksDb {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "rocksdb trie stores")
    }
}

impl DB for RocksDb {
    type Error = RocksDbError;
    /// Get a value from the database.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        match self.db.get(key) {
            Ok(Some(val)) => Ok(Some(val.to_owned())),
            Err(reason) => Err(RocksDbError::from(reason)),
            Ok(None) => Err(RocksDbError::from(String::from("Key not found"))),
        }
    }

    /// Insert a key value
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.db.put(key, value).map_err(|r| RocksDbError::from(r))
    }

    /// Check if a key is in the database
    fn contains(&self, key: &[u8]) -> Result<bool, Self::Error> {
        if let Ok(Some(_)) = self.get(key) {
            return Ok(true);
        }
        return Ok(false);
    }

    /// Remove a key/value pair
    fn remove(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        self.db.delete(key).map_err(|r| RocksDbError::from(r))
    }
}

#[cfg(test)]
mod tests {
    use super::RocksDb;
    use crate::cita_trie::codec::RLPNodeCodec;
    use crate::cita_trie::trie::{PatriciaTrie, Trie};
    use std::fs;

    #[test]
    fn test_rocksdb_trie_basics() {
        let test_dir = "data";
        let root1 = {
            let mut db = RocksDb::new(test_dir);
            let mut trie = PatriciaTrie::new(&mut db, RLPNodeCodec::default());
            trie.insert(b"hello-1", b"value-1").unwrap();
            trie.insert(b"hello-2", b"value-2").unwrap();
            trie.insert(b"hello-3", b"value-3").unwrap();
            let root = trie.root();
            assert!(root.is_ok());
            root.unwrap()
        }; // Note: rocksdb is dropped here

        let mut db1 = RocksDb::new(test_dir);
        let mut trie = PatriciaTrie::from(&mut db1, RLPNodeCodec::default(), &root1).unwrap();

        assert_eq!(true, trie.contains(b"hello-1").unwrap());
        assert_eq!(true, trie.contains(b"hello-3").unwrap());
        assert_eq!(false, trie.contains(b"NOPE").unwrap());

        let val = trie.get(b"hello-2").unwrap().unwrap();
        assert_eq!(b"value-2", val.as_slice());

        assert!(trie.remove(b"hello-3").is_ok());
        let _ = trie.root(); // Does commit...
        assert_eq!(false, trie.contains(b"hello-3").unwrap());

        let _ = fs::remove_dir_all(test_dir);
    }
}
