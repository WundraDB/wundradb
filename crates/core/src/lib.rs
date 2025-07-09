pub mod sql;
pub mod storage;
pub mod txn;
pub mod raft;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

pub use sql::engine::SqlEngine;
pub use storage::bptree::BPlusTree;
pub use txn::wal::WriteAheadLog;

pub type DatabaseRef = Arc<RwLock<Database>>;

#[derive(Debug)]
pub struct Database {
    pub engine: SqlEngine,
    pub storage: BPlusTree,
    pub wal: WriteAheadLog,
}

impl Database {
    pub async fn new(data_dir: &str) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        
        let wal_path = format!("{}/wal.log", data_dir);
        let storage_path = format!("{}/storage.db", data_dir);
        
        let mut wal = WriteAheadLog::new(&wal_path).await?;
        let mut storage = BPlusTree::new();
        
        // Replay WAL entries to restore state
        let entries = wal.replay().await?;
        for entry in entries {
            if let Err(e) = storage.apply_wal_entry(&entry) {
                tracing::warn!("Failed to apply WAL entry: {}", e);
            }
        }
        
        // Try to load existing storage snapshot
        if let Err(e) = storage.load_from_disk(&storage_path) {
            tracing::info!("No existing storage found, starting fresh: {}", e);
        }
        
        let engine = SqlEngine::new(storage.clone(), wal.clone());
        
        Ok(Database {
            engine,
            storage,
            wal,
        })
    }
    
    pub async fn execute_sql(&mut self, sql: &str) -> Result<String> {
        self.engine.execute(sql).await
    }
    
    pub async fn shutdown(&mut self) -> Result<()> {
        self.wal.sync().await?;
        self.storage.save_to_disk("data/storage.db")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_database_creation() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::new(temp_dir.path().to_str().unwrap()).await;
        assert!(db.is_ok());
    }
    
    #[tokio::test]
    async fn test_sql_execution() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = Database::new(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        let result = db.execute_sql("CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR(100))").await;
        assert!(result.is_ok());
        
        let result = db.execute_sql("INSERT INTO test (id, name) VALUES (1, 'Alice')").await;
        assert!(result.is_ok());
        
        let result = db.execute_sql("SELECT * FROM test").await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("Alice"));
    }
}