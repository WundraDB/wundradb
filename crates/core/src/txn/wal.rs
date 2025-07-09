use crate::sql::engine::{Row, TableSchema};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub id: Uuid,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub operation: WalOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOperation {
    CreateTable(TableSchema),
    Insert {
        table: String,
        key: String,
        row: Row,
    },
}

#[derive(Debug)]
pub struct WriteAheadLog {
    path: String,
    entries: Vec<WalEntry>,
}

impl WriteAheadLog {
    pub async fn new(path: &str) -> Result<Self> {
        let wal = Self {
            path: path.to_string(),
            entries: Vec::new(),
        };
        
        // Create WAL file if it doesn't exist
        if !tokio::fs::metadata(&wal.path).await.is_ok() {
            tokio::fs::File::create(&wal.path).await?;
        }
        
        Ok(wal)
    }

    pub async fn append(&mut self, entry: &WalEntry) -> Result<()> {
        // Serialize entry
        let serialized = bincode::serialize(entry)?;
        let size = serialized.len() as u32;
        
        // Open file in append mode
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        
        // Write size prefix followed by entry
        file.write_all(&size.to_le_bytes()).await?;
        file.write_all(&serialized).await?;
        file.sync_all().await?;
        
        // Add to in-memory cache
        self.entries.push(entry.clone());
        
        Ok(())
    }

    pub async fn replay(&mut self) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        
        // Check if file exists and has content
        let metadata = match tokio::fs::metadata(&self.path).await {
            Ok(metadata) => metadata,
            Err(_) => return Ok(entries), // File doesn't exist, no entries to replay
        };
        
        if metadata.len() == 0 {
            return Ok(entries);
        }
        
        // Read all entries from file
        let file = tokio::fs::File::open(&self.path).await?;
        let mut reader = BufReader::new(file);
        
        loop {
            // Read size prefix
            let mut size_buf = [0u8; 4];
            match reader.read_exact(&mut size_buf).await {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            
            let size = u32::from_le_bytes(size_buf) as usize;
            
            // Read entry data
            let mut entry_buf = vec![0u8; size];
            reader.read_exact(&mut entry_buf).await?;
            
            // Deserialize entry
            let entry: WalEntry = bincode::deserialize(&entry_buf)?;
            entries.push(entry);
        }
        
        // Update in-memory cache
        self.entries = entries.clone();
        
        Ok(entries)
    }

    pub async fn sync(&mut self) -> Result<()> {
        // Force sync to disk
        let file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .await?;
        
        file.sync_all().await?;
        Ok(())
    }

    pub async fn truncate(&mut self) -> Result<()> {
        // Clear WAL file (used after successful checkpoint)
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)
            .await?;
        
        file.sync_all().await?;
        self.entries.clear();
        Ok(())
    }

    pub fn get_entries(&self) -> &[WalEntry] {
        &self.entries
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub async fn checkpoint(&mut self) -> Result<()> {
        // Perform checkpoint - sync to disk and optionally truncate
        self.sync().await?;
        
        // In a production system, you might want to:
        // 1. Ensure all data is persisted to storage
        // 2. Create a checkpoint marker
        // 3. Truncate the WAL to save space
        
        tracing::info!("WAL checkpoint completed with {} entries", self.entry_count());
        Ok(())
    }

    pub async fn get_entries_since(&self, timestamp: chrono::DateTime<chrono::Utc>) -> Vec<WalEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.timestamp > timestamp)
            .cloned()
            .collect()
    }

    pub async fn get_entries_for_table(&self, table_name: &str) -> Vec<WalEntry> {
        self.entries
            .iter()
            .filter(|entry| match &entry.operation {
                WalOperation::CreateTable(schema) => schema.name == table_name,
                WalOperation::Insert { table, .. } => table == table_name,
            })
            .cloned()
            .collect()
    }
}

impl Clone for WriteAheadLog {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            entries: self.entries.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::engine::{Column, Row, SqlDataType, SqlValue, TableSchema};
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_wal_append_and_replay() {
        let temp_file = NamedTempFile::new().unwrap();
        let wal_path = temp_file.path().to_str().unwrap();
        
        // Create WAL and append entry
        let mut wal = WriteAheadLog::new(wal_path).await.unwrap();
        
        let mut row_values = HashMap::new();
        row_values.insert("id".to_string(), SqlValue::Integer(1));
        row_values.insert("name".to_string(), SqlValue::Varchar("Alice".to_string()));
        
        let entry = WalEntry {
            id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            operation: WalOperation::Insert {
                table: "users".to_string(),
                key: "users:1".to_string(),
                row: Row { values: row_values },
            },
        };
        
        wal.append(&entry).await.unwrap();
        
        // Create new WAL instance and replay
        let mut new_wal = WriteAheadLog::new(wal_path).await.unwrap();
        let entries = new_wal.replay().await.unwrap();
        
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, entry.id);
        
        match &entries[0].operation {
            WalOperation::Insert { table, key, .. } => {
                assert_eq!(table, "users");
                assert_eq!(key, "users:1");
            }
            _ => panic!("Expected Insert operation"),
        }
    }

    #[tokio::test]
    async fn test_wal_create_table() {
        let temp_file = NamedTempFile::new().unwrap();
        let wal_path = temp_file.path().to_str().unwrap();
        
        let mut wal = WriteAheadLog::new(wal_path).await.unwrap();
        
        let schema = TableSchema {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: SqlDataType::Integer,
                    nullable: false,
                    primary_key: true,
                },
                Column {
                    name: "name".to_string(),
                    data_type: SqlDataType::Varchar(100),
                    nullable: false,
                    primary_key: false,
                },
            ],
        };
        
        let entry = WalEntry {
            id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            operation: WalOperation::CreateTable(schema.clone()),
        };
        
        wal.append(&entry).await.unwrap();
        
        // Replay and verify
        let mut new_wal = WriteAheadLog::new(wal_path).await.unwrap();
        let entries = new_wal.replay().await.unwrap();
        
        assert_eq!(entries.len(), 1);
        
        match &entries[0].operation {
            WalOperation::CreateTable(replayed_schema) => {
                assert_eq!(replayed_schema.name, "users");
                assert_eq!(replayed_schema.columns.len(), 2);
            }
            _ => panic!("Expected CreateTable operation"),
        }
    }

    #[tokio::test]
    async fn test_wal_multiple_entries() {
        let temp_file = NamedTempFile::new().unwrap();
        let wal_path = temp_file.path().to_str().unwrap();
        
        let mut wal = WriteAheadLog::new(wal_path).await.unwrap();
        
        // Append multiple entries
        for i in 1..=5 {
            let mut row_values = HashMap::new();
            row_values.insert("id".to_string(), SqlValue::Integer(i));
            row_values.insert("name".to_string(), SqlValue::Varchar(format!("User{}", i)));
            
            let entry = WalEntry {
                id: Uuid::new_v4(),
                timestamp: chrono::Utc::now(),
                operation: WalOperation::Insert {
                    table: "users".to_string(),
                    key: format!("users:{}", i),
                    row: Row { values: row_values },
                },
            };
            
            wal.append(&entry).await.unwrap();
        }
        
        // Replay and verify
        let mut new_wal = WriteAheadLog::new(wal_path).await.unwrap();
        let entries = new_wal.replay().await.unwrap();
        
        assert_eq!(entries.len(), 5);
        
        for (i, entry) in entries.iter().enumerate() {
            match &entry.operation {
                WalOperation::Insert { table, key, .. } => {
                    assert_eq!(table, "users");
                    assert_eq!(key, &format!("users:{}", i + 1));
                }
                _ => panic!("Expected Insert operation"),
            }
        }
    }

    #[tokio::test]
    async fn test_wal_sync() {
        let temp_file = NamedTempFile::new().unwrap();
        let wal_path = temp_file.path().to_str().unwrap();
        
        let mut wal = WriteAheadLog::new(wal_path).await.unwrap();
        
        let mut row_values = HashMap::new();
        row_values.insert("id".to_string(), SqlValue::Integer(1));
        
        let entry = WalEntry {
            id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            operation: WalOperation::Insert {
                table: "users".to_string(),
                key: "users:1".to_string(),
                row: Row { values: row_values },
            },
        };
        
        wal.append(&entry).await.unwrap();
        
        // Test sync
        let result = wal.sync().await;
        assert!(result.is_ok());
        
        // Test checkpoint
        let result = wal.checkpoint().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wal_truncate() {
        let temp_file = NamedTempFile::new().unwrap();
        let wal_path = temp_file.path().to_str().unwrap();
        
        let mut wal = WriteAheadLog::new(wal_path).await.unwrap();
        
        // Add some entries
        let mut row_values = HashMap::new();
        row_values.insert("id".to_string(), SqlValue::Integer(1));
        
        let entry = WalEntry {
            id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            operation: WalOperation::Insert {
                table: "users".to_string(),
                key: "users:1".to_string(),
                row: Row { values: row_values },
            },
        };
        
        wal.append(&entry).await.unwrap();
        assert_eq!(wal.entry_count(), 1);
        
        // Truncate
        wal.truncate().await.unwrap();
        assert_eq!(wal.entry_count(), 0);
        
        // Verify file is empty
        let metadata = tokio::fs::metadata(&wal.path).await.unwrap();
        assert_eq!(metadata.len(), 0);
    }

    #[tokio::test]
    async fn test_wal_filter_methods() {
        let temp_file = NamedTempFile::new().unwrap();
        let wal_path = temp_file.path().to_str().unwrap();
        
        let mut wal = WriteAheadLog::new(wal_path).await.unwrap();
        let now = chrono::Utc::now();
        
        // Add entries for different tables
        for (table, i) in [("users", 1), ("products", 2), ("users", 3)] {
            let mut row_values = HashMap::new();
            row_values.insert("id".to_string(), SqlValue::Integer(i));
            
            let entry = WalEntry {
                id: Uuid::new_v4(),
                timestamp: now + chrono::Duration::seconds(i),
                operation: WalOperation::Insert {
                    table: table.to_string(),
                    key: format!("{}:{}", table, i),
                    row: Row { values: row_values },
                },
            };
            
            wal.append(&entry).await.unwrap();
        }
        
        // Test filter by table
        let user_entries = wal.get_entries_for_table("users").await;
        assert_eq!(user_entries.len(), 2);
        
        let product_entries = wal.get_entries_for_table("products").await;
        assert_eq!(product_entries.len(), 1);
        
        // Test filter by time
        let since_entries = wal.get_entries_since(now + chrono::Duration::seconds(2)).await;
        assert_eq!(since_entries.len(), 1);
    }
}