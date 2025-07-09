use crate::storage::bptree::BPlusTree;
use crate::txn::wal::{WriteAheadLog, WalEntry, WalOperation};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    ColumnDef, DataType, Expr, Ident, Query, SelectItem, SetExpr, Statement, TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlDataType {
    Integer,
    Varchar(u32),
    Decimal(u8, u8),
    Boolean,
    Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub values: HashMap<String, SqlValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlValue {
    Integer(i64),
    Varchar(String),
    Decimal(f64),
    Boolean(bool),
    Timestamp(chrono::DateTime<chrono::Utc>),
    Null,
}

#[derive(Debug, Clone)]
pub struct SqlEngine {
    storage: Arc<RwLock<BPlusTree>>,
    wal: Arc<RwLock<WriteAheadLog>>,
    schemas: Arc<RwLock<HashMap<String, TableSchema>>>,
}

impl SqlEngine {
    pub fn new(storage: BPlusTree, wal: WriteAheadLog) -> Self {
        Self {
            storage: Arc::new(RwLock::new(storage)),
            wal: Arc::new(RwLock::new(wal)),
            schemas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn execute(&self, sql: &str) -> Result<String> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| anyhow!("Parse error: {}", e))?;

        if ast.is_empty() {
            return Ok("No statement to execute".to_string());
        }

        match &ast[0] {
            Statement::CreateTable { name, columns, .. } => {
                self.execute_create_table(name, columns).await
            }
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.execute_insert(table_name, columns, source).await,
            Statement::Query(query) => self.execute_select(query).await,
            _ => Err(anyhow!("Unsupported statement type")),
        }
    }

    async fn execute_create_table(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        columns: &[ColumnDef],
    ) -> Result<String> {
        let name = table_name.to_string();
        let mut schema_columns = Vec::new();

        for col in columns {
            let column = Column {
                name: col.name.to_string(),
                data_type: self.convert_data_type(&col.data_type)?,
                nullable: col.options.iter().any(|opt| {
                    matches!(opt.option, sqlparser::ast::ColumnOption::Null)
                }),
                primary_key: col.options.iter().any(|opt| {
                    matches!(opt.option, sqlparser::ast::ColumnOption::Unique { is_primary: true })
                }),
            };
            schema_columns.push(column);
        }

        let schema = TableSchema {
            name: name.clone(),
            columns: schema_columns,
        };

        // Write to WAL first
        let wal_entry = WalEntry {
            id: uuid::Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            operation: WalOperation::CreateTable(schema.clone()),
        };
        
        {
            let mut wal = self.wal.write().await;
            wal.append(&wal_entry).await?;
        }

        // Update in-memory schema
        {
            let mut schemas = self.schemas.write().await;
            schemas.insert(name.clone(), schema);
        }

        Ok(format!("Table '{}' created successfully", name))
    }

    async fn execute_insert(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        columns: &[Ident],
        source: &Query,
    ) -> Result<String> {
        let table_name = table_name.to_string();
        
        // Get table schema
        let schema = {
            let schemas = self.schemas.read().await;
            schemas.get(&table_name)
                .ok_or_else(|| anyhow!("Table '{}' does not exist", table_name))?
                .clone()
        };

        // Parse values from INSERT statement
        let values = self.extract_insert_values(source)?;
        let column_names: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        
        let mut rows_inserted = 0;
        for value_row in values {
            let mut row = Row {
                values: HashMap::new(),
            };

            for (i, value) in value_row.iter().enumerate() {
                let column_name = if i < column_names.len() {
                    column_names[i].clone()
                } else {
                    return Err(anyhow!("Too many values provided"));
                };

                let sql_value = self.convert_value_to_sql_value(value)?;
                row.values.insert(column_name, sql_value);
            }

            // Generate key for the row (using primary key if available)
            let key = self.generate_row_key(&table_name, &row, &schema)?;

            // Write to WAL first
            let wal_entry = WalEntry {
                id: uuid::Uuid::new_v4(),
                timestamp: chrono::Utc::now(),
                operation: WalOperation::Insert {
                    table: table_name.clone(),
                    key: key.clone(),
                    row: row.clone(),
                },
            };
            
            {
                let mut wal = self.wal.write().await;
                wal.append(&wal_entry).await?;
            }

            // Insert into storage
            {
                let mut storage = self.storage.write().await;
                storage.insert(key, bincode::serialize(&row)?)?;
            }

            rows_inserted += 1;
        }

        Ok(format!("{} row(s) inserted", rows_inserted))
    }

    async fn execute_select(&self, query: &Query) -> Result<String> {
        match *query.body {
            SetExpr::Select(ref select) => {
                // Extract table name
                let table_name = match &select.from.first() {
                    Some(table) => match &table.relation {
                        TableFactor::Table { name, .. } => name.to_string(),
                        _ => return Err(anyhow!("Unsupported table factor")),
                    },
                    None => return Err(anyhow!("No table specified")),
                };

                // Get table schema
                let schema = {
                    let schemas = self.schemas.read().await;
                    schemas.get(&table_name)
                        .ok_or_else(|| anyhow!("Table '{}' does not exist", table_name))?
                        .clone()
                };

                // Read from storage
                let storage = self.storage.read().await;
                let all_keys = storage.scan_prefix(&format!("{}:", table_name))?;
                
                let mut rows = Vec::new();
                for key in all_keys {
                    if let Some(data) = storage.get(&key)? {
                        let row: Row = bincode::deserialize(&data)?;
                        rows.push(row);
                    }
                }

                // Apply WHERE clause if present
                if let Some(where_clause) = &select.selection {
                    rows = self.filter_rows(rows, where_clause)?;
                }

                // Apply ORDER BY if present
                if !query.order_by.is_empty() {
                    rows = self.sort_rows(rows, &query.order_by)?;
                }

                // Apply LIMIT if present
                if let Some(limit) = &query.limit {
                    if let Expr::Value(Value::Number(ref n, _)) = &limit {
                        let limit_count = n.parse::<usize>().unwrap_or(usize::MAX);
                        rows.truncate(limit_count);
                    }
                }

                // Format results
                self.format_select_results(&rows, &select.projection, &schema)
            }
            _ => Err(anyhow!("Unsupported query type")),
        }
    }

    fn extract_insert_values(&self, query: &Query) -> Result<Vec<Vec<Value>>> {
        match *query.body {
            SetExpr::Values(ref values) => {
                let mut result = Vec::new();
                for row in &values.rows {
                    let mut value_row = Vec::new();
                    for expr in row {
                        match expr {
                            Expr::Value(value) => value_row.push(value.clone()),
                            _ => return Err(anyhow!("Unsupported expression in VALUES")),
                        }
                    }
                    result.push(value_row);
                }
                Ok(result)
            }
            _ => Err(anyhow!("Unsupported INSERT source")),
        }
    }

    fn convert_data_type(&self, data_type: &DataType) -> Result<SqlDataType> {
        match data_type {
            DataType::Int(_) | DataType::Integer(_) => Ok(SqlDataType::Integer),
            DataType::Varchar(len) => {
                // len is Option<CharacterLength>
                let length = match len {
                    Some(cl) => {
                        // Use debug string to match variant
                        let s = format!("{:?}", cl);
                        if s.starts_with("Bounded") {
                            // Extract number from Bounded(n)
                            let start = s.find('(').unwrap_or(0) + 1;
                            let end = s.find(')').unwrap_or(s.len());
                            let num_str = &s[start..end];
                            num_str.parse::<u32>().unwrap_or(255)
                        } else if s == "Max" {
                            255
                        } else {
                            255
                        }
                    }
                    None => 255,
                };
                Ok(SqlDataType::Varchar(length))
            }
            DataType::Decimal(_) => {
                // Treat as unit variant, use default precision and scale
                Ok(SqlDataType::Decimal(10, 2))
            }
            DataType::Boolean => Ok(SqlDataType::Boolean),
            DataType::Timestamp(..) => Ok(SqlDataType::Timestamp),
            _ => Err(anyhow!("Unsupported data type: {:?}", data_type)),
        }
    }

    fn convert_value_to_sql_value(&self, value: &Value) -> Result<SqlValue> {
        match value {
            Value::Number(n, _) => {
                if n.contains('.') {
                    Ok(SqlValue::Decimal(n.parse()?))
                } else {
                    Ok(SqlValue::Integer(n.parse()?))
                }
            }
            Value::SingleQuotedString(s) => Ok(SqlValue::Varchar(s.clone())),
            Value::Boolean(b) => Ok(SqlValue::Boolean(*b)),
            Value::Null => Ok(SqlValue::Null),
            _ => Err(anyhow!("Unsupported value type: {:?}", value)),
        }
    }

    fn generate_row_key(&self, table_name: &str, row: &Row, schema: &TableSchema) -> Result<String> {
        // Try to use primary key
        for column in &schema.columns {
            if column.primary_key {
                if let Some(value) = row.values.get(&column.name) {
                    return Ok(format!("{}:{}", table_name, self.sql_value_to_string(value)));
                }
            }
        }
        
        // Fallback to UUID if no primary key
        Ok(format!("{}:{}", table_name, uuid::Uuid::new_v4()))
    }

    fn sql_value_to_string(&self, value: &SqlValue) -> String {
        match value {
            SqlValue::Integer(i) => i.to_string(),
            SqlValue::Varchar(s) => s.clone(),
            SqlValue::Decimal(d) => d.to_string(),
            SqlValue::Boolean(b) => b.to_string(),
            SqlValue::Timestamp(t) => t.to_rfc3339(),
            SqlValue::Null => "null".to_string(),
        }
    }

    fn filter_rows(&self, rows: Vec<Row>, _where_clause: &Expr) -> Result<Vec<Row>> {
        // Simplified WHERE clause handling - just return all rows for now
        // In a full implementation, this would parse and evaluate the WHERE expression
        Ok(rows)
    }

    fn sort_rows(&self, rows: Vec<Row>, _order_by: &[sqlparser::ast::OrderByExpr]) -> Result<Vec<Row>> {
        // Simplified ORDER BY handling - just return rows as-is for now
        // In a full implementation, this would sort based on the ORDER BY clause
        Ok(rows)
    }

    fn format_select_results(&self, rows: &[Row], projection: &[SelectItem], schema: &TableSchema) -> Result<String> {
        let mut result = String::new();
        
        // Determine which columns to show
        let columns: Vec<String> = match projection.first() {
            Some(SelectItem::Wildcard(..)) => {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            }
            _ => {
                let mut cols = Vec::new();
                for item in projection {
                    match item {
                        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                            cols.push(ident.to_string());
                        }
                        _ => {
                            // Handle other projection types as needed
                            cols.push("*".to_string());
                        }
                    }
                }
                cols
            }
        };

        // Header
        result.push_str(&columns.join("\t"));
        result.push('\n');
        result.push_str(&"-".repeat(columns.len() * 10));
        result.push('\n');

        // Data rows
        for row in rows {
            let mut row_values = Vec::new();
            for col in &columns {
                let value = row.values.get(col)
                    .map(|v| self.sql_value_to_string(v))
                    .unwrap_or_else(|| "NULL".to_string());
                row_values.push(value);
            }
            result.push_str(&row_values.join("\t"));
            result.push('\n');
        }

        if rows.is_empty() {
            result.push_str("(0 rows)\n");
        } else {
            result.push_str(&format!("({} rows)\n", rows.len()));
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::bptree::BPlusTree;
    use crate::txn::wal::WriteAheadLog;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_table() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let storage = BPlusTree::new();
        let wal = WriteAheadLog::new(wal_path.to_str().unwrap()).await.unwrap();
        let engine = SqlEngine::new(storage, wal);

        let result = engine.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))").await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("created successfully"));
    }

    #[tokio::test]
    async fn test_insert_and_select() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let storage = BPlusTree::new();
        let wal = WriteAheadLog::new(wal_path.to_str().unwrap()).await.unwrap();
        let engine = SqlEngine::new(storage, wal);

        // Create table
        engine.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))").await.unwrap();
        
        // Insert data
        let result = engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')").await;
        assert!(result.is_ok());

        // Select data
        let result = engine.execute("SELECT * FROM users").await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("Alice"));
    }
}