use crate::txn::wal::WalEntry;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

const NODE_SIZE: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BPlusTree {
    root: Option<NodeId>,
    nodes: BTreeMap<NodeId, Node>,
    next_node_id: NodeId,
    leaf_head: Option<NodeId>,
    operation_count: usize,
}

type NodeId = u64;
type Key = String;
type Value = Vec<u8>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Node {
    id: NodeId,
    is_leaf: bool,
    keys: Vec<Key>,
    values: Vec<Value>,
    children: Vec<NodeId>,
    next_leaf: Option<NodeId>,
    prev_leaf: Option<NodeId>,
}

impl Node {
    fn new(id: NodeId, is_leaf: bool) -> Self {
        Self {
            id,
            is_leaf,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            next_leaf: None,
            prev_leaf: None,
        }
    }

    fn is_full(&self) -> bool {
        self.keys.len() >= NODE_SIZE
    }

    fn find_key_index(&self, key: &str) -> usize {
        self.keys.binary_search_by(|k| k.as_str().cmp(key)).unwrap_or_else(|i| i)
    }
}

impl BPlusTree {
    pub fn new() -> Self {
        Self {
            root: None,
            nodes: BTreeMap::new(),
            next_node_id: 1,
            leaf_head: None,
            operation_count: 0,
        }
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Result<()> {
        if self.root.is_none() {
            // Create root node
            let root_id = self.allocate_node_id();
            let mut root = Node::new(root_id, true);
            root.keys.push(key);
            root.values.push(value);
            
            self.nodes.insert(root_id, root);
            self.root = Some(root_id);
            self.leaf_head = Some(root_id);
        } else {
            let root_id = self.root.unwrap();
            let new_root = self.insert_recursive(root_id, key, value)?;
            if let Some(new_root_id) = new_root {
                self.root = Some(new_root_id);
            }
        }
        
        self.operation_count += 1;
        Ok(())
    }

    fn insert_recursive(&mut self, node_id: NodeId, key: Key, value: Value) -> Result<Option<NodeId>> {
        let node = self.nodes.get(&node_id).unwrap().clone();
        
        if node.is_leaf {
            self.insert_into_leaf(node_id, key, value)
        } else {
            self.insert_into_internal(node_id, key, value)
        }
    }

    fn insert_into_leaf(&mut self, node_id: NodeId, key: Key, value: Value) -> Result<Option<NodeId>> {
        let node = self.nodes.get_mut(&node_id).unwrap();
        let index = node.find_key_index(&key);
        
        if index < node.keys.len() && node.keys[index] == key {
            // Update existing key
            node.values[index] = value;
            return Ok(None);
        }
        
        // Insert new key-value pair
        node.keys.insert(index, key);
        node.values.insert(index, value);
        
        if node.is_full() {
            self.split_leaf(node_id)
        } else {
            Ok(None)
        }
    }

    fn insert_into_internal(&mut self, node_id: NodeId, key: Key, value: Value) -> Result<Option<NodeId>> {
        let node = self.nodes.get(&node_id).unwrap().clone();
        let index = node.find_key_index(&key);
        
        let child_id = if index < node.children.len() {
            node.children[index]
        } else {
            return Err(anyhow!("Invalid child index"));
        };
        
        let new_child = self.insert_recursive(child_id, key, value)?;
        
        if let Some(new_child_id) = new_child {
            self.insert_child(node_id, new_child_id)
        } else {
            Ok(None)
        }
    }

    fn split_leaf(&mut self, node_id: NodeId) -> Result<Option<NodeId>> {
        let node = self.nodes.get(&node_id).unwrap().clone();
        let mid = node.keys.len() / 2;
        
        // Create new leaf node
        let new_node_id = self.allocate_node_id();
        let mut new_node = Node::new(new_node_id, true);
        
        // Move second half to new node
        new_node.keys = node.keys[mid..].to_vec();
        new_node.values = node.values[mid..].to_vec();
        
        // Update original node
        let old_node = self.nodes.get_mut(&node_id).unwrap();
        old_node.keys.truncate(mid);
        old_node.values.truncate(mid);
        
        // Update leaf pointers
        new_node.next_leaf = old_node.next_leaf;
        new_node.prev_leaf = Some(node_id);
        old_node.next_leaf = Some(new_node_id);
        
        if let Some(next_id) = new_node.next_leaf {
            if let Some(next_node) = self.nodes.get_mut(&next_id) {
                next_node.prev_leaf = Some(new_node_id);
            }
        }
        
        let promote_key = new_node.keys[0].clone();
        self.nodes.insert(new_node_id, new_node);
        
        // If this is the root, create new root
        if Some(node_id) == self.root {
            let new_root_id = self.allocate_node_id();
            let mut new_root = Node::new(new_root_id, false);
            new_root.keys.push(promote_key);
            new_root.children.push(node_id);
            new_root.children.push(new_node_id);
            
            self.nodes.insert(new_root_id, new_root);
            Ok(Some(new_root_id))
        } else {
            Ok(Some(new_node_id))
        }
    }

    fn insert_child(&mut self, parent_id: NodeId, child_id: NodeId) -> Result<Option<NodeId>> {
        let child = self.nodes.get(&child_id).unwrap().clone();
        let promote_key = child.keys[0].clone();
        
        let parent = self.nodes.get_mut(&parent_id).unwrap();
        let index = parent.find_key_index(&promote_key);
        
        parent.keys.insert(index, promote_key);
        parent.children.insert(index + 1, child_id);
        
        if parent.is_full() {
            self.split_internal(parent_id)
        } else {
            Ok(None)
        }
    }

    fn split_internal(&mut self, node_id: NodeId) -> Result<Option<NodeId>> {
        let node = self.nodes.get(&node_id).unwrap().clone();
        let mid = node.keys.len() / 2;
        
        // Create new internal node
        let new_node_id = self.allocate_node_id();
        let mut new_node = Node::new(new_node_id, false);
        
        // Move second half to new node
        new_node.keys = node.keys[mid + 1..].to_vec();
        new_node.children = node.children[mid + 1..].to_vec();
        
        let promote_key = node.keys[mid].clone();
        
        // Update original node
        let old_node = self.nodes.get_mut(&node_id).unwrap();
        old_node.keys.truncate(mid);
        old_node.children.truncate(mid + 1);
        
        self.nodes.insert(new_node_id, new_node);
        
        // If this is the root, create new root
        if Some(node_id) == self.root {
            let new_root_id = self.allocate_node_id();
            let mut new_root = Node::new(new_root_id, false);
            new_root.keys.push(promote_key);
            new_root.children.push(node_id);
            new_root.children.push(new_node_id);
            
            self.nodes.insert(new_root_id, new_root);
            Ok(Some(new_root_id))
        } else {
            Ok(Some(new_node_id))
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>> {
        if let Some(root_id) = self.root {
            self.get_recursive(root_id, key)
        } else {
            Ok(None)
        }
    }

    fn get_recursive(&self, node_id: NodeId, key: &str) -> Result<Option<Value>> {
        let node = self.nodes.get(&node_id).unwrap();
        
        if node.is_leaf {
            let index = node.find_key_index(key);
            if index < node.keys.len() && node.keys[index] == key {
                Ok(Some(node.values[index].clone()))
            } else {
                Ok(None)
            }
        } else {
            let index = node.find_key_index(key);
            let child_id = if index < node.children.len() {
                node.children[index]
            } else {
                return Ok(None);
            };
            self.get_recursive(child_id, key)
        }
    }

    pub fn scan_prefix(&self, prefix: &str) -> Result<Vec<Key>> {
        let mut results = Vec::new();
        
        if let Some(start_node_id) = self.find_leaf_for_prefix(prefix)? {
            let mut current = Some(start_node_id);
            
            while let Some(node_id) = current {
                let node = self.nodes.get(&node_id).unwrap();
                
                for key in &node.keys {
                    if key.starts_with(prefix) {
                        results.push(key.clone());
                        } else if key.as_str() > prefix {
                            // Keys are sorted, so we can stop here
                            return Ok(results);
                        }
                }
                
                current = node.next_leaf;
            }
        }
        
        Ok(results)
    }

    fn find_leaf_for_prefix(&self, prefix: &str) -> Result<Option<NodeId>> {
        if let Some(root_id) = self.root {
            self.find_leaf_recursive(root_id, prefix)
        } else {
            Ok(None)
        }
    }

    fn find_leaf_recursive(&self, node_id: NodeId, key: &str) -> Result<Option<NodeId>> {
        let node = self.nodes.get(&node_id).unwrap();
        
        if node.is_leaf {
            Ok(Some(node_id))
        } else {
            let index = node.find_key_index(key);
            let child_id = if index < node.children.len() {
                node.children[index]
            } else {
                return Ok(None);
            };
            self.find_leaf_recursive(child_id, key)
        }
    }

    pub fn save_to_disk(&self, path: &str) -> Result<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, self)?;
        Ok(())
    }

    pub fn load_from_disk(&mut self, path: &str) -> Result<()> {
        if !Path::new(path).exists() {
            return Err(anyhow!("Storage file does not exist"));
        }
        
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let loaded: BPlusTree = bincode::deserialize_from(reader)?;
        
        *self = loaded;
        Ok(())
    }

    pub fn apply_wal_entry(&mut self, entry: &WalEntry) -> Result<()> {
        match &entry.operation {
            crate::txn::wal::WalOperation::Insert { key, row, .. } => {
                let serialized_row = bincode::serialize(row)?;
                self.insert(key.clone(), serialized_row)?;
            }
            crate::txn::wal::WalOperation::CreateTable(_) => {
                // Table creation doesn't affect storage directly
            }
        }
        Ok(())
    }

    fn allocate_node_id(&mut self) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }

    pub fn should_checkpoint(&self) -> bool {
        self.operation_count >= 1000
    }

    pub fn reset_operation_count(&mut self) {
        self.operation_count = 0;
    }
}

impl Default for BPlusTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_insert_and_get() {
        let mut tree = BPlusTree::new();
        
        tree.insert("key1".to_string(), b"value1".to_vec()).unwrap();
        tree.insert("key2".to_string(), b"value2".to_vec()).unwrap();
        
        assert_eq!(tree.get("key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(tree.get("key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(tree.get("key3").unwrap(), None);
    }

    #[test]
    fn test_scan_prefix() {
        let mut tree = BPlusTree::new();
        
        tree.insert("user:1".to_string(), b"alice".to_vec()).unwrap();
        tree.insert("user:2".to_string(), b"bob".to_vec()).unwrap();
        tree.insert("product:1".to_string(), b"laptop".to_vec()).unwrap();
        
        let results = tree.scan_prefix("user:").unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"user:1".to_string()));
        assert!(results.contains(&"user:2".to_string()));
    }

    #[test]
    fn test_persistence() {
        let mut tree = BPlusTree::new();
        tree.insert("key1".to_string(), b"value1".to_vec()).unwrap();
        
        let temp_file = NamedTempFile::new().unwrap();
        tree.save_to_disk(temp_file.path().to_str().unwrap()).unwrap();
        
        let mut new_tree = BPlusTree::new();
        new_tree.load_from_disk(temp_file.path().to_str().unwrap()).unwrap();
        
        assert_eq!(new_tree.get("key1").unwrap(), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_many_inserts() {
        let mut tree = BPlusTree::new();
        
        // Insert many keys to trigger splits
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let value = format!("value{}", i).into_bytes();
            tree.insert(key, value).unwrap();
        }
        
        // Verify all keys are present
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let expected_value = format!("value{}", i).into_bytes();
            assert_eq!(tree.get(&key).unwrap(), Some(expected_value));
        }
    }
}