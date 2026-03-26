//! State storage abstraction

use crate::error::RuntimeResult;
use setu_types::{Address, CoinData, Object, ObjectId};
use std::collections::HashMap;

/// State storage trait
/// Can be replaced with persistent storage or Move VM state management in the future
pub trait StateStore {
    /// Read object
    fn get_object(&self, object_id: &ObjectId) -> RuntimeResult<Option<Object<CoinData>>>;

    /// Write object
    fn set_object(&mut self, object_id: ObjectId, object: Object<CoinData>) -> RuntimeResult<()>;

    /// Delete object
    fn delete_object(&mut self, object_id: &ObjectId) -> RuntimeResult<()>;

    /// Get all objects owned by an address
    fn get_owned_objects(&self, owner: &Address) -> RuntimeResult<Vec<ObjectId>>;

    /// Read generic VM object bytes (for non-coin Sui objects).
    fn get_vm_object(&self, _object_id: &ObjectId) -> RuntimeResult<Option<Vec<u8>>> {
        Ok(None)
    }

    /// Write generic VM object bytes (for non-coin Sui objects).
    fn set_vm_object(&mut self, _object_id: ObjectId, _bytes: Vec<u8>) -> RuntimeResult<()> {
        Ok(())
    }

    /// Delete generic VM object bytes (for non-coin Sui objects).
    fn delete_vm_object(&mut self, _object_id: &ObjectId) -> RuntimeResult<()> {
        Ok(())
    }

    /// Check if object exists
    fn exists(&self, object_id: &ObjectId) -> bool {
        self.get_object(object_id).ok().flatten().is_some()
    }
}

/// In-memory state storage (used for testing and simple scenarios)
#[derive(Debug, Clone)]
pub struct InMemoryStateStore {
    /// Object storage: ObjectId -> Object
    objects: HashMap<ObjectId, Object<CoinData>>,
    /// Generic VM object storage: ObjectId -> opaque bytes
    vm_objects: HashMap<ObjectId, Vec<u8>>,
    /// Ownership index: Address -> Vec<ObjectId>
    ownership_index: HashMap<Address, Vec<ObjectId>>,
}

impl InMemoryStateStore {
    /// Create new in-memory state storage
    pub fn new() -> Self {
        Self {
            objects: HashMap::new(),
            vm_objects: HashMap::new(),
            ownership_index: HashMap::new(),
        }
    }

    /// Update ownership index
    fn update_ownership_index(&mut self, object_id: ObjectId, new_owner: &Address) {
        // Remove from the old owner's index
        for objects in self.ownership_index.values_mut() {
            objects.retain(|id| id != &object_id);
        }

        // Add to the new owner's index
        self.ownership_index
            .entry(new_owner.clone())
            .or_insert_with(Vec::new)
            .push(object_id);
    }

    /// Remove object from ownership index
    fn remove_from_ownership_index(&mut self, object_id: &ObjectId) {
        for objects in self.ownership_index.values_mut() {
            objects.retain(|id| id != object_id);
        }
    }

    /// Get total balance (used for testing)
    pub fn get_total_balance(&self, owner: &Address) -> u64 {
        self.get_owned_objects(owner)
            .unwrap_or_default()
            .iter()
            .filter_map(|id| self.get_object(id).ok().flatten())
            .map(|obj| obj.data.balance.value())
            .sum()
    }
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for InMemoryStateStore {
    fn get_object(&self, object_id: &ObjectId) -> RuntimeResult<Option<Object<CoinData>>> {
        Ok(self.objects.get(object_id).cloned())
    }

    fn set_object(&mut self, object_id: ObjectId, object: Object<CoinData>) -> RuntimeResult<()> {
        // Update ownership index
        if let Some(owner) = &object.metadata.owner {
            self.update_ownership_index(object_id, owner);
        }

        // Store object
        self.objects.insert(object_id, object);
        Ok(())
    }

    fn delete_object(&mut self, object_id: &ObjectId) -> RuntimeResult<()> {
        self.objects.remove(object_id);
        self.remove_from_ownership_index(object_id);
        Ok(())
    }

    fn get_owned_objects(&self, owner: &Address) -> RuntimeResult<Vec<ObjectId>> {
        Ok(self.ownership_index.get(owner).cloned().unwrap_or_default())
    }

    fn get_vm_object(&self, object_id: &ObjectId) -> RuntimeResult<Option<Vec<u8>>> {
        Ok(self.vm_objects.get(object_id).cloned())
    }

    fn set_vm_object(&mut self, object_id: ObjectId, bytes: Vec<u8>) -> RuntimeResult<()> {
        self.vm_objects.insert(object_id, bytes);
        Ok(())
    }

    fn delete_vm_object(&mut self, object_id: &ObjectId) -> RuntimeResult<()> {
        self.vm_objects.remove(object_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_store_operations() {
        let mut store = InMemoryStateStore::new();

        let owner = Address::from_str_id("alice");
        let coin = setu_types::create_coin(owner.clone(), 1000);
        let coin_id = *coin.id();

        // Set object
        store.set_object(coin_id, coin.clone()).unwrap();

        // Read object
        let retrieved = store.get_object(&coin_id).unwrap().unwrap();
        assert_eq!(retrieved.id(), &coin_id);

        // Check ownership index
        let owned = store.get_owned_objects(&owner).unwrap();
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0], coin_id);

        // Delete object
        store.delete_object(&coin_id).unwrap();
        assert!(store.get_object(&coin_id).unwrap().is_none());

        let owned = store.get_owned_objects(&owner).unwrap();
        assert_eq!(owned.len(), 0);
    }
}
