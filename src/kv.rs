use std::collections::HashMap;

#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore{
        KvStore {
            store: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        if let Some(old) = self.store.insert(key, value) {
            eprint!("update old value:{}", old);
        }
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        self.store.get(&key).map(|r| r.clone())
    }

    pub fn remove(&mut self, key: String) {
        if let Some(old) = self.store.remove(&key) {
            eprint!("remove old value:{}", old);
        }
    }
}