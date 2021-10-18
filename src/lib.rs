//! In memory Key/Value store with LRU expire and concurrent access

struct CacheDb {
    /// when resizing the only by half of the current size (default is doubling the capacity)
    grow_half: bool,

    /// before resizing try first to evict until lowwater is reached
    prefer_evict: bool,

    /// Allow to shrink the cache when used size (hot+cold) falls below 25% utilization
    may_shrink: bool,

    /// When the percentage of dead members is more than this then old entries are more
    /// aggressively pruned. Defaults to 50%.
    highwater_mark: usize,

    /// how many items to evict per insert when over highwater. Default evict two
    /// per inserts.
    evicts_per_insert: usize,

    /// cache fills up until this much percent are dead, after that it starts to lazily evict
    /// members. Default 10%.
    lowwater_mark: usize,

    /// how many inserts to do before evicting one item when above lowwater. Default evict one
    /// per every two inserts.
    inserts_per_evict: usize,

    /// number of cached unused entries
    cold: usize,
}

impl CacheDb {
    // /// Create a new CacheDb with an initial capacity.
    // pub fn new(capacity: usize) -> CacheDb {
    //     CacheDb {  }
    // }
    // 
    // /// Set the highwater configuration
    // pub fn highwater(&mut self, mark: usize, evicts_per_insert: usize) {
    //     
    // }
    // 
    // /// Sets the lowwater configuration
    // pub fn lowwater(&mut self, mark: usize, inserts_per_evicts: usize) {
    //     
    // }
    // 
    // /// Configures the allocation policies
    // pub fn allocation_policy(grow_half: bool, prefer_evict: bool, may_shrink: bool) {
    //     
    // }
    // 
    // /// Query the size of the cache
    // pub fn capacity() -> usize {
    //     
    // }
    // 
    // /// Query the number of used entries
    // pub fn hot() -> usize {
    //     
    // }
    // 
    // /// Query the number of unused/cached entries
    // pub fn cold() -> usize {
    //     
    // }
    // 
    // /// Force evict up to n cold entries in LRU order
    // pub fn lru_evict(n: usize) {
    //     
    // }
    // 
    // /// Query the Entry associated with key for reading
    // pub fn get(key) -> Result<ReadGuard<'_, T>> {
    //     
    // }
    // 
    // /// Query an Entry for reading or construct it (atomically)
    // pub fn get_or(key, ctor) -> Result<ReadGuard<'_, T>> {
    //     
    // }
    // 
    // /// Query the Entry associated with key for writeing
    // pub fn get_mut(key) -> Result<WriteGuard<'_, T>> {
    //     
    // }
    // 
    // /// Query an Entry for writing or construct it (atomically)
    // pub fn get_mut_or(key, ctor) -> Result<WriteGuard<'_, T>> {
    //     
    // }

}
