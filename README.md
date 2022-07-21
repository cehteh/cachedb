In memory Key/Value store with LRU expire and concurrent access


# Description

Items are stored in N sharded/bucketized HashMaps to improve concurrency.  Every Item is
always behind a RwLock.  Quering an item will return a guard associated to this lock.
Items that are not locked are kept in a list to implement a least-recent-used expire
policy.  Locked items are removed from that lru list and put into the lru-list when they
become unlocked.  Locked Items will not block the hosting HashMap.

