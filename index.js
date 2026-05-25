const crypto = require('crypto');

// ==========================================
// 1. SIMPLE LRU CACHE IMPLEMENTATION
// ==========================================
class LRUCache {
    constructor(capacity) {
        this.capacity = capacity;
        this.cache = new Map();
    }

    get(key) {
        if (!this.cache.has(key)) return null;
        // Move accessed item to the end to maintain 'Most Recently Used' status
        const val = this.cache.get(key);
        this.cache.delete(key);
        this.cache.set(key, val);
        return val;
    }

    put(key, value) {
        if (this.cache.has(key)) {
            this.cache.delete(key);
        } else if (this.cache.size >= this.capacity) {
            // Evict Least Recently Used item (first item in Map keys iterator)
            const firstKey = this.cache.keys().next().value;
            this.cache.delete(firstKey);
        }
        this.cache.set(key, value);
    }
}

// ==========================================
// 2. STORAGE NODE ARCHITECTURE
// ==========================================
class StorageNode {
    constructor(name, cacheCapacity = 2) {
        this.name = name;
        this.store = new Map(); // Physical persistent store
        this.cache = new LRUCache(cacheCapacity); // Local cache layer
    }

    put(key, value) {
        this.store.set(key, value);
        this.cache.put(key, value); // Populate cache on write
    }

    get(key) {
        // Look up cache first
        const cachedVal = this.cache.get(key);
        if (cachedVal) {
            console.log(`   [CACHE HIT] Found inside ${this.name}`);
            return cachedVal;
        }
        // Fallback to storage
        if (this.store.has(key)) {
            console.log(`   [CACHE MISS] Retrieved from disk store inside ${this.name}`);
            const val = this.store.get(key);
            this.cache.put(key, val); // Refresh cache
            return val;
        }
        return null;
    }
}

// ==========================================
// 3. CONSISTENT HASHING RING ENGINE
// ==========================================
class ConsistentHashRing {
    constructor(numberOfReplicas = 3) {
        this.numberOfReplicas = numberOfReplicas; // Virtual nodes per physical node
        this.ring = new Map(); // MD5 Hash string -> Node object
        this.sortedHashes = []; // Array to traverse sorted ring points
    }

    // Helper to generate md5 hash as an integer-comparable hex string
    _hash(key) {
        return crypto.createHash('md5').update(key).digest('hex');
    }

    addNode(node) {
        for (let i = 0; i < this.numberOfReplicas; i++) {
            const virtualNodeKey = `${node.name}-virtual-${i}`;
            const hash = this._hash(virtualNodeKey);
            this.ring.set(hash, node);
            this.sortedHashes.push(hash);
        }
        this.sortedHashes.sort();
    }

    removeNode(node) {
        for (let i = 0; i < this.numberOfReplicas; i++) {
            const virtualNodeKey = `${node.name}-virtual-${i}`;
            const hash = this._hash(virtualNodeKey);
            this.ring.delete(hash);
        }
        this.sortedHashes = this.sortedHashes.filter(h => this.ring.has(h));
    }

    getNode(key) {
        if (this.sortedHashes.length === 0) return null;
        const hash = this._hash(key);
        
        // Find the first virtual node hash greater than or equal to the key's hash
        for (let i = 0; i < this.sortedHashes.length; i++) {
            if (hash <= this.sortedHashes[i]) {
                return this.ring.get(this.sortedHashes[i]);
            }
        }
        // If it falls past the end, wrap around to the first node on the circle ring
        return this.ring.get(this.sortedHashes[0]);
    }
}

// ==========================================
// 4. DISTRIBUTED CLUSTER (TRANSPARENCY LAYER)
// ==========================================
class DistributedKVStore {
    constructor() {
        this.ring = new ConsistentHashRing();
        this.nodes = new Map(); // Node name -> StorageNode instance
    }

    addStorageNode(name) {
        const node = new StorageNode(name);
        this.nodes.set(name, node);
        this.ring.addNode(node);
        console.log(`🟢 Physical Node "${name}" joined cluster.`);
    }

    removeStorageNode(name) {
        const node = this.nodes.get(name);
        if (node) {
            this.ring.removeNode(node);
            this.nodes.delete(name);
            console.log(`🔴 Physical Node "${name}" gracefully removed or failed.`);
            return node; // Return for data migration tracing
        }
    }

    // Hides distributed layout logic from users entirely
    set(key, value) {
        const targetNode = this.ring.getNode(key);
        if (!targetNode) throw new Error("No nodes available inside storage cluster.");
        targetNode.put(key, value);
        console.log(`📥 Key "${key}" mapped and routed directly to ${targetNode.name}`);
    }

    get(key) {
        const targetNode = this.ring.getNode(key);
        if (!targetNode) return null;
        console.log(`📤 Query for "${key}" routed transparently to ${targetNode.name}`);
        return targetNode.get(key);
    }
}

// ==========================================
// 5. SIMULATION SUITE
// ==========================================
const cluster = new DistributedKVStore();

console.log("--- Phase 1: Cluster Initialization ---");
cluster.addStorageNode("Node-A");
cluster.addStorageNode("Node-B");
cluster.addStorageNode("Node-C");

console.log("\n--- Phase 2: Loading Expected Data ---");
const sampleData = [
    { key: "user:101", value: { name: "Alice" } },
    { key: "user:102", value: { name: "Bob" } },
    { key: "user:103", value: { name: "Charlie" } },
    { key: "user:104", value: { name: "Diana" } },
    { key: "user:105", value: { name: "Eve" } },
    { key: "user:106", value: { name: "Frank" } }
];

sampleData.forEach(item => cluster.set(item.key, JSON.stringify(item.value)));

console.log("\n--- Phase 3: Verifying Transparency & LRU Caching Layer ---");
// First read: Triggers Cache Miss (Retrieved from store)
console.log(`Result: ${cluster.get("user:101")}`);
// Second read: Triggers Cache Hit (Retrieved instantly from LRU)
console.log(`Result: ${cluster.get("user:101")}`);

console.log("\n--- Phase 4: Simulating Dynamic Scale Out (Node Join) ---");
// Capturing current location mapping of user:102 before node layout shift
const oldTarget = cluster.ring.getNode("user:102").name;
cluster.addStorageNode("Node-D");
const newTarget = cluster.ring.getNode("user:102").name;
console.log(`Key "user:102" placement check: [Before: ${oldTarget}] -> [After: ${newTarget}]`);
console.log("Notice: Keys are only redistributed if their hash range falls into the new Node-D slice!");

console.log("\n--- Phase 5: Simulating Node Failure & Availability Impact ---");
// Intentionally terminate Node-B
const deadNode = cluster.removeStorageNode("Node-B");

// Attempt reading a key that belonged to Node-B
console.log("Querying key immediately after parent node crash...");
const recoveryValue = cluster.get("user:102"); 
console.log(`Result: ${recoveryValue ? recoveryValue : "⚠️ Data temporarily unavailable (Needs Replication Layer)"}`);
console.log("System Status: Operational. Hash ring re-routed around dead node to ensure system remains up.");
