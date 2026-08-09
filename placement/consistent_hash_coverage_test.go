package placement_test

import (
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/placement"
)

func TestConsistentHashRing_InvalidShardCount(t *testing.T) {
	// Should default to 1 shard if <= 0
	ring := placement.NewConsistentHashRing(0)
	
	// Test the fallback works by checking if we get shard 0 for an arbitrary hash
	shard := ring.GetShard(12345)
	if shard != 0 {
		t.Errorf("Expected shard 0 for fallback ring, got %d", shard)
	}
}

func TestConsistentHashRing_EmptyRing(t *testing.T) {
	// To test GetShard with empty hashes, we need to create an empty ring.
	// Since NewConsistentHashRing forces at least 1 shard, we can't create it via New.
	// We'd have to construct the struct manually if it were exported. 
	// Wait! ConsistentHashRing is exported, but its fields are not.
	// Actually, wait, `NewConsistentHashRing(-1)` sets shardCount to 1.
	// The only way `len(r.hashes) == 0` is if `virtualNodesPerShard` was 0, but it's a const 500.
	// Or if someone manually constructs `&ConsistentHashRing{}`.
	ring := &placement.ConsistentHashRing{}
	
	shard := ring.GetShard(12345)
	if shard != 0 {
		t.Errorf("Expected 0 from empty ring, got %d", shard)
	}
}
