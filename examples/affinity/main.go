package main

import (
	"fmt"
	"log"

	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/scheduler"
)

type Item struct {
	ID string
}

// SessionKey implements placement.AffinityIdentifier.
// The scheduler uses this to deterministically hash and route the request.
type SessionKey string

func (s SessionKey) AppendAffinityBytes(dst []byte) []byte {
	return append(dst, string(s)...)
}

func main() {
	// A dummy comparator (all resources equal priority)
	compare := func(a, b *Item) int { return 0 }
	keyFunc := func(i *Item) string { return i.ID }

	// Configure with multiple shards to demonstrate routing.
	cfg := config.Config[*Item, string]{
		HeapCount:  8,
		Comparator: compare,
		KeyFunc:    keyFunc,
	}

	sched, err := scheduler.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer sched.Shutdown()

	// Add resources across all shards.
	for i := 0; i < 16; i++ {
		sched.Add(&Item{ID: fmt.Sprintf("item-%d", i)})
	}

	// 1. Acquire using a deterministic session key.
	// The scheduler hashes the key using a Consistent Hash Ring to map to a shard.
	session1 := SessionKey("user-123")
	res1, _ := sched.AcquireByAffinity(session1)
	fmt.Printf("Session 1 acquired: %s\n", res1.ID)

	// 2. Acquire again with the same key. We are guaranteed to hit the same shard,
	// meaning we will get the exact same resource if we use config.Shared.
	res2, _ := sched.AcquireByAffinity(session1)
	fmt.Printf("Session 1 re-acquired: %s\n", res2.ID)

	// 3. A different session key will hash to a different shard.
	session2 := SessionKey("user-456")
	res3, _ := sched.AcquireByAffinity(session2)
	fmt.Printf("Session 2 acquired: %s\n", res3.ID)
}
