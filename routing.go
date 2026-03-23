package f2p

import (
	"bytes"
	"sort"
	"sync"
)

type KBucket struct {
	Contacts []NodeInfo
	mu       sync.RWMutex
}

func (b *KBucket) AddOrUpdate(contact NodeInfo) (evicted NodeInfo, needsPing bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if idx := indexOfContact(b.Contacts, contact.ID); idx >= 0 {
		b.Contacts = moveContactToTail(b.Contacts, idx, contact)
		return NodeInfo{}, false
	}

	if len(b.Contacts) < parameterK {
		b.Contacts = append(b.Contacts, contact)
		return NodeInfo{}, false
	}

	return b.Contacts[0], true
}

func (b *KBucket) Remove(id Key) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx := indexOfContact(b.Contacts, id)
	if idx < 0 {
		return false
	}

	b.Contacts = deleteContactAt(b.Contacts, idx)
	return true
}

func (b *KBucket) Replace(id Key, contact NodeInfo) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if idx := indexOfContact(b.Contacts, contact.ID); idx >= 0 {
		b.Contacts = moveContactToTail(b.Contacts, idx, contact)
		return true
	}

	if idx := indexOfContact(b.Contacts, id); idx >= 0 {
		b.Contacts = append(deleteContactAt(b.Contacts, idx), contact)
		return true
	}

	if len(b.Contacts) < parameterK {
		b.Contacts = append(b.Contacts, contact)
		return true
	}

	return false
}

func (b *KBucket) IsFull() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.Contacts) >= parameterK
}

type RoutingTable struct {
	LocalID Key
	Buckets [160]KBucket
	mu      sync.RWMutex
	ping    func(NodeInfo) bool
}

func (rt *RoutingTable) GetBucketIndex(target Key) int {
	for i := 0; i < len(rt.LocalID); i++ {
		xor := rt.LocalID[i] ^ target[i]
		if xor == 0 {
			continue
		}

		for j := 0; j < 8; j++ {
			if xor&(1<<(7-j)) != 0 {
				return i*8 + j
			}
		}
	}

	return -1
}

func (rt *RoutingTable) UpdateContact(contact NodeInfo) {
	if rt == nil {
		return
	}

	bucketIndex := rt.GetBucketIndex(contact.ID)
	if bucketIndex < 0 {
		return
	}

	evicted, needsPing := rt.Buckets[bucketIndex].AddOrUpdate(contact)
	if !needsPing {
		return
	}

	ping := rt.pingFunc()
	if ping == nil {
		return
	}

	bucket := &rt.Buckets[bucketIndex]
	if ping(evicted) {
		bucket.AddOrUpdate(evicted)
		return
	}

	bucket.Replace(evicted.ID, contact)
}

func (rt *RoutingTable) SetPingFunc(ping func(NodeInfo) bool) {
	if rt == nil {
		return
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.ping = ping
}

func (rt *RoutingTable) pingFunc() func(NodeInfo) bool {
	if rt == nil {
		return nil
	}

	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.ping
}

func (rt *RoutingTable) FindClosest(target Key, limit int) []NodeInfo {
	if rt == nil || limit <= 0 {
		return nil
	}

	bucketOrder := make([]int, 0, len(rt.Buckets))
	targetBucket := rt.GetBucketIndex(target)
	if targetBucket < 0 {
		for i := 0; i < len(rt.Buckets); i++ {
			bucketOrder = append(bucketOrder, i)
		}
	} else {
		bucketOrder = append(bucketOrder, targetBucket)
		for step := 1; step < len(rt.Buckets); step++ {
			left := targetBucket - step
			right := targetBucket + step
			if left >= 0 {
				bucketOrder = append(bucketOrder, left)
			}
			if right < len(rt.Buckets) {
				bucketOrder = append(bucketOrder, right)
			}
		}
	}

	contacts := make([]NodeInfo, 0, limit)
	seen := make(map[Key]struct{}, limit)
	for _, bucketIndex := range bucketOrder {
		bucket := &rt.Buckets[bucketIndex]
		bucket.mu.RLock()
		for _, contact := range bucket.Contacts {
			if contact.ID == rt.LocalID {
				continue
			}
			if _, ok := seen[contact.ID]; ok {
				continue
			}
			seen[contact.ID] = struct{}{}
			contacts = append(contacts, contact)
		}
		bucket.mu.RUnlock()
	}

	sortContactsByDistance(target, contacts)
	if len(contacts) > limit {
		contacts = contacts[:limit]
	}

	return contacts
}

func indexOfContact(contacts []NodeInfo, id Key) int {
	for i, contact := range contacts {
		if contact.ID == id {
			return i
		}
	}

	return -1
}

func moveContactToTail(contacts []NodeInfo, index int, contact NodeInfo) []NodeInfo {
	copy(contacts[index:], contacts[index+1:])
	contacts[len(contacts)-1] = contact
	return contacts
}

func deleteContactAt(contacts []NodeInfo, index int) []NodeInfo {
	copy(contacts[index:], contacts[index+1:])
	contacts[len(contacts)-1] = NodeInfo{}
	return contacts[:len(contacts)-1]
}

func mergeClosest(target Key, localID Key, current []NodeInfo, extra []NodeInfo, limit int) []NodeInfo {
	if limit <= 0 {
		return nil
	}

	mergedByID := make(map[Key]NodeInfo, len(current)+len(extra))
	for _, contact := range current {
		if contact.ID == localID {
			continue
		}
		mergedByID[contact.ID] = contact
	}
	for _, contact := range extra {
		if contact.ID == localID {
			continue
		}
		mergedByID[contact.ID] = contact
	}

	merged := make([]NodeInfo, 0, len(mergedByID))
	for _, contact := range mergedByID {
		merged = append(merged, contact)
	}

	sortContactsByDistance(target, merged)
	if len(merged) > limit {
		merged = merged[:limit]
	}

	return merged
}

func sortContactsByDistance(target Key, contacts []NodeInfo) {
	sort.SliceStable(contacts, func(i, j int) bool {
		left := target.Distance(contacts[i].ID)
		right := target.Distance(contacts[j].ID)

		cmp := bytes.Compare(left[:], right[:])
		if cmp != 0 {
			return cmp < 0
		}

		return bytes.Compare(contacts[i].ID[:], contacts[j].ID[:]) < 0
	})
}

func sortProviders(providers []NodeInfo) {
	sort.SliceStable(providers, func(i, j int) bool {
		return bytes.Compare(providers[i].ID[:], providers[j].ID[:]) < 0
	})
}

func mergeProviders(current []NodeInfo, extra []NodeInfo) []NodeInfo {
	mergedByID := make(map[Key]NodeInfo, len(current)+len(extra))
	for _, provider := range current {
		mergedByID[provider.ID] = provider
	}
	for _, provider := range extra {
		mergedByID[provider.ID] = provider
	}

	merged := make([]NodeInfo, 0, len(mergedByID))
	for _, provider := range mergedByID {
		merged = append(merged, provider)
	}
	sortProviders(merged)
	return merged
}
