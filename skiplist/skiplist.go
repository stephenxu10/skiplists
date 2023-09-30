package skiplist

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
)

type UpdateCheck[K cmp.Ordered, V any] func(key K, currValue V, exists bool) (newValue V, err error)

type Pair[K cmp.Ordered, V any] struct {
	Key   K
	Value V
}

type SkipList[K cmp.Ordered, V any] interface {
	Upsert(key K, check UpdateCheck[K, V]) (updated bool, err error)
	Remove(key K) (removedValue V, removed bool)
	Find(key K) (foundValue V, found bool)
	Query(ctx context.Context, start K, end K) (results []Pair[K, V], err error)
}

type node[K cmp.Ordered, V any] struct {
	sync.Mutex
	key         K
	item        V
	topLevel    int
	marked      atomic.Bool
	fullyLinked atomic.Bool
	nexts       []atomic.Pointer[node[K, V]]
}

/*
The concrete class that implements the SkipList interface. The class stores a pointer
to the head node, whose fields are:
- sync.Mutex: The lock on the node.
- key K, value V: Pretty self-explanatory
- topLevel: The highest level list containing this node
- marked: The bit that indicates whether it is being removed (atomic)
- fullyLinked: The bit that indicates if the node has been added to all lists up to topLevel (atomic)
- next: The pointer to the next node in the list  (atomic)

We also store a counter field that atomically increments whenever an insert/delete operation
is performed on the list.
*/
type List[K cmp.Ordered, V any] struct {
	head    *node[K, V]
	Counter int64
}

/*
Helper generic method that initializes a new skip list with dummy header
and sentinel nodes.
*/
func NewList[K cmp.Ordered, V any](minKey K, maxKey K, maxLevels int) List[K, V] {
	// Create header and sentinel nodes with minimum and maximum possible keys.
	header := &node[K, V]{key: minKey, nexts: make([]atomic.Pointer[node[K, V]], maxLevels)}
	sentinel := &node[K, V]{key: maxKey, nexts: make([]atomic.Pointer[node[K, V]], maxLevels)}

	header.fullyLinked.Store(true)
	sentinel.fullyLinked.Store(true)

	// Initialize the nexts slices for both header and sentinel nodes.
	for i := 0; i < maxLevels; i++ {
		header.nexts[i].Store(sentinel)
	}

	list := List[K, V]{head: header}
	return list
}

/*
This function randomly generates the highest level of a new node in the skip list
by randomly flipping a coin until heads (1) is encountered. Until then,
a counter is incremented up to a specified limit.
*/
func random_level(maxLevel int) int {
	flips := 0

	for {
		flip := rand.Intn(2)

		if flip == 1 {
			break
		}
		flips++

		if flips == maxLevel {
			return maxLevel
		}
	}
	return flips
}

/*
Helper function that finds the predecessor and successor nodes at each level where
a given key is desired to be inserted. Updates the input pointers to slices of
nodes and returns True if the key does not exist (false otherwise).
*/
func (l *List[K, V]) get_neighbors(key K, preds []*node[K, V], succs []*node[K, V]) int {
	found := -1
	prev := l.head
	for level := len(prev.nexts) - 1; level >= 0; level-- {
		curr := prev.nexts[level].Load()

		for key > curr.key {
			prev = curr
			curr = prev.nexts[level].Load()
		}

		if found == -1 && key == curr.key {
			found = level
		}

		preds[level] = prev
		succs[level] = curr
	}
	return found
}

/*
Required method implementation for Find. Given an input key, this method traverses
through the skip list to attempt to locate the value associated with the key. Returns
the value if it was found and the corresponding boolean that indicates if the
search was successful.
*/
func (l *List[K, V]) Find(key K) (foundValue V, found bool) {
	maxLevels := len(l.head.nexts)
	preds := make([]*node[K, V], maxLevels)
	succs := make([]*node[K, V], maxLevels)

	level := l.get_neighbors(key, preds, succs)

	if level == -1 {
		var zeroValue V
		return zeroValue, false
	}
	node := succs[level]
	return node.item, node.fullyLinked.Load() && !node.marked.Load()
}

/*
This method will either UPdate or inSERT into a SkipList. It takes an input function
check that takes in a key K and returns a value of type V associated with the key,
if it exists.
*/
func (l *List[K, V]) Upsert(key K, check UpdateCheck[K, V]) (updated bool, err error) {
	// Pick a random top level
	maxLevels := len(l.head.nexts)
	topLevel := random_level(maxLevels - 1)

	preds := make([]*node[K, V], maxLevels)
	succs := make([]*node[K, V], maxLevels)

	for {
		levelFound := l.get_neighbors(key, preds, succs)
		if levelFound != -1 {
			found := succs[levelFound]

			if !found.marked.Load() {
				// Node is being added, wait for other insert to finish
				for !found.fullyLinked.Load() {

				}

				// Otherwise, we have identified that the node can be safely updated
				// Why? It exists, it is not marked, and it is fully linked.
				// To update it, we lock the node, call the check function, and make the change.
				found.Lock()
				newValue, err := check(found.key, found.item, true)

				if err != nil {
					found.Unlock()
					return false, err
				} else {
					found.item = newValue
				}

				found.Unlock()
				atomic.AddInt64(&l.Counter, 1)
				return true, nil
			}
			// Found node is being removed, try again
			continue
		} else {
			valid := true
			level := 0

			// This differs from the slide code - maintain a mapping of nodes we lock so that
			// we don't end up locking a node that is already locked.
			lockedNodes := make(map[*node[K, V]]int)
			for valid && level <= topLevel {
				if _, exists := lockedNodes[preds[level]]; !exists {
					preds[level].Lock()
					lockedNodes[preds[level]] = 1
				}

				// Check if they are still valid
				unmarked := !preds[level].marked.Load() && !succs[level].marked.Load()
				connected := preds[level].nexts[level].Load() == succs[level]
				valid = unmarked && connected

				// move onto the next level
				level++
			}

			// Predecessors or successors changed, unlock and try again
			if !valid {
				for nodeToUnlock := range lockedNodes {
					nodeToUnlock.Unlock()
				}
				continue
			}
			// Now we can insert the new node that does not exist!
			var nothing V
			newValue, err := check(key, nothing, false)

			if err != nil {
				return false, err
			}
			newNode := &node[K, V]{
				key:      key,
				item:     newValue,
				topLevel: topLevel,
				nexts:    make([]atomic.Pointer[node[K, V]], topLevel+1),
			}

			// Set the next pointers and build from bottom up
			level = 0
			for level <= topLevel {
				newNode.nexts[level].Store(succs[level])
				preds[level].nexts[level].Store(newNode)
				level++
			}
			// node is fully linked
			newNode.fullyLinked.Store(true)

			// Unlock all the nodes that got locked
			for nodeToUnlock := range lockedNodes {
				nodeToUnlock.Unlock()
			}

			atomic.AddInt64(&l.Counter, 1)
			return true, nil
		}
	}
}

/*
This function deletes a node from a skip list and returns the corresponding
removed value or returns an indication that the node was not found.
*/
func (l *List[K, V]) Remove(key K) (removedValue V, removed bool) {
	var zero V
	maxLevels := len(l.head.nexts)
	var victim *node[K, V]
	isMarked := false
	topLevel := -1

	preds := make([]*node[K, V], maxLevels)
	succs := make([]*node[K, V], maxLevels)

	// Keep trying to remove until success or failure
	for {
		levelFound := l.get_neighbors(key, preds, succs)

		if levelFound != -1 {
			victim = succs[levelFound]
		}

		if !isMarked {
			if levelFound == -1 || !victim.fullyLinked.Load() || victim.marked.Load() || victim.topLevel != levelFound {
				return zero, false
			}
			topLevel = victim.topLevel

			victim.Lock()
			// Another remove call beat us
			if victim.marked.Load() {
				victim.Unlock()
				return zero, false
			}
			victim.marked.Store(true)
			isMarked = true
		}

		// At this point, victim is locked and marked.
		level := 0
		valid := true

		// Store all acquired locks in a map we don't attempt to lock a node more than once
		lockedNodes := make(map[*node[K, V]]int)

		// Lock the predecessors
		for valid && level <= topLevel {
			if _, exists := lockedNodes[preds[level]]; !exists {
				preds[level].Lock()
				lockedNodes[preds[level]] = 1
			}

			// Check if they are still valid
			successor := (preds[level].nexts[level].Load() == victim)
			valid = !preds[level].marked.Load() && successor

			// move onto the next level
			level++
		}
		// Unlock the predecessors and try again if not valid
		if !valid {
			for nodeToUnlock := range lockedNodes {
				nodeToUnlock.Unlock()
			}
			continue
		}

		// All conditions are satisfied, so delete the node by linking preds to succs
		for level = topLevel; level >= 0; level-- {
			preds[level].nexts[level].Store(victim.nexts[level].Load())
		}

		// Wrap everything up with unlocking
		victim.Unlock()
		for nodeToUnlock := range lockedNodes {
			nodeToUnlock.Unlock()
		}

		atomic.AddInt64(&l.Counter, 1)
		return victim.item, true
	}
}

/*
A range query method that returns all key, value pairs of entries
in the skip list where the keys are within a certain min, max range.

We implement the second solution where we maintain an atomic counter
for the whole list. If the counter remains the same before & after
the range query, the list has not changed so we can return the result. Otherwise,
we try again (or quit based on the input context).
*/
func (l *List[K, V]) Query(ctx context.Context, start K, end K) (results []Pair[K, V], err error) {
	results = make([]Pair[K, V], 0)

	if start > end {
		return results, errors.New("query start bound greater than end bound")
	}

	for {
		before := atomic.LoadInt64(&l.Counter)

		select {
		case <-ctx.Done():
			return results, errors.New("request timed out")
		default:
			curr := l.head

			// traverse level 0 and add all nodes within the key range
			for curr != nil {
				if curr.key > start && curr.key < end {
					key_value := Pair[K, V]{Key: curr.key, Value: curr.item}
					results = append(results, key_value)
				}
				curr = curr.nexts[0].Load()
			}

			// check that before and after counters match
			after := atomic.LoadInt64(&l.Counter)
			if before == after {
				return results, nil
			}
		}

	}
}

/*
Helper method to display all levels of a SkipList.
*/
func (l *List[K, V]) Display() {
	maxLevel := len(l.head.nexts) - 1

	for level := maxLevel; level >= 0; level-- {
		fmt.Printf("Level %d:  ", level)
		curr := l.head

		for curr != nil {
			arrow := "->"
			if curr.nexts[level].Load() == nil {
				arrow = ""
			}
			fmt.Printf("%v %s ", curr.key, arrow)
			curr = curr.nexts[level].Load()
		}
		fmt.Println()
	}
}
