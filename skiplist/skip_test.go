package skiplist

import (
	"sync"
	"testing"
)

/*
A simple UpdateCheck function that behaves as follows:
  - if the input key does not exist (i.e. currValue is 0), we return a newValue of 1
  - if the input key does exist, we return a new value that is double the previous value.
*/
func CustomUpdateCheck(key int, currValue int, exists bool) (newValue int, err error) {
	if !exists {
		return 1, nil
	} else {
		return 2 * currValue, nil
	}
}

/*
 * Insert
 */

func TestInsertSuccess(t *testing.T) {
	list := NewList[int, int](0, 10, 3)
	ok, _ := list.Upsert(1, CustomUpdateCheck)

	if !ok {
		t.Fatalf("expected true. got %t", ok)
	}
}

func TestConcurrentDistinctInserts(t *testing.T) {
	for j := 1; j < 100; j++ {
		list := NewList[int, int](0, 10, 3)
		iters := 5

		var wg sync.WaitGroup

		for i := 1; i <= iters; i++ {
			wg.Add(1)

			go func(k int) {
				defer wg.Done()

				ok, _ := list.Upsert(k, CustomUpdateCheck)
				if !ok {
					t.Errorf("expected true. got %t", ok)
				}
			}(i)
		}

		wg.Wait()
	}
}

func TestConcurrentRepeatedInserts(t *testing.T) {
	for j := 1; j < 100; j++ {
		list := NewList[int, int](0, 10, 3)
		iters := 5

		var wg sync.WaitGroup
		var okChan chan bool = make(chan bool)

		for i := 0; i < iters; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()
				res, _ := list.Upsert(1, CustomUpdateCheck)
				okChan <- res
			}()
		}

		numSuccesses := 0
		for i := 0; i < iters; i++ {
			ok := <-okChan
			if ok {
				numSuccesses++
			}
		}

		wg.Wait()

		if numSuccesses != iters {
			t.Fatalf("expected %d successful upserts. got %d", iters, numSuccesses)
		}
	}
}

func TestConcurrentRepeatedRemoves(t *testing.T) {
	for j := 1; j < 100; j++ {
		list := NewList[int, int](0, 10, 3)
		iters := 5

		ok, _ := list.Upsert(1, CustomUpdateCheck)
		if !ok {
			t.Fatalf("expected true. got %t", ok)
		}

		var wg sync.WaitGroup
		var okChan chan bool = make(chan bool)

		for i := 0; i < iters; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()
				_, ok := list.Remove(1)
				okChan <- ok
			}()
		}

		numSuccesses := 0
		for i := 0; i < iters; i++ {
			ok := <-okChan
			if ok {
				numSuccesses++
			}
		}

		wg.Wait()

		if numSuccesses != 1 {
			t.Fatalf("expected only one successful remove. got %d", numSuccesses)
		}
	}
}

func TestConcurrentInsertsAndFinds(t *testing.T) {
	list := NewList[int, int](0, 200, 4)
	numInserts := 100
	numFinds := 100
	var insertWG sync.WaitGroup
	var findWG sync.WaitGroup

	for i := 0; i < numInserts; i++ {
		insertWG.Add(1)
		go func(key int) {
			defer insertWG.Done()
			list.Upsert(key, CustomUpdateCheck)
		}(i)
	}

	// Wait for inserts to finish before starting finds
	insertWG.Wait()

	for i := 0; i < numFinds; i++ {
		findWG.Add(1)
		go func(key int) {
			defer findWG.Done()
			_, ok := list.Find(key)
			if !ok {
				t.Errorf("Expected to find key %d", key)
			}
		}(i)
	}
	findWG.Wait()
	list.Display()
}

func TestConcurrentInsertsAndRemoves(t *testing.T) {
	list := NewList[int, int](-1, 200, 5)
	numInserts := 100
	numRemoves := 50
	var insertWG sync.WaitGroup
	var removeWG sync.WaitGroup

	for i := 0; i < numInserts; i++ {
		insertWG.Add(1)
		go func(key int) {
			defer insertWG.Done()
			list.Upsert(key, CustomUpdateCheck)
		}(i)
	}

	// Wait for inserts to finish before starting removes
	insertWG.Wait()

	for i := 0; i < numRemoves; i++ {
		removeWG.Add(1)
		go func(key int) {
			defer removeWG.Done()
			_, ok := list.Remove(key)
			if !ok {
				t.Errorf("Expected to remove key %d", key)
			}
		}(i * 2)
	}
	removeWG.Wait()
	list.Display()
}
