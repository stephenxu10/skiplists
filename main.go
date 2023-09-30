package main

import (
	"context"
	"fmt"
	"time"

	"github.com/comp-318/skiplists/skiplist"
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

func main() {
	fmt.Println("Attempting some insertions...")
	list := skiplist.NewList[int, int](-1000, 1000, 5)
	for i := 0; i <= 70; i += 3 {
		list.Upsert(i, CustomUpdateCheck)
	}
	fmt.Println("Insertions Completed!")
	list.Display()

	fmt.Println("")
	fmt.Println("Attempting some updates...")
	for i := 0; i <= 70; i += 6 {
		list.Upsert(i, CustomUpdateCheck)
		list.Upsert(i, CustomUpdateCheck)
	}
	fmt.Println("Updates complete!")
	list.Display()

	fmt.Println("")
	fmt.Println("Attempting some deletions...")
	for i := 35; i <= 90; i += 1 {
		list.Remove(i)
	}
	list.Display()

	fmt.Println("")
	lower := 10
	upper := 50
	fmt.Printf("Now we try a range query from %d to %d\n", lower, upper)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	results, err := list.Query(ctx, lower, upper)

	if err != nil {
		fmt.Printf("Query error: %v\n", err)
	} else {
		fmt.Println("Query results:")
		for _, pair := range results {
			fmt.Printf("Key: %d, Value: %d\n", pair.Key, pair.Value)
		}
	}

	fmt.Println()
	fmt.Printf("Final counter: %d\n", list.Counter)
}
