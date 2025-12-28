package main

import distkv "distributed-kv-store/internal/node"

func main() {
	node := distkv.NewNode()
	err := node.Init(":8080")
	if err != nil {
		panic(err)
	}

	for {
	}
}
