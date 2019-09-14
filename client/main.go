package main

import (
	"fmt"
	"net/rpc"
	"os"

	"ycsw/common"
)

func main() {
	client, err := rpc.DialHTTP("tcp", "172.22.154.175"+":7123")
	if err != nil {
		fmt.Printf("Dialing error: %v\n", err)
		os.Exit(1)
	}

	args := &common.Args{"712.*"}
	var reply common.Reply
	err = client.Call("RpcClient.GrepLogFile", args, &reply)
	if err == nil {
		for i, idx := range reply.LineIndices {
			fmt.Printf("Get %v: %v\n", idx, reply.Lines[i])
		}
	}
}
