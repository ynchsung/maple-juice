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
	var reply common.ReplyGrepList
	err = client.Call("RpcClient.GrepLogFile", args, &reply)
	if err == nil {
		for _, replyGrepObj := range reply.Replys {
			fmt.Printf("Host %v\n", replyGrepObj.Host)
			for _, line := range replyGrepObj.Lines {
				fmt.Printf("\tLine %v: %v\n", line.LineNum, line.LineStr)
			}
		}
	}
}
