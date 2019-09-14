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

	args := &common.ArgGrep{[]string{"machine.i.log"}, "712.*"}
	var reply common.ReplyGrepList
	err = client.Call("RpcClient.GrepLogFile", args, &reply)
	if err == nil {
		for _, replyGrep := range reply {
			if !replyGrep.Flag {
				fmt.Printf("Host %v error: %v\n", replyGrep.Host, replyGrep.ErrStr)
				continue
			}

			fmt.Printf("Host %v got %v lines\n", replyGrep.Host, replyGrep.LineCount)
			for _, file := range replyGrep.Files {
				fmt.Printf("\t%v\n", file.Path)
				fmt.Printf("\t===============\n")
				for _, line := range file.Lines {
					fmt.Printf("\tLine %v: %v\n", line.LineNum, line.LineStr)
				}
				fmt.Printf("\n")
			}
		}
	}
}
