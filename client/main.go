package main

import (
	"fmt"
	"os"

	"ycsw/common"
)

func main() {
	var (
		args common.ArgGrep = common.ArgGrep{
			[]string{
				"vm1.log",
				"vm2.log",
				"vm3.log",
				"vm4.log",
				"vm5.log",
				"vm6.log",
				"vm7.log",
				"vm8.log",
				"vm9.log",
				"vm10.log",
			},
			"^[0-9]*[a-z]{5}",
		}
		reply common.ReplyGrepList
	)

	c := make(chan error)
	go common.CallRpcClientGrepFile("172.22.154.175", ":7123", &args, &reply, c)
	err := <-c
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	for _, replyGrep := range reply {
		if !replyGrep.Flag {
			fmt.Printf("Host %v error: %v\n", replyGrep.Host, replyGrep.ErrStr)
			continue
		}

		fmt.Printf("Host %v got %v lines\n", replyGrep.Host, replyGrep.LineCount)
		for _, file := range replyGrep.Files {
			fmt.Printf("\t%v has %v lines\n", file.Path, len(file.Lines))
			/*
				fmt.Printf("\t--\n")
				for _, line := range file.Lines {
					fmt.Printf("\tLine %v: %v\n", line.LineNum, line.LineStr)
				}
				fmt.Printf("\n\t========================\n\n")
			*/
		}
	}
}
