package main

import (
	"fmt"
	"os"

	"ycsw/common"
)

func go_grep_log(args *common.ArgGrep, reply *common.ReplyGrepList, flag bool) {
	// call RpcClient Grep File
	c := make(chan error)
	go common.CallRpcClientGeneral("GrepFile", os.Args[2], os.Args[3], args, reply, c)
	err := <-c
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// print the result
	lineCount := 0
	for _, replyGrep := range *reply {
		if !replyGrep.Flag {
			fmt.Printf("Host %v error: %v\n", replyGrep.Host, replyGrep.ErrStr)
			continue
		}

		fmt.Printf("Host %v got %v lines\n", replyGrep.Host, replyGrep.LineCount)
		for _, file := range replyGrep.Files {
			fmt.Printf("\t%v has %v lines\n", file.Path, len(file.Lines))
			lineCount += len(file.Lines)

			if flag {
				fmt.Printf("\t--\n")
				for _, line := range file.Lines {
					fmt.Printf("\tLine %v: %v\n", line.LineNum, line.LineStr)
				}
				fmt.Printf("\n\t========================\n\n")
			}
		}
	}

	fmt.Printf("\n\nTotal: %v lines\n", lineCount)
}

func demo_log() {
	isRegex := true
	if os.Args[5] == "pattern" {
		isRegex = false
	}

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
			os.Args[4],
			isRegex,
		}
		reply common.ReplyGrepList
	)
	go_grep_log(&args, &reply, false)
}

func grep_log() {
	isRegex := true
	if os.Args[5] == "pattern" {
		isRegex = false
	}

	var (
		args common.ArgGrep = common.ArgGrep{
			[]string{"machine.i.log"},
			os.Args[4],
			isRegex,
		}
		reply common.ReplyGrepList
	)
	go_grep_log(&args, &reply, true)
}

func shutdown() {
	var (
		args  common.ArgShutdown = 1
		reply common.ReplyShutdown
	)

	// call RpcClient Grep File
	c := make(chan error)
	go common.CallRpcClientGeneral("Shutdown", os.Args[2], os.Args[3], &args, &reply, c)
	err := <-c
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func main() {
	if os.Args[1] == "log" {
		grep_log()
	} else if os.Args[1] == "demo_log" {
		demo_log()
	} else if os.Args[1] == "shutdown" {
		shutdown()
	}
}
