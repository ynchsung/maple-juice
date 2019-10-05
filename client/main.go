package main

import (
	"fmt"
	"os"

	"ycsw/common"
)

func go_grep_log(args *common.ArgGrep, reply *common.ReplyGrepList, flag bool) {
	// call RpcClient Grep File
	task := common.RpcAsyncCallerTask{
		"GrepFile",
		common.HostInfo{os.Args[2], os.Args[3], "", 0},
		args,
		reply,
		make(chan error),
	}

	go common.CallRpcClientGeneral(&task)

	err := <-task.Chan
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

func get_machine_id() {
	var (
		args  common.ArgGetMachineID = 1
		reply common.ReplyGetMachineID
	)

	task := common.RpcAsyncCallerTask{
		"GetMachineID",
		common.HostInfo{os.Args[2], os.Args[3], "", 0},
		&args,
		&reply,
		make(chan error),
	}

	go common.CallRpcClientGeneral(&task)

	err := <-task.Chan
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Host %v, port %v, udp_port %v, id %v\n",
			reply.Host,
			reply.Port,
			reply.UdpPort,
			reply.MachineID,
		)
	}
}

func get_member_list() {
	var (
		args  common.ArgGetMemberList = 1
		reply common.ReplyGetMemberList
	)

	task := common.RpcAsyncCallerTask{
		"GetMemberList",
		common.HostInfo{os.Args[2], os.Args[3], "", 0},
		&args,
		&reply,
		make(chan error),
	}

	go common.CallRpcClientGeneral(&task)

	err := <-task.Chan
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Get %v members\n", len(reply))
		for _, mem := range reply {
			fmt.Printf("Host %v, id %v, timestamp %v\n", mem.Info.Host, mem.Info.MachineID, mem.Timestamp.Unix())
		}
	}
}

func member_join() {
	var (
		args  common.ArgClientMemberJoin = 1
		reply common.ReplyClientMemberJoin
	)

	task := common.RpcAsyncCallerTask{
		"MemberJoin",
		common.HostInfo{os.Args[2], os.Args[3], "", 0},
		&args,
		&reply,
		make(chan error),
	}

	go common.CallRpcClientGeneral(&task)

	err := <-task.Chan
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("MemberJoin result %v\n", reply.Flag)
		if !reply.Flag {
			fmt.Printf("error %v\n", reply.ErrStr)
		}
	}
}

func member_leave() {
	var (
		args  common.ArgClientMemberLeave = 1
		reply common.ReplyClientMemberLeave
	)

	task := common.RpcAsyncCallerTask{
		"MemberLeave",
		common.HostInfo{os.Args[2], os.Args[3], "", 0},
		&args,
		&reply,
		make(chan error),
	}

	go common.CallRpcClientGeneral(&task)

	err := <-task.Chan
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("MemberLeave result %v\n", reply.Flag)
		if !reply.Flag {
			fmt.Printf("error %v\n", reply.ErrStr)
		}
	}
}

func main() {
	if os.Args[1] == "log" {
		grep_log()
	} else if os.Args[1] == "demo_log" {
		demo_log()
	} else if os.Args[1] == "get_machine_id" {
		get_machine_id()
	} else if os.Args[1] == "get_member_list" {
		get_member_list()
	} else if os.Args[1] == "member_join" {
		member_join()
	} else if os.Args[1] == "member_leave" {
		member_leave()
	}
}
