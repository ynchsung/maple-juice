package common

import (
	"log"
	"os"
)

type RpcClient struct {
}

type RpcS2S struct {
}

func (t *RpcClient) GrepFile(args *ArgGrep, reply *ReplyGrepList) error {
	var tasks []RpcAsyncCallerTask

	// Send RpcS2S Grep File for all machines in the cluster
	for _, host := range Cfg.ClusterInfo.Hosts {
		r := &ReplyGrep{host.Host, true, "", 0, []*GrepInfo{}}
		c := make(chan error)

		go CallRpcS2SGrepFile(host.Host, host.Port, args, r, c)

		tasks = append(tasks, RpcAsyncCallerTask{"GrepFile", &host, args, r, c})
		*reply = append(*reply, r)
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		if err != nil {
			log.Printf("[Error] Fail to send GrepFile to %v: %v",
				task.Info.Host,
				err,
			)
		}
	}

	return nil
}

func (t *RpcClient) Shutdown(args *ArgShutdown, reply *ReplyShutdown) error {
	var tasks []RpcAsyncCallerTask

	// Send RpcS2S Leave to all members
	members := GetMemberList()
	args2 := ArgMemberLeave(Cfg.Self)
	for _, mem := range members {
		if mem.Info.Host == Cfg.Self.Host {
			continue
		}

		task := RpcAsyncCallerTask{
			"MemberLeave",
			&mem.Info,
			&args2,
			&ReplyMemberLeave{true, ""},
			make(chan error),
		}

		go CallRpcS2SGeneral(&task)

		tasks = append(tasks, task)
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		if err != nil {
			log.Printf("[Error] Fail to send MemberLeave to %v: %v",
				task.Info.Host,
				err,
			)
		}
	}

	log.Printf("[Info] Shutdown")
	os.Exit(0)

	return nil
}

func (t *RpcS2S) GrepFile(args *ArgGrep, reply *ReplyGrep) error {
	reply.Host = Cfg.Self.Host
	reply.Flag = true
	reply.ErrStr = ""
	reply.LineCount = 0
	reply.Files = []*GrepInfo{}
	for _, path := range args.Paths {
		info, err := GrepFile(path, args.RequestStr, args.IsRegex)
		if err == nil {
			reply.LineCount += len(info.Lines)
			reply.Files = append(reply.Files, info)
		}
	}

	return nil
}

func (t *RpcS2S) WriteFile(args *ArgWriteFile, reply *ReplyWriteFile) error {
	var err error = nil

	reply.Flag = true
	reply.ErrStr = ""
	reply.ByteWritten, err = WriteFile(args.Path, args.Content)
	if err != nil {
		reply.Flag = false
		reply.ErrStr = err.Error()
		reply.ByteWritten = 0
	}

	return nil
}

func (t *RpcS2S) MemberJoin(args *ArgMemberJoin, reply *ReplyMemberJoin) error {
	log.Printf("[Info] MemberJoin: host %v, port %v, udp_port %v, id %v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	if Cfg.Self.Host == Cfg.Introducer.Host {
		// introducer should forward add member to all other members
		var tasks []RpcAsyncCallerTask

		members := GetMemberList()
		for _, mem := range members {
			if mem.Info.Host == Cfg.Self.Host {
				continue
			}

			task := RpcAsyncCallerTask{
				"MemberJoin",
				&mem.Info,
				args,
				&ReplyMemberJoin{true, ""},
				make(chan error),
			}

			go CallRpcS2SGeneral(&task)

			tasks = append(tasks, task)
		}

		// Wait for all RpcAsyncCallerTask
		for _, task := range tasks {
			err := <-task.Chan
			if err != nil {
				log.Printf("[Error] Fail to send MemberJoin to %v: %v",
					task.Info.Host,
					err,
				)
			}
		}
	}

	// ping the new node to add myself
	arg2 := ArgMemberAdd(Cfg.Self)

	task := RpcAsyncCallerTask{
		"MemberAdd",
		&HostInfo{args.Host, args.Port, "", 0},
		&arg2,
		&ReplyMemberAdd{},
		make(chan error),
	}

	go CallRpcS2SGeneral(&task)

	err := <-task.Chan
	if err != nil {
		log.Printf("[Error] Fail to send MemberAdd to %v: %v",
			args.Host,
			err,
		)
	}

	AddMember(HostInfo(*args))
	return nil
}

func (t *RpcS2S) MemberAdd(args *ArgMemberAdd, reply *ReplyMemberAdd) error {
	log.Printf("[Info] MemberAdd: host %v, port %v, udp_port %v, id %v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	AddMember(HostInfo(*args))
	return nil
}

func (t *RpcS2S) MemberLeave(args *ArgMemberLeave, reply *ReplyMemberLeave) error {
	log.Printf("[Info] MemberLeave: host %v, port %v, udp_port %v, id %v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	DeleteMember(HostInfo(*args))
	return nil
}

func (t *RpcS2S) MemberFailure(args *ArgMemberFailure, reply *ReplyMemberFailure) error {
	log.Printf("[Info] MemberFailure: host %v, port %v, udp_port %v, id %v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	DeleteMember(HostInfo(*args))
	return nil
}
