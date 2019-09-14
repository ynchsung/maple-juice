package common

import (
	"log"
	"net/rpc"
)

type Args struct {
	Request string
}

type ReplyGrepObj struct {
	Host  string
	Flag  bool
	Lines []Line
}

type ReplyGrepList struct {
	Replys []ReplyGrepObj
}

type RpcClient struct {
}

type RpcS2S struct {
}

func (t *RpcClient) GrepLogFile(args *Args, reply *ReplyGrepList) error {
	var err error
	defer func() {
		if err != nil {
			log.Printf("RPC Client GrepLogFile error: %v", err)
		}
	}()

	// FIXME
	var (
		clients   []*rpc.Client
		divCalls  []*rpc.Call
		replyList []*ReplyGrepObj
	)

	for _, addr := range Cfg.Hosts {
		r := &ReplyGrepObj{addr, true, nil}
		client, err := rpc.DialHTTP("tcp", addr+Cfg.Port)
		if err == nil {
			divCalls = append(divCalls, client.Go("RpcS2S.GrepLogFile", args, r, nil))
		} else {
			clients = nil
			r.Flag = false
		}

		clients = append(clients, client)
		replyList = append(replyList, r)
	}

	for i, _ := range Cfg.Hosts {
		if clients[i] == nil {
			continue
		}

		_ = <-divCalls[i].Done
		reply.Replys = append(reply.Replys, *replyList[i])
	}
	return err
}

func (t *RpcS2S) GrepLogFile(args *Args, reply *ReplyGrepObj) error {
	var err error
	defer func() {
		if err != nil {
			log.Printf("RPC S2S GrepLogFile error: %v", err)
		}
	}()

	reply.Lines, err = GrepFile(Cfg.LogPath, args.Request)
	return err
}
