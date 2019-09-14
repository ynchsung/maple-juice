package common

import (
	"fmt"
	"log"
	"net/rpc"
)

type Args struct {
	Request string
}

type Reply struct {
	LineIndices []int
	Lines       []string
}

type RpcClient struct {
}

type RpcS2S struct {
}

func (t *RpcClient) GrepLogFile(args *Args, reply *Reply) error {
	var err error
	defer func() {
		if err != nil {
			log.Printf("RPC Client GrepLogFile error: %v", err)
		}
	}()

	// FIXME
	var (
		clients  []*rpc.Client
		divCalls []*rpc.Call
		replys   []*Reply
	)

	for _, addr := range Cfg.Hosts {
		client, err := rpc.DialHTTP("tcp", addr+Cfg.Port)
		if err != nil {
			clients = append(clients, nil)
			replys = append(replys, nil)
			continue
		}

		r := new(Reply)
		clients = append(clients, client)
		replys = append(replys, r)

		divCalls = append(divCalls, client.Go("RpcS2S.GrepLogFile", args, r, nil))
	}

	for i, addr := range Cfg.Hosts {
		if clients[i] == nil {
			fmt.Printf("Host %v: not available\n", addr)
			continue
		}

		_ = <-divCalls[i].Done
		fmt.Printf("Host %v:\n", addr)
		for j, idx := range replys[i].LineIndices {
			fmt.Printf("\tLine %v: %v\n", idx, replys[i].Lines[j])
		}
		fmt.Printf("\n=====\n")
	}
	return err
}

func (t *RpcS2S) GrepLogFile(args *Args, reply *Reply) error {
	var err error
	defer func() {
		if err != nil {
			log.Printf("RPC S2S GrepLogFile error: %v", err)
		}
	}()

	reply.LineIndices, reply.Lines, err = GrepFile(Cfg.LogPath, args.Request)
	return err
}
