package common

import (
	"net/rpc"
)

type RpcAsyncCallerTask struct {
	RpcName string
	Info    *HostInfo
	Args    interface{}
	Reply   interface{}
	Chan    chan error
}

/* Asynchronous RPC caller
 * It should be called by creating a go-routine, and passing a channel for waiting
 */

// TODO: add 1 second timeout

func CallRpcClientGeneral(task *RpcAsyncCallerTask) {
	client, err := rpc.DialHTTP("tcp", task.Info.Host+task.Info.Port)
	if err != nil {
		task.Chan <- err
		return
	}

	task.Chan <- client.Call("RpcClient."+task.RpcName, task.Args, task.Reply)
}

func CallRpcS2SGeneral(task *RpcAsyncCallerTask) {
	client, err := rpc.DialHTTP("tcp", task.Info.Host+task.Info.Port)
	if err != nil {
		task.Chan <- err
		return
	}

	task.Chan <- client.Call("RpcS2S."+task.RpcName, task.Args, task.Reply)
}

func CallRpcS2SGrepFile(host string, port string, args *ArgGrep, reply *ReplyGrep, c chan error) {
	var err error
	defer func() {
		if err != nil {
			reply.Flag = false
			reply.ErrStr = err.Error()
		}
	}()

	client, err := rpc.DialHTTP("tcp", host+port)
	if err != nil {
		c <- err
		return
	}

	err = client.Call("RpcS2S.GrepFile", args, reply)
	c <- err
}

func CallRpcS2SWriteFile(host string, port string, args *ArgWriteFile, reply *ReplyWriteFile, c chan error) {
	var err error
	defer func() {
		if err != nil {
			reply.Flag = false
			reply.ErrStr = err.Error()
		}
	}()

	client, err := rpc.DialHTTP("tcp", host+port)
	if err != nil {
		c <- err
		return
	}

	err = client.Call("RpcS2S.WriteFile", args, reply)
	c <- err
}
