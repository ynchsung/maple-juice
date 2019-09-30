package common

import (
	"net/rpc"
)

/* Asynchronous RPC caller
 * It should be called by creating a go-routine, and passing a channel for waiting
 */

func CallRpcClientGrepFile(host string, port string, args *ArgGrep, reply *ReplyGrepList, c chan error) {
	client, err := rpc.DialHTTP("tcp", host+port)
	if err != nil {
		c <- err
		return
	}

	c <- client.Call("RpcClient.GrepFile", args, reply)
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

func CallRpcS2SMemberJoin(host string, port string, args *ArgMemberJoin, reply *ReplyMemberJoin, c chan error) {
	client, err := rpc.DialHTTP("tcp", host+port)
	if err != nil {
		c <- err
		return
	}

	err = client.Call("RpcS2S.MemberJoin", args, reply)
	c <- err
}

func CallRpcS2SMemberAdd(host string, port string, args *ArgMemberAdd, reply *ReplyMemberAdd, c chan error) {
	client, err := rpc.DialHTTP("tcp", host+port)
	if err != nil {
		c <- err
		return
	}

	err = client.Call("RpcS2S.MemberAdd", args, reply)
	c <- err
}

func CallRpcS2SMemberLeave(host string, port string, args *ArgMemberLeave, reply *ReplyMemberLeave, c chan error) {
	client, err := rpc.DialHTTP("tcp", host+port)
	if err != nil {
		c <- err
		return
	}

	err = client.Call("RpcS2S.MemberLeave", args, reply)
	c <- err
}
