package common

import (
	"errors"
)

func SDFSDownloadFile(filename string, host HostInfo) ([]byte, int, error) {
	args := &ArgClientGetFile{filename}
	reply := new(ReplyClientGetFile)

	task := &RpcAsyncCallerTask{
		"GetFile",
		host,
		args,
		reply,
		make(chan error),
	}

	go CallRpcClientGeneral(task)

	err := <-task.Chan
	if err == nil && !reply.Flag {
		err = errors.New(reply.ErrStr)
	}

	return reply.Content, reply.Length, err
}

func SDFSUploadFileChunk(host HostInfo, token string, filename string, length int, offset int, content []byte, force bool) (*ReplyClientUpdateFile, error) {
	args := &ArgClientUpdateFile{
		token,
		filename,
		false,
		length,
		offset,
		content,
		force,
	}
	reply := new(ReplyClientUpdateFile)

	task := RpcAsyncCallerTask{
		"UpdateFile",
		host,
		args,
		reply,
		make(chan error),
	}

	go CallRpcClientGeneral(&task)

	err := <-task.Chan
	return task.Reply.(*ReplyClientUpdateFile), err
}

func SDFSUploadFile(host HostInfo, filename string, content []byte, force bool) (bool, bool, error) {
	token := GenRandomString(16)

	// prepare chunk
	finish := false
	offset := 0
	l := len(content)
	for offset < l {
		end := offset + SDFS_MAX_BUFFER_SIZE
		if end > l {
			end = l
		}

		reply, err := SDFSUploadFileChunk(host, token, filename, l, offset, content[offset:end], force)
		if err != nil {
			return false, false, err
		}

		if !reply.Flag {
			return false, reply.NeedForce, errors.New(reply.ErrStr)
		}

		if reply.Finish {
			finish = true
		}

		offset = end
	}

	return finish, false, nil
}
