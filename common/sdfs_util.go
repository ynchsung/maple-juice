package common

import (
	"errors"
)

func SDFSDownloadFile(filename string, host HostInfo) ([]byte, int, error) {
	// try to get file from local replica
	_, deleteFlag, length, content, err := SDFSReadFile(filename)
	if err == nil {
		if deleteFlag {
			return nil, 0, errors.New("file not exist")
		} else {
			return content, length, nil
		}
	}

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

	err = <-task.Chan
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

func SDFSUploadFile2(host HostInfo, filename string, content [][]byte, length int, force bool) (bool, bool, error) {
	token := GenRandomString(16)

	// prepare chunk
	finish := false
	offset := 0
	for _, sub_content := range content {
		end := offset + len(sub_content)

		reply, err := SDFSUploadFileChunk(host, token, filename, length, offset, sub_content, force)
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

func SDFSDeleteFile(host HostInfo, filename string, force bool) (bool, bool, error) {
	args := &ArgClientUpdateFile{
		GenRandomString(16),
		filename,
		true,
		0,
		0,
		nil,
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
	if err != nil {
		return false, false, err
	}

	if !reply.Flag {
		return false, reply.NeedForce, errors.New(reply.ErrStr)
	}

	return reply.Finish, reply.NeedForce, nil
}
