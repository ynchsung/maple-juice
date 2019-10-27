package common

type ReplyGrep struct {
	Host      string
	Flag      bool
	ErrStr    string
	LineCount int
	Files     []*GrepInfo
}

type ReplyGrepList []*ReplyGrep

type ReplyWriteFile struct {
	Flag        bool
	ErrStr      string
	ByteWritten int
}

type ReplyGetMachineID HostInfo

type ReplyGetMemberList []MemberInfo

type ReplyClientMemberJoin struct {
	Flag   bool
	ErrStr string
}

type ReplyClientMemberLeave struct {
	Flag   bool
	ErrStr string
}

type ReplyClientPutFile struct {
	Flag   bool
	ErrStr string
}

type ReplyClientGetFile struct {
	Flag    bool
	ErrStr  string
	Length  int
	Content []byte
}

type ReplyClientDeleteFile struct {
	Flag   bool
	ErrStr string
}

type ReplyMemberJoin struct {
	Flag   bool
	ErrStr string
}

type ReplyMemberAdd struct {
	Flag   bool
	ErrStr string
}

type ReplyMemberLeave struct {
	Flag   bool
	ErrStr string
}

type ReplyMemberFailure struct {
	Flag   bool
	ErrStr string
}

type ReplyPutFile struct {
	Flag   bool
	ErrStr string
}

type ReplyGetFile struct {
	Flag    bool
	ErrStr  string
	Length  int
	Content []byte
}

type ReplyDeleteFile struct {
	Flag   bool
	ErrStr string
}
