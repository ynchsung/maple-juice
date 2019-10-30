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

type ReplyClientUpdateFile struct {
	Flag   bool
	ErrStr string
}

type ReplyClientGetFile struct {
	Flag    bool
	ErrStr  string
	Length  int
	Content []byte
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

type ReplyUpdateFileVersion struct {
	Flag    bool
	ErrStr  string
	Version int
}

type ReplyUpdateFile struct {
	Flag   bool
	ErrStr string
}

type ReplyGetFile struct {
	Flag    bool
	ErrStr  string
	Length  int
	Content []byte
}
