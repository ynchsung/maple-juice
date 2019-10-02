package common

type ReplyGrep struct {
	Host      string
	Flag      bool
	ErrStr    string
	LineCount int
	Files     []*GrepInfo
}

type ReplyGrepList []*ReplyGrep

type ReplyShutdown int

type ReplyWriteFile struct {
	Flag        bool
	ErrStr      string
	ByteWritten int
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
