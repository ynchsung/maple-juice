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
	Flag      bool
	ErrStr    string
	Finish    bool
	NeedForce bool
}

type ReplyClientGetFile struct {
	Flag    bool
	ErrStr  string
	Length  int
	Content []byte
}

type ReplyClientListHostsByFile struct {
	Flag   bool
	ErrStr string
	Hosts  []HostInfo
}

type ReplyClientListFilesByHost struct {
	Flag   bool
	ErrStr string
	Files  []SDFSFileInfo2
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
	Flag      bool
	ErrStr    string
	NeedForce bool
	Version   int
}

type ReplyUpdateFile struct {
	Flag   bool
	ErrStr string
	Finish bool
}

type ReplyGetFile struct {
	Flag       bool
	ErrStr     string
	DeleteFlag bool
	Version    int
	Length     int
	Content    []byte
}

type ReplyExistFile bool

type ReplyListFile []SDFSFileInfo2

// Map Reduce
type ReplyMapTaskStart struct {
	Flag   bool
	ErrStr string
}

type ReplyMapTaskPrepareWorker struct {
	Flag   bool
	ErrStr string
}

type ReplyMapTaskDispatch struct {
	Flag   bool
	ErrStr string
}

type ReplyMapTaskSendKeyValues struct {
	Flag   bool
	ErrStr string
}

type ReplyMapTaskGetKeyValues struct {
	Flag   bool
	ErrStr string
	Data   map[string][]MapReduceKeyValue
}

type ReplyMapTaskWriteIntermediateFile struct {
	Flag   bool
	ErrStr string
}

type ReplyMapTaskNotifyMaster struct {
	Flag   bool
	ErrStr string
}

type ReplyMapTaskFinish struct {
	Flag   bool
	ErrStr string
}
