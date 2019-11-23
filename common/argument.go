package common

type ArgGrep struct {
	Paths      []string
	RequestStr string
	IsRegex    bool
}

type ArgWriteFile struct {
	Path    string
	Content []byte
}

type ArgGetMachineID int

type ArgGetMemberList int

type ArgClientMemberJoin int

type ArgClientMemberLeave int

type ArgClientUpdateFile struct {
	RequestToken string
	Filename     string
	DeleteFlag   bool
	Length       int
	Offset       int
	Content      []byte
	ForceFlag    bool
}

type ArgClientGetFile struct {
	Filename string
}

type ArgClientListHostsByFile struct {
	Filename string
}

type ArgClientListFilesByHost struct {
	MachineID int
	Regex     string
}

type ArgMemberJoin HostInfo

type ArgMemberAdd HostInfo

type ArgMemberLeave HostInfo

type ArgMemberFailure struct {
	MonitorInfo HostInfo
	FailureInfo HostInfo
}

type ArgUpdateFileVersion struct {
	Filename  string
	ForceFlag bool
}

type ArgUpdateFile struct {
	Filename   string
	DeleteFlag bool
	Version    int
	Length     int
	Offset     int
	Content    []byte
}

type ArgGetFile struct {
	Filename string
}

type ArgExistFile struct {
	Filename string
}

type ArgListFile string

// Map Reduce
type ArgMapTaskStart struct {
	ExecFilename               string
	MachineNum                 int
	IntermediateFilenamePrefix string
	InputFilenamePrefix        string
}

type ArgMapTaskPrepareWorker struct {
	ExecFilename               string
	MasterHost                 HostInfo
	WorkerList                 []MemberInfo
	WorkerNum                  int
	IntermediateFilenamePrefix string
}

type ArgMapTaskDispatch struct {
	InputFilename string
}

type ArgMapTaskSendKeyValues struct {
	Sender        HostInfo
	InputFilename string
	Data          []MapReduceKeyValue
}

type ArgMapTaskWriteIntermediateFile struct {
	RequestToken   string
	WorkerListView []MemberInfo
}

type ArgMapTaskNotifyMaster struct {
	Sender HostInfo
	Type   int
	Token  string
}

type ArgMapTaskFinish int
