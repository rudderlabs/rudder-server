package objectdb

import "github.com/objectbox/objectbox-go/objectbox"

type Objectdb interface {
	GetOrPut(interface{}) (interface{}, error)
}

type jobStateT struct {
	isValid    bool
	isTerminal bool
	State      string
}

//State definitions
var (
	//Not valid, Not terminal
	// NotProcessed = jobStateT{isValid: false, isTerminal: false, State: "not_picked_yet"}

	//Valid, Not terminal
	failed       = jobStateT{isValid: true, isTerminal: false, State: "failed"}
	executing    = jobStateT{isValid: true, isTerminal: false, State: "executing"}
	waiting      = jobStateT{isValid: true, isTerminal: false, State: "waiting"}
	waitingRetry = jobStateT{isValid: true, isTerminal: false, State: "waiting_retry"}
	migrating    = jobStateT{isValid: true, isTerminal: false, State: "migrating"}
	importing    = jobStateT{isValid: true, isTerminal: false, State: "importing"}

	//Valid, Terminal
	succeeded   = jobStateT{isValid: true, isTerminal: true, State: "succeeded"}
	aborted     = jobStateT{isValid: true, isTerminal: true, State: "aborted"}
	migrated    = jobStateT{isValid: true, isTerminal: true, State: "migrated"}
	wontMigrate = jobStateT{isValid: true, isTerminal: true, State: "wont_migrate"}

	Failed, Executing, Waiting, WaitingRetry, Migrating, Importing, Succeeded, Aborted, Migrated, WontMigrate *CustomVal
)

//Adding a new state to this list, will require an enum change in postgres db.
var jobStates []jobStateT = []jobStateT{
	// NotProcessed,
	failed,
	executing,
	waiting,
	waitingRetry,
	migrating,
	succeeded,
	aborted,
	migrated,
	wontMigrate,
	importing,
}

var (
	WorkspaceIDMap map[string]*WorkspaceID
	CustomValMap   map[string]*CustomVal
	JobStateMap    map[string]*JobState
)

func init() {
	WorkspaceIDMap = make(map[string]*WorkspaceID)
	CustomValMap = make(map[string]*CustomVal)
	JobStateMap = make(map[string]*JobState)
}

func NewObjectBox() (*Box, error) {
	objectBox, err := objectbox.NewBuilder().Model(ObjectBoxModel()).Build()
	if err != nil {
		return nil, err
	}
	box := &Box{ObjectBox: objectBox}
	// box.customValBox = BoxForCustomVal(box.ObjectBox)
	box.jobStateBox = BoxForJobState(box.ObjectBox)
	return box, err
}

func (box *Box) CloseObjectBox() {
	box.Close()
}

type Box struct {
	ObjectBox     *objectbox.ObjectBox
	customValBox  *CustomValBox
	jobStateBox   *JobStateBox
	gatewayJobBox *GatewayJobBox
}

func (box *Box) Close() {
	box.ObjectBox.Close()
}

func (box *Box) setupJobStates(jobStateMap map[string]*JobState) error {
	jobStateBox := box.jobStateBox
	noStates, err := jobStateBox.IsEmpty()
	if err != nil {
		return err
	}
	if noStates {
		err = jobStateBox.ObjectBox.RunInWriteTx(func() error {
			for _, jobState := range jobStates {
				newState := &JobState{Name: jobState.State}
				_, putError := jobStateBox.Put(newState)
				if putError != nil {
					return putError
				}
				jobStateMap[jobState.State] = newState
			}
			return nil
		})
	}
	return err
}
