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
	NotProcessed = jobStateT{isValid: false, isTerminal: false, State: "not_picked_yet"}

	//Valid, Not terminal
	Failed       = jobStateT{isValid: true, isTerminal: false, State: "failed"}
	Executing    = jobStateT{isValid: true, isTerminal: false, State: "executing"}
	Waiting      = jobStateT{isValid: true, isTerminal: false, State: "waiting"}
	WaitingRetry = jobStateT{isValid: true, isTerminal: false, State: "waiting_retry"}
	Migrating    = jobStateT{isValid: true, isTerminal: false, State: "migrating"}
	Importing    = jobStateT{isValid: true, isTerminal: false, State: "importing"}

	//Valid, Terminal
	Succeeded   = jobStateT{isValid: true, isTerminal: true, State: "succeeded"}
	Aborted     = jobStateT{isValid: true, isTerminal: true, State: "aborted"}
	Migrated    = jobStateT{isValid: true, isTerminal: true, State: "migrated"}
	WontMigrate = jobStateT{isValid: true, isTerminal: true, State: "wont_migrate"}
)

//Adding a new state to this list, will require an enum change in postgres db.
var jobStates []jobStateT = []jobStateT{
	NotProcessed,
	Failed,
	Executing,
	Waiting,
	WaitingRetry,
	Migrating,
	Succeeded,
	Aborted,
	Migrated,
	WontMigrate,
	Importing,
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

func (box *Box) setupJobStates(jobStateMap map[string]*JobState) error {
	jobStateBox := BoxForJobState(box.ObjectBox)
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

func NewObjectBox() (*Box, error) {
	objectBox, err := objectbox.NewBuilder().Model(ObjectBoxModel()).Build()
	if err != nil {
		return nil, err
	}
	box := &Box{ObjectBox: objectBox}
	err = box.setupJobStates(JobStateMap)
	return box, err
}

func (box *Box) CloseObjectBox() {
	box.Close()
}

type Box struct {
	ObjectBox *objectbox.ObjectBox
}

func (box *Box) Close() {
	box.ObjectBox.Close()
}
