package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/misc"
)

// RecoveryDataT : DS to store the recovery process data
type RecoveryDataT struct {
	StartTimes []int64
}

func getRecoveryData() []byte {
	storagePath := config.GetString("recovery.storagePath", "/tmp/recovery_data.json")
	data, err := ioutil.ReadFile(storagePath)
	if os.IsNotExist(err) {
		data = []byte("{}")
	}
	return data
}

/*
RecordAppStart : Log App starts to monitor crashes
*/
func RecordAppStart() {
	enabled := config.GetBool("recovery.enabled", false)
	if !enabled {
		return
	}

	storagePath := config.GetString("recovery.storagePath", "/tmp/recovery_data.json")

	var recoveryData RecoveryDataT
	err := json.Unmarshal(getRecoveryData(), &recoveryData)
	misc.AssertError(err)

	maxCrashes := config.GetInt("recovery.crashThreshold", 5)

	recoveryData.StartTimes = append(recoveryData.StartTimes, time.Now().Unix())
	arrayLength := len(recoveryData.StartTimes)

	// We store upto 2*maxCrashes for more data during debugging
	if arrayLength > 2*maxCrashes {
		recoveryData.StartTimes = recoveryData.StartTimes[arrayLength-(2*maxCrashes) : arrayLength]
	}
	recoveryDataJSON, err := json.Marshal(&recoveryData)
	misc.AssertError(err)

	err = ioutil.WriteFile(storagePath, recoveryDataJSON, 0644)
	misc.AssertError(err)
}

/*
CheckProbableInconsistentDB : If the server is crashing frequently,
that could be because of a DB with inconsistent data.
We try to recover by renaming the OLD DB and creating a new Database
This has the side effect of writing the server starts
This should be called only in the main() and once.
*/
func CheckProbableInconsistentDB() (shouldResetDB bool) {
	enabled := config.GetBool("recovery.enabled", false)
	if !enabled {
		return
	}

	var recoveryData RecoveryDataT
	err := json.Unmarshal(getRecoveryData(), &recoveryData)
	misc.AssertError(err)

	maxCrashes := config.GetInt("recovery.crashThreshold", 5)
	duration := config.GetInt("recovery.durationInS", 300)

	sort.Slice(recoveryData.StartTimes, func(i, j int) bool {
		return recoveryData.StartTimes[i] < recoveryData.StartTimes[j]
	})

	recentCrashCount := 0
	checkPointTime := time.Now().Unix() - int64(duration*1000)

	for i := len(recoveryData.StartTimes) - 1; i >= 0; i-- {
		if recoveryData.StartTimes[i] < checkPointTime {
			break
		}
		recentCrashCount++
	}
	if recentCrashCount >= maxCrashes {
		shouldResetDB = true
	}
	return
}

/*
IsDBPresent - Check if the DB is present
*/
func IsDBPresent() bool {
	db, err := sql.Open("postgres", GetConnectionString())
	if err != nil {
		return false
	}
	defer db.Close()
	return true
}

/*
ReplaceDB : Rename the OLD DB and create a new one.
Since we are not journaling, this should be idemponent
*/
func ReplaceDB() {
	enabled := config.GetBool("recovery.enabled", false)
	if !enabled {
		return
	}

	fmt.Println("Starting Recovery. Connecting to default DB 'postgres'")
	connInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		host, port, user, password)
	db, err := sql.Open("postgres", connInfo)
	misc.AssertError(err)
	defer db.Close()

	renameDBStatement := fmt.Sprintf("ALTER DATABASE %s RENAME TO %s",
		dbname, "original_"+dbname+"_"+strconv.FormatInt(time.Now().Unix(), 10))
	fmt.Println(renameDBStatement)
	_, err = db.Exec(renameDBStatement)

	// If we crashed earlier, after ALTER but before CREATE, we can create again
	// So just logging the error instead of assert
	if err != nil {
		fmt.Println(err.Error())
	}

	createDBStatement := fmt.Sprintf("CREATE DATABASE %s", dbname)
	_, err = db.Exec(createDBStatement)
	misc.AssertError(err)

}
