package batch

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type S3DeleteManager struct {
}

//reason behind using sed: https://www.rtuin.nl/2012/01/fast-search-and-replace-in-large-files-with-sed/
//Delete user details corresponding to `userAttributes` from `uncompressedFileName` & delete `uncompuressedFileName`
func (dm *S3DeleteManager) delete(ctx context.Context, userAttributes []model.UserAttribute, uncompressedFileName string) ([]byte, error) {

	//actual delete
	var n int
	for i := 0; i < len(userAttributes); i++ {
		n += len(userAttributes[i].UserID) + 4
		if userAttributes[i].Email != nil {
			n += len(*userAttributes[i].Email) + 4
		}
		if userAttributes[i].Phone != nil {
			n += len(*userAttributes[i].Phone) + 4
		}
	}

	searchObject := make([]byte, 0, n)
	for _, users := range userAttributes {
		searchObject = append(searchObject, "/"...)
		searchObject = append(searchObject, users.UserID...)
		searchObject = append(searchObject, "/d;"...)

		if users.Email != nil {
			searchObject = append(searchObject, "/"...)
			searchObject = append(searchObject, []byte(*users.Email)...)
			searchObject = append(searchObject, "/d;"...)
		}

		if users.Phone != nil {
			searchObject = append(searchObject, "/"...)
			searchObject = append(searchObject, []byte(*users.Phone)...)
			searchObject = append(searchObject, "/d;"...)
		}
	}

	out, err := exec.Command("sed", "-e", string(searchObject), uncompressedFileName).Output()
	if err != nil {
		return nil, fmt.Errorf("error while running sed command: %s", err)
	}

	os.Remove(uncompressedFileName)

	return out, nil
}
