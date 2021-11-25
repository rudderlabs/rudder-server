package batch

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

type S3DeleteManager struct {
}

//reason behind using sed: https://www.rtuin.nl/2012/01/fast-search-and-replace-in-large-files-with-sed/
//Delete user details corresponding to `userAttributes` from `uncompressedFileName` & delete `uncompuressedFileName`
func (dm *S3DeleteManager) delete(ctx context.Context, searchPatternFile, uncompressedFileName string) ([]byte, error) {

	//actual delete
	out, err := exec.Command("sed", "-f", searchPatternFile, uncompressedFileName).Output()
	if err != nil {
		fmt.Println("sed error= ", err)
		return nil, fmt.Errorf("error while running sed command: %s", err)
	}

	os.Remove(uncompressedFileName)

	return out, nil
}
