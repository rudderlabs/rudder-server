package batch

import (
	"context"
	"fmt"
	"os/exec"
)

type S3DeleteManager struct {
}

//reason behind using sed: https://www.rtuin.nl/2012/01/fast-search-and-replace-in-large-files-with-sed/
//Delete user details corresponding to `userAttributes` from `uncompressedFileName` & delete `uncompuressedFileName`
func (dm *S3DeleteManager) delete(ctx context.Context, patternFile, decompressedFile string) ([]byte, error) {
	pkgLogger.Debugf("deleting pattern in file: %w", patternFile, " from decompressed file: %w", decompressedFile, "using sed command")
	//actual delete
	out, err := exec.Command("sed", "-f", patternFile, decompressedFile).Output()
	if err != nil {
		pkgLogger.Errorf("error while executing sed command: %v", err)
		return nil, fmt.Errorf("error while running sed command: %s", err)
	}

	pkgLogger.Debugf("deletion successful from file: %w", decompressedFile)
	return out, nil
}
