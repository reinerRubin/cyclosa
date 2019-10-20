package cyclosa

import (
	"fmt"
	"io"
	"os"
)

func mergeStats(artifacts []string, targetFilename string) error {
	mergeFile, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("cant open file (%s): %s", targetFilename, err)
	}
	defer mergeFile.Close()

	for _, artifact := range artifacts {
		err := mergeStat(mergeFile, artifact)
		if err != nil {
			return fmt.Errorf("cant merge stat (%s) to %s: %s",
				artifact, targetFilename, err)
		}
	}

	if err := mergeFile.Close(); err != nil {
		return fmt.Errorf("cant close mergestat (%s): %s", targetFilename, err)
	}

	return nil
}

func mergeStat(dst io.Writer, statFilename string) error {
	stat, err := os.Open(statFilename)
	if err != nil {
		return fmt.Errorf("cant open file (%s): %s", statFilename, err)
	}
	defer stat.Close()

	_, err = io.Copy(dst, stat)
	if err != nil {
		return fmt.Errorf("cant copy stat file (%s): %s", statFilename, err)
	}

	if err := stat.Close(); err != nil {
		return fmt.Errorf("cant close stat (%s): %s", statFilename, err)
	}

	return nil
}
