package badger

import (
	"os"
)

func Bootstrap(_ Config) error {
	return nil
}

func Clean(conf Config) error {
	path, err := Path(conf)
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}
