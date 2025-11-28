package badger

import "os"

func Bootstrap(conf Config) error {
	var err error
	if conf.Path, err = Path(conf); err != nil {
		return err
	}
	return err
}

func Clean(conf Config) error {
	path, err := Path(conf)
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}
