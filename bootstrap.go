package badger

import (
	"fmt"
	"os"

	vocab "github.com/go-ap/activitypub"
)

func Bootstrap(conf Config, self vocab.Item) error {
	r, err := New(conf)
	if err != nil {
		return err
	}
	return vocab.OnActor(self, r.CreateService)
}

func Clean(conf Config) error {
	path, err := Path(conf)
	if err != nil {
		return fmt.Errorf("unable to update %s db: %w", "badger", err)
	}

	return os.RemoveAll(path)
}
