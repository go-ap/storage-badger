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

	if err := vocab.OnActor(self, r.CreateService); err != nil {
		return err
	}
	return vocab.OnActor(self, func(service *vocab.Actor) error {
		for _, stream := range service.Streams {
			if _, err := r.Create(&vocab.OrderedCollection{ID: stream.GetID()}); err != nil {
				r.errFn("Unable to create %s collection for actor %s", stream.GetID(), service.GetLink())
			}
		}
		return nil
	})
}

func Clean(conf Config) error {
	path, err := Path(conf)
	if err != nil {
		return fmt.Errorf("unable to update %s db: %w", "badger", err)
	}

	return os.RemoveAll(path)
}
