package badger

import (
	"fmt"
	"os"

	vocab "github.com/go-ap/activitypub"
	ap "github.com/go-ap/fedbox/activitypub"
	"github.com/go-ap/jsonld"
)

var encodeFn = jsonld.Marshal
var decodeFn = jsonld.Unmarshal

func Bootstrap(conf Config, url string) error {
	r, err := New(conf)
	if err != nil {
		return err
	}
	self := ap.Self(ap.DefaultServiceIRI(url))
	actors := &vocab.OrderedCollection{ID: ap.ActorsType.IRI(&self)}
	activities := &vocab.OrderedCollection{ID: ap.ActivitiesType.IRI(&self)}
	objects := &vocab.OrderedCollection{ID: ap.ObjectsType.IRI(&self)}
	if _, err = r.Create(actors); err != nil {
		return err
	}
	if _, err = r.Create(activities); err != nil {
		return err
	}
	if _, err = r.Create(objects); err != nil {
		return err
	}
	return nil
}

func Clean(conf Config) error {
	path, err := Path(conf)
	if err != nil {
		return fmt.Errorf("unable to update %s db: %w", "badger", err)
	}

	return os.RemoveAll(path)
}
