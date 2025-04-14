package badger

import (
	"fmt"
	"testing"
	"time"

	vocab "github.com/go-ap/activitypub"
)

func initBadgerForTesting(t *testing.T) (*repo, error) {
	tempDir, err := Path(Config{Path: t.TempDir()})
	if err != nil {
		return nil, fmt.Errorf("invalid path for initializing boltdb %s: %s", tempDir, err)
	}

	r := &repo{
		path:  tempDir,
		logFn: t.Logf,
		errFn: t.Errorf,
	}

	t.Logf("Initialized test db at %s", r.path)
	return r, nil
}

func orderedCollection(iri vocab.IRI) vocab.CollectionInterface {
	col := vocab.OrderedCollectionNew(iri)
	col.Published = time.Now().UTC().Truncate(time.Second)
	return col
}

func Test_repo_AddTo(t *testing.T) {
	type args struct {
		col vocab.IRI
		it  vocab.ItemCollection
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "inbox One IRI",
			args: args{
				col: vocab.IRI("http://example.com/inbox"),
				it:  vocab.ItemCollection{vocab.IRI("http://example.com/1")},
			},
			wantErr: false,
		},
		{
			name: "replies One IRI",
			args: args{
				col: vocab.IRI("http://example.com/replies"),
				it:  vocab.ItemCollection{vocab.IRI("http://example.com/1")},
			},
			wantErr: false,
		},
		{
			name: "replies multiple IRI",
			args: args{
				col: vocab.IRI("http://example.com/replies"),
				it:  vocab.ItemCollection{vocab.IRI("http://example.com/1"), vocab.IRI("http://example.com/2")},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := initBadgerForTesting(t)
			if err != nil {
				t.Errorf("Unable to initialize boltdb: %s", err)
			}
			_ = r.Open()
			defer r.Close()

			if _, err = r.Create(orderedCollection(tt.args.col)); err != nil {
				t.Errorf("unable to create collection %s: %s", tt.args.it, err)
			}
			for _, it := range tt.args.it {
				mock := vocab.Object{ID: it.GetLink()}
				if _, err = r.Save(mock); err != nil {
					t.Errorf("unable to save %s: %s", tt.args.it, err)
				}

				if err := r.AddTo(tt.args.col, it); (err != nil) != tt.wantErr {
					t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
				}
				if tt.wantErr {
					return
				}
			}

			res, err := r.Load(tt.args.col.GetLink())
			if err != nil {
				t.Errorf("unable to load %s: %s", tt.args.col, err)
			}
			for _, expected := range tt.args.it {
				err := vocab.OnCollectionIntf(res, func(col vocab.CollectionInterface) error {
					if col.Contains(expected) {
						return nil
					}
					return fmt.Errorf("unable to find expected item in loaded collection: %s", expected.GetLink())
				})
				if err != nil {
					t.Errorf("%s", err)
				}
			}
		})
	}
}
