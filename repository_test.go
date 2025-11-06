package badger

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func initBadgerForTesting(t *testing.T) (*repo, error) {
	tempDir, err := Path(Config{Path: t.TempDir()})
	if err != nil {
		return nil, fmt.Errorf("invalid path for initializing boltdb %s: %s", tempDir, err)
	}

	r := &repo{
		path:  tempDir,
		logFn: emptyLogFn,
		errFn: emptyLogFn,
	}

	//t.Logf("Initialized test db at %s", r.path)
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
		{
			name: "outbox multiple activities",
			args: args{
				col: vocab.IRI("http://example.com/outbox"),
				it: vocab.ItemCollection{
					vocab.Activity{ID: "http://example.com/1", Type: vocab.CreateType},
					vocab.Activity{ID: "http://example.com/2", Type: vocab.CreateType},
				},
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
				mock := it
				if vocab.IsIRI(it) {
					mock = vocab.Object{ID: it.GetLink(), Type: vocab.NoteType}
				}
				if _, err = r.Save(mock); err != nil {
					t.Errorf("unable to save %s: %s", tt.args.it, err)
				}
			}
			if err := r.AddTo(tt.args.col, tt.args.it...); (err != nil) != tt.wantErr {
				t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
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

func badgerOpen(t *testing.T) *badger.DB {
	db, _ := badger.Open(badgerOpenConfig(t.TempDir(), emptyLogFn, emptyLogFn))
	return db
}

func Test_repo_Create(t *testing.T) {
	var emptyExample = emptyCollection("https://example.com/test", vocab.IRI("https://example.com/~jdoe"))

	type fields struct {
		d     *badger.DB
		path  string
		cache cache.CanStore
	}

	tests := []struct {
		name    string
		fields  fields
		col     vocab.CollectionInterface
		want    vocab.CollectionInterface
		wantErr error
	}{
		{
			name:    "empty",
			wantErr: errNotOpen,
		},
		{
			name: "empty collection",
			fields: fields{
				d:    badgerOpen(t),
				path: t.TempDir(),
			},
			col:  emptyExample,
			want: emptyExample,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &repo{
				d:     tt.fields.d,
				path:  tt.fields.path,
				cache: tt.fields.cache,
				logFn: emptyLogFn,
				errFn: emptyLogFn,
			}
			got, err := r.Create(tt.col)
			if !cmp.Equal(err, tt.wantErr, cmpopts.EquateErrors(), cmpopts.EquateApproxTime(5*time.Second)) {
				t.Fatalf("Create() error = %s", cmp.Diff(tt.wantErr, err, cmpopts.EquateErrors()))
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("Create() got = %s", cmp.Diff(tt.want, got))
			}
			if tt.col == nil {
				return
			}
			loaded, err := r.Load(tt.col.GetLink())
			if err != nil {
				t.Fatalf("Loaded collection after Create() error = %+s", err)
			}
			if !cmp.Equal(loaded, tt.want) {
				t.Errorf("Loaded collection after Create() got = %s", cmp.Diff(tt.want, loaded))
			}
		})
	}
}
