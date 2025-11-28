package badger

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
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
				root:  tt.fields.d,
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

func Test_repo_Load(t *testing.T) {
	// NOTE(marius): happy path tests for a fully mocked repo
	r := mockRepo(t, fields{path: t.TempDir()}, withOpenRoot, withGeneratedMocks)
	t.Cleanup(r.Close)

	type args struct {
		iri vocab.IRI
		fil filters.Checks
	}
	tests := []struct {
		name    string
		args    args
		want    vocab.Item
		wantErr error
	}{
		{
			name:    "empty",
			args:    args{iri: ""},
			want:    nil,
			wantErr: errors.NotFoundf("file not found"),
		},
		{
			name:    "empty iri gives us not found",
			args:    args{iri: ""},
			want:    nil,
			wantErr: errors.NotFoundf("file not found"),
		},
		{
			name: "root iri gives us the root",
			args: args{iri: "https://example.com"},
			want: root,
		},
		{
			name:    "invalid iri gives 404",
			args:    args{iri: "https://example.com/dsad"},
			want:    nil,
			wantErr: errors.NotFoundf("dsad not found"),
		},
		{
			name: "first Person",
			args: args{iri: "https://example.com/person/1"},
			want: filter(*allActors.Load(), filters.HasType("Person")).First(),
		},
		{
			name: "first Follow",
			args: args{iri: "https://example.com/follow/1"},
			want: filter(*allActivities.Load(), filters.HasType("Follow")).First(),
		},
		{
			name: "first Image",
			args: args{iri: "https://example.com/image/1"},
			want: filter(*allObjects.Load(), filters.SameID("https://example.com/image/1")).First(),
		},
		{
			name: "full outbox",
			args: args{iri: rootOutboxIRI},
			want: wantsRootOutbox(),
		},
		{
			name: "limit to max 2 things",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{filters.WithMaxCount(2)},
			},
			want: wantsRootOutboxPage(2, filters.WithMaxCount(2)),
		},
		{
			name: "inbox?type=Create",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
				},
			},
			want: wantsRootOutbox(filters.HasType(vocab.CreateType)),
		},
		{
			name: "inbox?type=Create&actor.name=Hank",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
					filters.Actor(filters.NameIs("Hank")),
				},
			},
			want: wantsRootOutbox(
				filters.HasType(vocab.CreateType),
				filters.Actor(filters.NameIs("Hank")),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.Load(tt.args.iri, tt.args.fil...)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(tt.want, got, EquateItemCollections) {
				t.Errorf("Load() got = %s", cmp.Diff(tt.want, got, EquateItemCollections))
			}
		})
	}
}

func Test_New(t *testing.T) {
	dir := os.TempDir()

	conf := Config{
		Path:  dir,
		LogFn: func(s string, p ...interface{}) { t.Logf(s, p...) },
		ErrFn: func(s string, p ...interface{}) { t.Errorf(s, p...) },
	}
	repo, _ := New(conf)
	if repo == nil {
		t.Errorf("Nil result from opening boltdb %s", conf.Path)
		return
	}
	if repo.root != nil {
		t.Errorf("Non nil boltdb from New")
	}
	if repo.errFn == nil {
		t.Errorf("Nil error log function, expected %T[%p]", t.Errorf, t.Errorf)
	}
	if repo.logFn == nil {
		t.Errorf("Nil log function, expected %T[%p]", t.Logf, t.Logf)
	}
}

func TestRepo_Close(t *testing.T) {
	dir := os.TempDir()
	conf := Config{
		Path: dir,
	}
	path, _ := Path(conf)
	err := Bootstrap(conf)
	if err != nil {
		t.Errorf("Unable to bootstrap boltdb %s: %s", path, err)
	}
	defer os.Remove(path)

	repo, err := New(conf)
	if err != nil {
		t.Errorf("Error initializing db: %s", err)
	}
	err = repo.Open()
	if err != nil {
		t.Errorf("Unable to open boltdb %s: %s", path, err)
	}
	err = repo.close()
	if err != nil {
		t.Errorf("Unable to close boltdb %s: %s", path, err)
	}
	os.Remove(path)
}

func Test_repo_Save(t *testing.T) {
	type test struct {
		name     string
		fields   fields
		setupFns []initFn
		it       vocab.Item
		want     vocab.Item
		wantErr  error
	}
	tests := []test{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty item can't be saved",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.Newf("Unable to save nil element"),
		},
		{
			name:     "save item collection",
			setupFns: []initFn{withOpenRoot},
			fields:   fields{path: t.TempDir()},
			it:       mockItems,
			want:     mockItems,
		},
	}
	for i, mockIt := range mockItems {
		tests = append(tests, test{
			name:     fmt.Sprintf("save %d %T to repo", i, mockIt),
			setupFns: []initFn{withOpenRoot},
			fields:   fields{path: t.TempDir()},
			it:       mockIt,
			want:     mockIt,
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.Save(tt.it)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Save() error = %s", cmp.Diff(tt.wantErr, err))
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("Save() got = %s", cmp.Diff(tt.want, got))
			}
		})
	}
}
