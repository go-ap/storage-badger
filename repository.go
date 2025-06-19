package badger

import (
	"bytes"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
)

type repo struct {
	d     *badger.DB
	path  string
	cache cache.CanStore
	logFn loggerFn
	errFn loggerFn
}

var encodeItemFn = vocab.MarshalJSON
var decodeItemFn = vocab.UnmarshalJSON

type loggerFn func(string, ...interface{})

// Config
type Config struct {
	Path        string
	CacheEnable bool
	LogFn       loggerFn
	ErrFn       loggerFn
}

var emptyLogFn = func(string, ...interface{}) {}

type Filterable = vocab.LinkOrIRI

// New returns a new repo repository
func New(c Config) (*repo, error) {
	var err error
	c.Path, err = Path(c)
	if err != nil {
		return nil, err
	}
	b := repo{
		path:  c.Path,
		logFn: emptyLogFn,
		errFn: emptyLogFn,
	}
	if c.LogFn != nil {
		b.logFn = c.LogFn
	}
	if c.ErrFn != nil {
		b.errFn = c.ErrFn
	}
	return &b, nil
}

// Open opens the badger database if possible.
func (r *repo) Open() error {
	c := badger.DefaultOptions(r.path)
	logger := logger{logFn: r.logFn, errFn: r.errFn}
	c = c.WithLogger(logger)
	if r.path == "" {
		c.InMemory = true
	}
	c.MetricsEnabled = false

	var err error
	r.d, err = badger.Open(c)
	if err != nil {
		err = errors.Annotatef(err, "unable to open storage")
	}
	return err
}

// Close closes the badger database if possible.
func (r *repo) close() error {
	if r.d == nil {
		return nil
	}
	return r.d.Close()
}

// Load
func (r *repo) Load(i vocab.IRI, checks ...filters.Check) (vocab.Item, error) {
	ret, err := r.loadFromPath(i, checks...)
	return filters.Checks(checks).Run(ret), err
}

func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	if vocab.IsIRI(col) {
		return col, errors.Errorf("invalid collection to save: %s", col)
	}

	err := r.d.Update(func(txn *badger.Txn) error {
		return saveRawItem(txn, col)
	})
	return col, err
}

// Save
func (r *repo) Save(it vocab.Item) (vocab.Item, error) {
	var err error

	if it, err = save(r, it); err == nil {
		op := "Updated"
		id := it.GetID()
		if !id.IsValid() {
			op = "Added new"
		}
		r.logFn("%s %s: %s", op, it.GetType(), it.GetLink())
	}

	return it, err
}

func onCollection(r *repo, col vocab.IRI, it vocab.Item, fn func(iris vocab.IRIs) (vocab.IRIs, error)) error {
	if vocab.IsNil(it) {
		return errors.Newf("Unable to operate on nil element")
	}
	if len(col) == 0 {
		return errors.Newf("Unable to find collection")
	}
	if len(it.GetLink()) == 0 {
		return errors.Newf("Invalid collection, it does not have a valid IRI")
	}
	p := itemPath(col)

	return r.d.Update(func(tx *badger.Txn) error {
		var iris vocab.IRIs

		rawKey := getObjectKey(p)
		if i, err := tx.Get(rawKey); err == nil {
			err = i.Value(func(raw []byte) error {
				it, err := decodeItemFn(raw)
				if err != nil {
					return errors.Annotatef(err, "Unable to unmarshal collection %s", p)
				}
				err = vocab.OnIRIs(it, func(col *vocab.IRIs) error {
					iris = *col
					return nil
				})
				if err != nil {
					return errors.Annotatef(err, "Unable to unmarshal to IRI collection %s", p)
				}
				return nil
			})
		}
		var err error
		iris, err = fn(iris)
		if err != nil {
			return errors.Annotatef(err, "Unable operate on collection %s", p)
		}
		var raw []byte
		raw, err = encodeItemFn(iris)
		if err != nil {
			return errors.Newf("Unable to marshal entries in collection %s", p)
		}
		err = tx.Set(rawKey, raw)
		if err != nil {
			return errors.Annotatef(err, "Unable to save entries to collection %s", p)
		}
		return err
	})
}

// RemoveFrom
func (r *repo) RemoveFrom(col vocab.IRI, it vocab.Item) error {
	return onCollection(r, col, it, func(iris vocab.IRIs) (vocab.IRIs, error) {
		for k, iri := range iris {
			if iri.GetLink().Equals(it.GetLink(), false) {
				iris = append(iris[:k], iris[k+1:]...)
				break
			}
		}
		return iris, nil
	})
}

func addCollectionOnObject(r *repo, col vocab.IRI) error {
	allStorageCollections := append(vocab.ActivityPubCollections, filters.FedBOXCollections...)
	if ob, t := allStorageCollections.Split(col); vocab.ValidCollection(t) {
		// Create the collection on the object, if it doesn't exist
		if i, _ := r.loadOneFromPath(ob); i != nil {
			if _, ok := t.AddTo(i); ok {
				_, err := save(r, i)
				return err
			}
		}
	}
	return nil
}

// AddTo
func (r *repo) AddTo(col vocab.IRI, it vocab.Item) error {
	_ = addCollectionOnObject(r, col)
	return onCollection(r, col, it, func(iris vocab.IRIs) (vocab.IRIs, error) {
		if iris.Contains(it.GetLink()) {
			return iris, nil
		}
		return append(iris, it.GetLink()), nil
	})
}

// Delete
func (r *repo) Delete(it vocab.Item) error {
	return delete(r, it)
}

const objectKey = "__raw"

func delete(r *repo, it vocab.Item) error {
	if it.IsCollection() {
		return vocab.OnCollectionIntf(it, func(c vocab.CollectionInterface) error {
			for _, it := range c.Collection() {
				if err := delete(r, it); err != nil {
					r.errFn("Unable to remove item %s: %+s", it.GetLink(), err)
				}
			}
			return nil
		})
	}

	old, err := r.loadOneFromPath(it.GetLink(), filters.HasType(it.GetType()))
	if err != nil {
		return err
	}

	db := r.d.NewWriteBatch()
	return deleteFromPath(r, db, old)
}

// createCollections
func createCollections(tx *badger.Txn, it vocab.Item) error {
	if vocab.IsNil(it) || !it.IsObject() {
		return nil
	}
	if vocab.ActorTypes.Contains(it.GetType()) {
		_ = vocab.OnActor(it, func(p *vocab.Actor) error {
			if p.Inbox != nil {
				p.Inbox, _ = createCollectionInPath(tx, p.Inbox, p)
			}
			if p.Outbox != nil {
				p.Outbox, _ = createCollectionInPath(tx, p.Outbox, p)
			}
			if p.Followers != nil {
				p.Followers, _ = createCollectionInPath(tx, p.Followers, p)
			}
			if p.Following != nil {
				p.Following, _ = createCollectionInPath(tx, p.Following, p)
			}
			if p.Liked != nil {
				p.Liked, _ = createCollectionInPath(tx, p.Liked, p)
			}
			return nil
		})
	}
	return vocab.OnObject(it, func(o *vocab.Object) error {
		if o.Replies != nil {
			o.Replies, _ = createCollectionInPath(tx, o.Replies, o)
		}
		if o.Likes != nil {
			o.Likes, _ = createCollectionInPath(tx, o.Likes, o)
		}
		if o.Shares != nil {
			o.Shares, _ = createCollectionInPath(tx, o.Shares, o)
		}
		return nil
	})
}

// deleteCollections
func deleteCollections(r *repo, it vocab.Item) error {
	tx := r.d.NewWriteBatch()
	if vocab.ActorTypes.Contains(it.GetType()) {
		return vocab.OnActor(it, func(p *vocab.Actor) error {
			var err error
			err = deleteFromPath(r, tx, vocab.Inbox.IRI(p))
			err = deleteFromPath(r, tx, vocab.Outbox.IRI(p))
			err = deleteFromPath(r, tx, vocab.Followers.IRI(p))
			err = deleteFromPath(r, tx, vocab.Following.IRI(p))
			err = deleteFromPath(r, tx, vocab.Liked.IRI(p))
			return err
		})
	}
	if vocab.ObjectTypes.Contains(it.GetType()) {
		return vocab.OnObject(it, func(o *vocab.Object) error {
			var err error
			err = deleteFromPath(r, tx, vocab.Replies.IRI(o))
			err = deleteFromPath(r, tx, vocab.Likes.IRI(o))
			err = deleteFromPath(r, tx, vocab.Shares.IRI(o))
			return err
		})
	}
	return nil
}

func save(r *repo, it vocab.Item) (vocab.Item, error) {
	itPath := itemPath(it.GetLink())

	err := r.d.Update(func(txn *badger.Txn) error {
		if err := createCollections(txn, it); err != nil {
			return errors.Annotatef(err, "could not create object's collections")
		}

		entryBytes, err := encodeItemFn(it)
		if err != nil {
			return errors.Annotatef(err, "could not marshal object")
		}

		k := getObjectKey(itPath)
		if err = txn.Set(k, entryBytes); err != nil {
			return errors.Annotatef(err, "could not store encoded object")
		}
		return nil
	})

	return it, err
}

func rawCollection(colIRI vocab.IRI, owner vocab.Item) vocab.OrderedCollection {
	col := vocab.OrderedCollection{
		ID:        colIRI,
		Type:      vocab.OrderedCollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC(),
	}
	if !vocab.IsNil(owner) {
		col.AttributedTo = owner.GetLink()
	}
	return col
}

func saveRawItem(txn *badger.Txn, it vocab.Item) error {
	entryBytes, err := encodeItemFn(it)
	if err != nil {
		return errors.Annotatef(err, "could not marshal object")
	}
	p := getObjectKey(itemPath(it.GetLink()))
	if err = txn.Set(p, entryBytes); err != nil {
		return errors.Annotatef(err, "could not store encoded object")
	}

	return nil
}

func createCollectionInPath(txn *badger.Txn, it vocab.Item, owner vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}

	if vocab.IsIRI(it) {
		it = rawCollection(it.GetLink(), owner)
	}
	err := saveRawItem(txn, it)
	return it.GetLink(), err
}

func deleteFromPath(r *repo, b *badger.WriteBatch, it vocab.Item) error {
	if vocab.IsNil(it) {
		return nil
	}
	p := getObjectKey(itemPath(it.GetLink()))
	if err := b.Delete(p); err != nil {
		return err
	}
	return nil
}

func (r *repo) loadFromIterator(col *vocab.ItemCollection, f Filterable) func(val []byte) error {
	isColFn := func(ff Filterable) bool {
		_, ok := ff.(vocab.IRI)
		return ok
	}
	return func(val []byte) error {
		it, err := loadItem(val)
		if err != nil || vocab.IsNil(it) {
			return errors.NewNotFound(err, "not found")
		}
		if !it.IsObject() && it.IsLink() {
			c, err := r.loadItemsElements(f, it.GetLink())
			if err != nil {
				return err
			}
			for _, it := range c {
				if col.Contains(it.GetLink()) {
					continue
				}
				*col = append(*col, it)
			}
		} else if it.IsCollection() {
			return vocab.OnCollectionIntf(it, func(ci vocab.CollectionInterface) error {
				if isColFn(f) {
					f = ci.Collection()
				}
				c, err := r.loadItemsElements(f, ci.Collection()...)
				if err != nil {
					return err
				}
				for _, it := range c {
					if col.Contains(it.GetLink()) {
						continue
					}
					*col = append(*col, it)
				}
				return nil
			})
		} else {
			if it.GetType() == vocab.CreateType {
				// TODO(marius): this seems terribly not nice
				_ = vocab.OnActivity(it, func(a *vocab.Activity) error {
					if !a.Object.IsObject() {
						ob, _ := r.loadOneFromPath(a.Object.GetLink())
						a.Object = ob
					}
					return nil
				})
			}

			it, err = filters.FilterIt(it, f)
			if err != nil {
				return err
			}
			if it != nil {
				if vocab.ActorTypes.Contains(it.GetType()) {
					_ = vocab.OnActor(it, loadFilteredPropsForActor(r, f))
				}
				if vocab.ObjectTypes.Contains(it.GetType()) {
					_ = vocab.OnObject(it, loadFilteredPropsForObject(r, f))
				}
				if vocab.IntransitiveActivityTypes.Contains(it.GetType()) {
					_ = vocab.OnIntransitiveActivity(it, loadFilteredPropsForIntransitiveActivity(r, f))
				}
				if vocab.ActivityTypes.Contains(it.GetType()) {
					_ = vocab.OnActivity(it, loadFilteredPropsForActivity(r, f))
				}
				if !col.Contains(it.GetLink()) {
					*col = append(*col, it)
				}
			}
		}
		return nil
	}
}

func loadFilteredPropsForActor(r *repo, f Filterable) func(a *vocab.Actor) error {
	return func(a *vocab.Actor) error {
		return vocab.OnObject(a, loadFilteredPropsForObject(r, f))
	}
}

func loadFilteredPropsForObject(r *repo, f Filterable) func(o *vocab.Object) error {
	return func(o *vocab.Object) error {
		if len(o.Tag) == 0 {
			return nil
		}
		return vocab.OnItemCollection(o.Tag, func(col *vocab.ItemCollection) error {
			for i, t := range *col {
				if vocab.IsNil(t) || !vocab.IsIRI(t) {
					return nil
				}
				if ob, err := r.loadOneFromPath(t.GetLink()); err == nil {
					(*col)[i] = ob
				}
			}
			return nil
		})
	}
}
func loadFilteredPropsForActivity(r *repo, f Filterable) func(a *vocab.Activity) error {
	return func(a *vocab.Activity) error {
		if ok, fo := filters.FiltersOnActivityObject(f); ok && !vocab.IsNil(a.Object) && vocab.IsIRI(a.Object) {
			if ob, err := r.loadOneFromPath(a.Object.GetLink()); err == nil {
				if ob, _ = filters.FilterIt(ob, fo); ob != nil {
					a.Object = ob
				}
			}
		}
		return vocab.OnIntransitiveActivity(a, loadFilteredPropsForIntransitiveActivity(r, f))
	}
}

func loadFilteredPropsForIntransitiveActivity(r *repo, f Filterable) func(a *vocab.IntransitiveActivity) error {
	return func(a *vocab.IntransitiveActivity) error {
		if ok, fa := filters.FiltersOnActivityActor(f); ok && !vocab.IsNil(a.Actor) && vocab.IsIRI(a.Actor) {
			if act, err := r.loadOneFromPath(a.Actor.GetLink()); err == nil {
				if act, _ = filters.FilterIt(act, fa); act != nil {
					a.Actor = act
				}
			}
		}
		if ok, ft := filters.FiltersOnActivityTarget(f); ok && !vocab.IsNil(a.Target) && vocab.IsIRI(a.Target) {
			if t, err := r.loadOneFromPath(a.Target.GetLink()); err == nil {
				if t, _ = filters.FilterIt(t, ft); t != nil {
					a.Target = t
				}
			}
		}
		return nil
	}
}

var sep = []byte{'/'}

func isObjectKey(k []byte) bool {
	return bytes.HasSuffix(k, []byte(objectKey))
}

func isStorageCollectionKey(p []byte) bool {
	lst := vocab.CollectionPath(filepath.Base(string(p)))
	return vocab.CollectionPaths{filters.ActivitiesType, filters.ActorsType, filters.ObjectsType}.Contains(lst)
}

func iterKeyIsTooDeep(base, k []byte, depth int) bool {
	res := bytes.TrimPrefix(k, append(base, sep...))
	res = bytes.TrimSuffix(res, []byte(objectKey))
	cnt := bytes.Count(res, sep)
	return cnt > depth
}

func (r *repo) loadFromPath(iri vocab.IRI, checks ...filters.Check) (vocab.ItemCollection, error) {
	col := make(vocab.ItemCollection, 0)

	err := r.d.View(func(tx *badger.Txn) error {
		fullPath := itemPath(iri)
		k := getObjectKey(fullPath)

		i, err := tx.Get(fullPath)
		if err != nil {
			return errors.NotFoundf("unable to load item %s: %+s", fullPath, err)
		}

		if err = i.Value(r.loadFromIterator(&col, iri)); err != nil {
			r.errFn("unable to load item %s: %+s", k, err)
			return err
		}
		return nil
	})

	return col, err
}

func (r *repo) loadOneFromPath(f vocab.IRI, filters ...filters.Check) (vocab.Item, error) {
	col, err := r.loadFromPath(f, filters...)
	if err != nil {
		return nil, err
	}
	if len(col) == 0 {
		return nil, errors.NotFoundf("nothing found")
	}
	return col.First(), nil
}

func getObjectKey(p []byte) []byte {
	return bytes.Join([][]byte{p, []byte(objectKey)}, sep)
}

func (r *repo) loadItemsElements(f Filterable, iris ...vocab.Item) (vocab.ItemCollection, error) {
	col := make(vocab.ItemCollection, 0)
	err := r.d.View(func(tx *badger.Txn) error {
		for _, iri := range iris {
			it, err := r.loadItem(tx, itemPath(iri.GetLink()), f)
			if err != nil || vocab.IsNil(it) || col.Contains(it.GetLink()) {
				continue
			}
			col = append(col, it)
		}
		return nil
	})
	return col, err
}

func (r *repo) loadItem(b *badger.Txn, path []byte, f Filterable) (vocab.Item, error) {
	i, err := b.Get(getObjectKey(path))
	if err != nil {
		return nil, errors.NewNotFound(err, "Unable to load path %s", path)
	}
	var raw []byte
	i.Value(func(val []byte) error {
		raw = val
		return nil
	})
	if raw == nil {
		return nil, nil
	}
	var it vocab.Item
	it, err = loadItem(raw)
	if err != nil {
		return nil, err
	}
	if vocab.IsNil(it) {
		return nil, errors.NotFoundf("not found")
	}
	if it.IsCollection() {
		// we need to dereference them, so no further filtering/processing is needed here
		return it, nil
	}
	if vocab.IsIRI(it) {
		it, _ = r.loadOneFromPath(it.GetLink())
	}
	if f != nil {
		return filters.FilterIt(it, f)
	}
	return it, nil
}

func loadItem(raw []byte) (vocab.Item, error) {
	if raw == nil || len(raw) == 0 {
		// TODO(marius): log this instead of stopping the iteration and returning an error
		return nil, errors.Errorf("empty raw item")
	}
	return decodeItemFn(raw)
}

func itemPath(iri vocab.IRI) []byte {
	url, err := iri.URL()
	if err != nil {
		return nil
	}
	return []byte(filepath.Join(url.Host, url.Path))
}

func Path(c Config) (string, error) {
	if c.Path == "" {
		return "", nil
	}
	return c.Path, mkDirIfNotExists(c.Path)
}

func mkDirIfNotExists(p string) error {
	const defaultPerm = os.ModeDir | os.ModePerm | 0700
	p, _ = filepath.Abs(p)
	if fi, err := os.Stat(p); err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(p, defaultPerm); err != nil {
				return err
			}
		}
	} else if !fi.IsDir() {
		return errors.Errorf("path exists, and is not a folder %s", p)
	}
	return nil
}
