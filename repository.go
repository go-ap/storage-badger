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
	root  *badger.DB
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

func badgerOpenConfig(path string, logFn, errFn loggerFn) badger.Options {
	c := badger.DefaultOptions(path)
	logger := logger{logFn: logFn, errFn: errFn}
	c = c.WithLogger(logger)
	if path == "" {
		c.InMemory = true
	}
	c.MetricsEnabled = false
	return c
}

// Open opens the badger database if possible.
func (r *repo) Open() error {
	if r == nil {
		return errors.Newf("Unable to open uninitialized db")
	}
	var err error
	r.root, err = badger.Open(badgerOpenConfig(r.path, r.logFn, r.errFn))
	if err != nil {
		err = errors.Annotatef(err, "unable to open storage")
	}
	return err
}

// Close closes the badger database if possible.
func (r *repo) close() error {
	if r == nil {
		return errors.Newf("Unable to close uninitialized db")
	}
	if r.root == nil {
		return nil
	}

	if err := r.root.Close(); err != nil {
		r.errFn("error closing the badger db: %+s", err)
	}

	return nil
}

func firstOrItem(it vocab.Item) vocab.Item {
	if vocab.IsNil(it) {
		return it
	}
	if vocab.IsItemCollection(it) {
		_ = vocab.OnItemCollection(it, func(col *vocab.ItemCollection) error {
			it = col.First()
			return nil
		})
	}
	return it
}

// Load
func (r *repo) Load(i vocab.IRI, checks ...filters.Check) (vocab.Item, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	var ret vocab.Item
	err := r.root.View(func(tx *badger.Txn) error {
		it, err := r.loadFromPath(tx, i, checks...)
		if err != nil {
			return err
		}
		ret = it
		return nil
	})
	if err != nil {
		return nil, err
	}
	return filters.Checks(checks).Run(firstOrItem(ret)), nil
}

var errNotOpen = errors.Newf("badger db is not open")

func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	if r.root == nil {
		return nil, errNotOpen
	}
	if vocab.IsIRI(col) {
		return col, errors.Errorf("invalid collection to save: %s", col)
	}

	it, err := save(r, col)
	if err != nil {
		return nil, err
	}

	var ok bool
	col, ok = it.(vocab.CollectionInterface)
	if !ok {
		return col, errors.Errorf("invalid collection saved: %s", col)
	}
	return col, nil
}

func onCollection(r *repo, col vocab.Item, fn func(iris vocab.IRIs) (vocab.IRIs, error)) error {
	if vocab.IsNil(col) {
		return errors.Newf("Unable to find collection")
	}
	p := itemPath(col.GetLink())

	return r.root.Update(func(tx *badger.Txn) error {
		var iris vocab.IRIs

		rawKey := getItemsKey(p)
		if i, err := tx.Get(rawKey); err == nil {
			err = i.Value(func(raw []byte) error {
				colItems, err := decodeItemFn(raw)
				if err != nil {
					return errors.Annotatef(err, "Unable to unmarshal collection %s", p)
				}

				err = vocab.OnIRIs(colItems, func(col *vocab.IRIs) error {
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
		return tx.Set(rawKey, raw)
	})
}

// Save
func (r *repo) Save(it vocab.Item) (vocab.Item, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	if vocab.IsNil(it) {
		return nil, errors.Newf("Unable to save nil element")
	}

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

// RemoveFrom
func (r *repo) RemoveFrom(col vocab.IRI, items ...vocab.Item) error {
	return onCollection(r, col, func(iris vocab.IRIs) (vocab.IRIs, error) {
		iris.Remove(items...)
		return iris, nil
	})
}

func isHiddenCollectionIRI(iri vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(iri.String()))
	return filters.HiddenCollections.Contains(lst)
}

func emptyCollection(colIRI vocab.IRI, owner vocab.Item) vocab.CollectionInterface {
	col := vocab.OrderedCollection{
		ID:        colIRI,
		Type:      vocab.OrderedCollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC().Truncate(time.Second),
	}
	if !vocab.IsNil(owner) {
		col.AttributedTo = owner.GetLink()
		_ = vocab.OnObject(owner, func(ob *vocab.Object) error {
			if !ob.Published.IsZero() {
				col.Published = ob.Published
			}
			return nil
		})
	}
	return &col
}

// AddTo
func (r *repo) AddTo(colIRI vocab.IRI, items ...vocab.Item) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	var col vocab.Item
	toWrite := make(vocab.ItemCollection, 0)
	err := r.root.View(func(tx *badger.Txn) error {
		maybeCol, err := r.loadOneFromPath(tx, colIRI)
		if err != nil && !isHiddenCollectionIRI(colIRI) {
			return err
		}
		col = maybeCol
		if col == nil && isHiddenCollectionIRI(colIRI) {
			// NOTE(marius): for hidden collections we try to create it automatically if it doesn't exist.
			// Here we assume the owner can be inferred from the collection IRI, but that's just a FedBOX implementation
			// detail. We should find a different way to pass collection owner - maybe the processing package checks for
			// existence of the blocked collection, and explicitly creates it if it doesn't.
			maybeOwner, _ := vocab.Split(colIRI)
			col = emptyCollection(colIRI, maybeOwner)
			_ = toWrite.Append(col)
		}
		for _, it := range items {
			_, err = r.loadItem(tx, getObjectKey(itemPath(it.GetLink())))
			if err != nil && errors.IsNotFound(err) && !vocab.IsIRI(it) {
				_ = toWrite.Append(it)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	wb := r.root.NewWriteBatch()
	for _, it := range toWrite {
		if err := writeFromPath(wb, it); err != nil {
			return err
		}
	}
	if err := wb.Flush(); err != nil {
		return err
	}

	return onCollection(r, col, func(iris vocab.IRIs) (vocab.IRIs, error) {
		return iris, iris.Append(items...)
	})
}

// Delete
func (r *repo) Delete(it vocab.Item) error {
	return delete(r, it)
}

const objectKey = "__raw"
const itemsKey = "__items"

func delete(r *repo, it vocab.Item) error {
	var old vocab.Item
	err := r.root.View(func(tx *badger.Txn) error {
		ob, err := r.loadOneFromPath(tx, it.GetLink())
		if err != nil {
			return err
		}
		old = ob
		return nil
	})

	tx := r.root.NewWriteBatch()
	if err = deleteFromTx(tx, old); err != nil {
		return err
	}
	return tx.Flush()
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

func save(r *repo, it vocab.Item) (vocab.Item, error) {
	err := r.root.Update(func(txn *badger.Txn) error {
		return saveRawItem(txn, it)
	})

	return it, err
}

var collectionTypes = vocab.ActivityVocabularyTypes{
	vocab.CollectionType,
	vocab.OrderedCollectionType,
	vocab.CollectionPageType,
	vocab.OrderedCollectionPageType,
}

var emptyJsonCollection = []byte{'[', ']'}

func saveRawItem(txn *badger.Txn, it vocab.Item) error {
	entryBytes, err := encodeItemFn(it)
	if err != nil {
		return errors.Annotatef(err, "could not marshal object")
	}
	rawKey := getObjectKey(itemPath(it.GetLink()))
	exists := false
	if _, err := txn.Get(rawKey); err == nil {
		exists = true
	}

	if err = txn.Set(rawKey, entryBytes); err != nil {
		return errors.Annotatef(err, "could not store encoded object")
	}

	if !exists {
		if err = createCollections(txn, it); err != nil {
			return errors.Annotatef(err, "could not create object's collections")
		}
		if collectionTypes.Contains(it.GetType()) {
			colItemsKey := getItemsKey(itemPath(it.GetLink()))
			if err = txn.Set(colItemsKey, emptyJsonCollection); err != nil {
				return err
			}
		}
	}

	return nil
}

func createCollectionInPath(txn *badger.Txn, it vocab.Item, owner vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}

	if vocab.IsIRI(it) {
		it = emptyCollection(it.GetLink(), owner)
	}

	if err := saveRawItem(txn, it); err != nil {
		return nil, err
	}
	rawKey := getItemsKey(itemPath(it.GetLink()))
	if err := txn.Set(rawKey, emptyJsonCollection); err != nil {
		return nil, err
	}
	return it.GetLink(), nil
}

func writeFromPath(tx *badger.WriteBatch, it vocab.Item) error {
	if vocab.IsNil(it) {
		return nil
	}
	raw, err := encodeFn(it)
	if err != nil {
		return err
	}
	rawKey := getObjectKey(itemPath(it.GetLink()))
	if err := tx.Set(rawKey, raw); err != nil {
		return err
	}
	return nil
}

func deleteFromTx(tx *badger.WriteBatch, it vocab.Item) error {
	if vocab.IsNil(it) {
		return nil
	}
	p := getObjectKey(itemPath(it.GetLink()))
	if err := tx.Delete(p); err != nil {
		return err
	}
	return nil
}

func (r *repo) loadFromItem(tx *badger.Txn, into *vocab.ItemCollection, iri vocab.IRI, f ...filters.Check) func(val []byte) error {
	return func(val []byte) error {
		it, err := loadItem(val)
		if err != nil || vocab.IsNil(it) {
			return errors.NewNotFound(err, "not found")
		}
		if !it.IsObject() && it.IsLink() {
			c, err := r.loadItemsByIRIs(tx, iri, it.GetLink())
			if err != nil {
				return err
			}
			for _, it := range c {
				if into.Contains(it.GetLink()) {
					continue
				}
				*into = append(*into, it)
			}
		} else if it.IsCollection() {
			return vocab.OnOrderedCollection(it, func(ci *vocab.OrderedCollection) error {
				c, err := r.loadCollectionItems(tx, ci.ID)
				if err != nil {
					return err
				}
				ci.ID = iri
				if len(c) > 0 {
					ci.OrderedItems = c
					ci.TotalItems = uint(len(c))
				}
				*into = append(*into, ci)
				return nil
			})
		} else {
			if !vocab.IsNil(it) {
				if vocab.ActorTypes.Contains(it.GetType()) {
					_ = vocab.OnActor(it, loadFilteredPropsForActor(r, tx, f...))
				}
				if vocab.ObjectTypes.Contains(it.GetType()) {
					_ = vocab.OnObject(it, loadFilteredPropsForObject(r, tx, f...))
				}
				if vocab.IntransitiveActivityTypes.Contains(it.GetType()) {
					_ = vocab.OnIntransitiveActivity(it, loadFilteredPropsForIntransitiveActivity(r, tx, f...))
				}
				if vocab.ActivityTypes.Contains(it.GetType()) {
					_ = vocab.OnActivity(it, loadFilteredPropsForActivity(r, tx, f...))
				}
				if !into.Contains(it.GetLink()) {
					*into = append(*into, it)
				}
			}
		}
		return nil
	}
}

func loadFilteredPropsForActor(r *repo, tx *badger.Txn, f ...filters.Check) func(a *vocab.Actor) error {
	return func(a *vocab.Actor) error {
		return vocab.OnObject(a, loadFilteredPropsForObject(r, tx, f...))
	}
}

func loadFilteredPropsForObject(r *repo, tx *badger.Txn, _ ...filters.Check) func(o *vocab.Object) error {
	return func(o *vocab.Object) error {
		if len(o.Tag) == 0 {
			return nil
		}
		return vocab.OnItemCollection(o.Tag, func(col *vocab.ItemCollection) error {
			for i, t := range *col {
				if vocab.IsNil(t) || !vocab.IsIRI(t) {
					return nil
				}
				if ob, err := r.loadOneFromPath(tx, t.GetLink()); err == nil {
					(*col)[i] = ob
				}
			}
			return nil
		})
	}
}

func loadFilteredPropsForActivity(r *repo, tx *badger.Txn, f ...filters.Check) func(a *vocab.Activity) error {
	return func(a *vocab.Activity) error {
		if len(filters.ObjectChecks(f...)) > 0 {
			if ob, err := r.loadOneFromPath(tx, a.Object.GetLink()); err == nil {
				a.Object = ob
			}
		}
		return vocab.OnIntransitiveActivity(a, loadFilteredPropsForIntransitiveActivity(r, tx, f...))
	}
}

func loadFilteredPropsForIntransitiveActivity(r *repo, tx *badger.Txn, f ...filters.Check) func(a *vocab.IntransitiveActivity) error {
	return func(a *vocab.IntransitiveActivity) error {
		if len(filters.ActorChecks(f...)) > 0 {
			if act, err := r.loadOneFromPath(tx, a.Actor.GetLink()); err == nil {
				a.Actor = act
			}
		}
		if len(filters.TargetChecks(f...)) > 0 {
			if t, err := r.loadOneFromPath(tx, a.Target.GetLink()); err == nil {
				a.Target = t
			}
		}
		return nil
	}
}

var sep = []byte{'/'}

func isObjectKey(k []byte) bool {
	return bytes.HasSuffix(k, []byte(objectKey))
}

func isItemsKey(k []byte) bool {
	return bytes.HasSuffix(k, []byte(itemsKey))
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

func (r *repo) loadFromPath(tx *badger.Txn, iri vocab.IRI, checks ...filters.Check) (vocab.ItemCollection, error) {
	col := make(vocab.ItemCollection, 0)

	fullPath := itemPath(iri)
	k := getObjectKey(fullPath)

	i, err := tx.Get(k)
	if err != nil {
		return nil, errors.NotFoundf("unable to load item %s: %+s", fullPath, err)
	}

	if err = i.Value(r.loadFromItem(tx, &col, iri, checks...)); err != nil {
		r.errFn("unable to load item %s: %+s", k, err)
		return nil, err
	}

	return col, nil
}

func (r *repo) loadOneFromPath(tx *badger.Txn, f vocab.IRI, filters ...filters.Check) (vocab.Item, error) {
	col, err := r.loadFromPath(tx, f, filters...)
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

func getItemsKey(p []byte) []byte {
	return bytes.Join([][]byte{p, []byte(itemsKey)}, sep)
}

func (r *repo) loadItemsByIRIs(tx *badger.Txn, iris ...vocab.Item) (vocab.ItemCollection, error) {
	col := make(vocab.ItemCollection, 0)
	for _, iri := range iris {
		it, err := r.loadItem(tx, itemPath(iri.GetLink()))
		if err != nil || vocab.IsNil(it) || col.Contains(it.GetLink()) {
			continue
		}
		_ = col.Append(it)
	}
	return col, nil
}

func (r *repo) loadCollectionItems(tx *badger.Txn, colIRI vocab.IRI) (vocab.ItemCollection, error) {
	col := make(vocab.ItemCollection, 0)
	path := itemPath(colIRI)

	if isStorageCollectionKey(path) {
		depth := 1
		if vocab.ValidCollectionIRI(colIRI) {
			depth = 2
		}
		opt := badger.DefaultIteratorOptions
		opt.Prefix = path
		it := tx.NewIterator(opt)
		defer it.Close()
		pathExists := false
		for it.Seek(path); it.ValidForPrefix(path); it.Next() {
			i := it.Item()
			k := i.Key()
			pathExists = true
			if iterKeyIsTooDeep(path, k, depth) || (isStorageCollectionKey([]byte(filepath.Dir(string(k)))) && (isObjectKey(k) || isItemsKey(k))) {
				continue
			}

			if isObjectKey(k) {
				if err := i.Value(r.loadFromItem(tx, &col, "", nil)); err != nil {
					r.errFn("unable to load item %s: %+s", k, err)
					continue
				}
			}
		}
		if !pathExists && len(col) == 0 {
			return nil, errors.NotFoundf("%s does not exist", path)
		}
	} else {
		rawKey := getItemsKey(path)
		i, err := tx.Get(rawKey)
		if err != nil {
			return nil, errors.NewNotFound(err, "Unable to load path %s", path)
		}
		err = i.Value(func(val []byte) error {
			it, err := decodeItemFn(val)
			if err != nil {
				return err
			}
			cc, err := vocab.ToItemCollection(it)
			if err != nil {
				return err
			}
			col = *cc
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if isStorageCollectionKey(path) {
		return col, nil
	}

	return r.loadItemsByIRIs(tx, col...)
}

func (r *repo) loadItem(tx *badger.Txn, path []byte) (vocab.Item, error) {
	i, err := tx.Get(getObjectKey(path))
	if err != nil {
		return nil, errors.NewNotFound(err, "Unable to load path %s", path)
	}
	var raw []byte
	_ = i.Value(func(val []byte) error {
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
	if err := mkDirIfNotExists(c.Path); err != nil {
		return c.Path, err
	}
	_, err := os.Stat(c.Path)
	return c.Path, err
}

const defaultPerm = os.ModeDir | os.ModePerm | 0x700

func mkDirIfNotExists(p string) error {
	if p != "" && !filepath.IsAbs(p) {
		p, _ = filepath.Abs(p)
	}
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
