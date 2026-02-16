package badger

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"time"

	"github.com/dgraph-io/badger/v4"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/openshift/osin"
)

const (
	clientsBucket   = "clients"
	authorizeBucket = "authorize"
	accessBucket    = "access"
	refreshBucket   = "refresh"
	folder          = "oauth"
)

type cl struct {
	Id          string
	Secret      string
	RedirectUri string
	Extra       interface{}
}

type auth struct {
	Client              string
	Code                string
	ExpiresIn           time.Duration
	Scope               string
	RedirectURI         string
	State               string
	CreatedAt           time.Time
	Extra               interface{}
	CodeChallengeMethod string
	CodeChallenge       string
}

type acc struct {
	Client       string
	Authorize    string
	AccessToken  string
	RefreshToken string
	ExpiresIn    time.Duration
	Scope        string
	RedirectURI  string
	CreatedAt    time.Time
	Extra        interface{}
}

type ref struct {
	Access string
}

var encodeFn = func(v any) ([]byte, error) {
	buf := bytes.Buffer{}
	err := json.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

var decodeFn = func(data []byte, m any) error {
	return json.NewDecoder(bytes.NewReader(data)).Decode(m)
}

func interfaceIsNil(c interface{}) bool {
	return reflect.ValueOf(c).Kind() == reflect.Ptr && reflect.ValueOf(c).IsNil()
}

// Close closes the badger database if possible.
func (r *repo) Close() {
	_ = r.close()
}

// Clone
func (r *repo) Clone() osin.Storage {
	return r
}

func badgerItemPath(pieces ...string) []byte {
	// Open opens the badger database if possible.
	pieces = append([]string{folder}, pieces...)
	return []byte(filepath.Join(pieces...))
}

func (r *repo) clientPath(id string) []byte {
	return badgerItemPath(clientsBucket, id)
}

func (r *repo) loadTxnClient(c *osin.DefaultClient, id string) func(tx *badger.Txn) error {
	fullPath := r.clientPath(id)
	return func(tx *badger.Txn) error {
		it, err := tx.Get(fullPath)
		if err != nil {
			return errors.NewNotFound(err, "Invalid path %s", fullPath)
		}
		return it.Value(loadRawClient(c))
	}
}

func loadRawClient(c *osin.DefaultClient) func(raw []byte) error {
	return func(raw []byte) error {
		cl := cl{}
		if err := decodeFn(raw, &cl); err != nil {
			return errors.Annotatef(err, "Unable to unmarshal client object")
		}
		c.Id = cl.Id
		c.Secret = cl.Secret
		c.RedirectUri = cl.RedirectUri
		c.UserData = cl.Extra
		return nil
	}
}

// GetClient
func (r *repo) GetClient(id string) (osin.Client, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	if id == "" {
		return nil, errors.NotFoundf("Empty client id")
	}

	c := new(osin.DefaultClient)
	if err := r.root.View(r.loadTxnClient(c, id)); err != nil {
		return nil, err
	}
	return c, nil
}

func (r *repo) ListClients() ([]osin.Client, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	clients := make([]osin.Client, 0)
	err := r.root.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = badgerItemPath(clientsBucket)
		it := tx.NewIterator(opts)
		defer it.Close()

		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			item := it.Item()

			c := osin.DefaultClient{}
			err := item.Value(loadRawClient(&c))
			if err != nil {
				return err
			}

			clients = append(clients, &c)
		}
		return nil
	})
	return clients, err
}

// UpdateClient updates the client (identified by its id) and replaces the values with the values of client.
func (r *repo) UpdateClient(c osin.Client) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	if interfaceIsNil(c) {
		return nil
	}
	cl := cl{
		Id:          c.GetId(),
		Secret:      c.GetSecret(),
		RedirectUri: c.GetRedirectUri(),
		Extra:       c.GetUserData(),
	}
	raw, err := encodeFn(cl)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal client object")
	}
	tx := r.root.NewWriteBatch()
	err = tx.Set(r.clientPath(c.GetId()), raw)
	if err != nil {
		return err
	}
	return tx.Flush()
}

// CreateClient stores the client in the database and returns an error, if something went wrong.
func (r *repo) CreateClient(c osin.Client) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	return r.UpdateClient(c)
}

// RemoveClient removes a client (identified by id) from the database. Returns an error if something went wrong.
func (r *repo) RemoveClient(id string) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	tx := r.root.NewWriteBatch()
	if err := tx.Delete(r.clientPath(id)); err != nil {
		return err
	}
	return tx.Flush()
}

func (r *repo) authorizePath(code string) []byte {
	return badgerItemPath(authorizeBucket, code)
}

// SaveAuthorize
func (r *repo) SaveAuthorize(data *osin.AuthorizeData) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	if data == nil {
		return errors.Newf("unable to save nil authorization data")
	}
	auth := auth{
		Client:              data.Client.GetId(),
		Code:                data.Code,
		ExpiresIn:           time.Duration(data.ExpiresIn),
		Scope:               data.Scope,
		RedirectURI:         data.RedirectUri,
		State:               data.State,
		CreatedAt:           data.CreatedAt.UTC(),
		Extra:               data.UserData,
		CodeChallenge:       data.CodeChallenge,
		CodeChallengeMethod: data.CodeChallengeMethod,
	}
	raw, err := encodeFn(auth)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal authorization object")
	}
	tx := r.root.NewWriteBatch()
	if err = tx.Set(r.authorizePath(data.Code), raw); err != nil {
		return err
	}
	return tx.Flush()
}

func (r *repo) loadTxnAuthorize(a *osin.AuthorizeData, code string) func(tx *badger.Txn) error {
	fullPath := r.authorizePath(code)
	return func(tx *badger.Txn) error {
		it, err := tx.Get(fullPath)
		if err != nil {
			return errors.NotFoundf("Invalid path %s", fullPath)
		}
		if err := it.Value(loadRawAuthorize(a)); err != nil {
			return err
		}
		if a.Client != nil {
			id := a.Client.GetId()
			client, _ := a.Client.(*osin.DefaultClient)
			if err = r.loadTxnClient(client, id)(tx); err != nil {
				return err
			}
			a.Client = client
		}
		return nil
	}
}

func loadRawAuthorize(a *osin.AuthorizeData) func(raw []byte) error {
	return func(raw []byte) error {
		auth := auth{}
		if err := decodeFn(raw, &auth); err != nil {
			return errors.Annotatef(err, "Unable to unmarshal authorize object")
		}
		a.Code = auth.Code
		a.ExpiresIn = int32(auth.ExpiresIn)
		a.Scope = auth.Scope
		a.RedirectUri = auth.RedirectURI
		a.State = auth.State
		a.CreatedAt = auth.CreatedAt
		a.CodeChallenge = auth.CodeChallenge
		a.CodeChallengeMethod = auth.CodeChallengeMethod
		if len(auth.Code) > 0 {
			a.Client = &osin.DefaultClient{Id: auth.Client}
		}
		if a.ExpireAt().Before(time.Now().UTC()) {
			return errors.Errorf("Token expired at %s.", a.ExpireAt().String())
		}
		if userData, ok := auth.Extra.(string); ok {
			a.UserData = vocab.IRI(userData)
		}
		return nil
	}
}

// LoadAuthorize
func (r *repo) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	if code == "" {
		return nil, errors.NotFoundf("Empty authorize code")
	}
	data := osin.AuthorizeData{}

	err := r.root.View(r.loadTxnAuthorize(&data, code))
	if err != nil {
		return nil, err
	}
	if data.Client != nil {
	}
	return &data, err
}

// RemoveAuthorize
func (r *repo) RemoveAuthorize(code string) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	return r.root.Update(func(tx *badger.Txn) error {
		return tx.Delete(r.authorizePath(code))
	})
}

func (r *repo) accessPath(code string) []byte {
	return badgerItemPath(accessBucket, code)
}

// SaveAccess
func (r *repo) SaveAccess(data *osin.AccessData) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}

	authorizeData := &osin.AuthorizeData{}

	if data.AccessData != nil && data.RefreshToken == "" {
		data.RefreshToken = data.AccessData.AccessToken
	}

	if data.AuthorizeData != nil {
		authorizeData = data.AuthorizeData
	}

	tx := r.root.NewWriteBatch()
	if data.RefreshToken != "" {
		if err := r.saveRefresh(tx, data.RefreshToken, data.AccessToken); err != nil {
			return err
		}
	}

	if data.Client == nil {
		return errors.Newf("data.Client must not be nil")
	}

	acc := acc{
		Client:       data.Client.GetId(),
		Authorize:    authorizeData.Code,
		AccessToken:  data.AccessToken,
		RefreshToken: data.RefreshToken,
		ExpiresIn:    time.Duration(data.ExpiresIn),
		Scope:        data.Scope,
		RedirectURI:  data.RedirectUri,
		CreatedAt:    data.CreatedAt.UTC(),
		Extra:        data.UserData,
	}
	raw, err := encodeFn(acc)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal access object")
	}
	if err = tx.Set(r.accessPath(acc.AccessToken), raw); err != nil {
		return err
	}
	return tx.Flush()
}

func loadRawAccess(a *osin.AccessData, loadDeps bool) func(raw []byte) error {
	return func(raw []byte) error {
		access := acc{}
		if err := decodeFn(raw, &access); err != nil {
			return errors.Annotatef(err, "Unable to unmarshal client object")
		}
		a.AccessToken = access.AccessToken
		a.RefreshToken = access.RefreshToken
		a.ExpiresIn = int32(access.ExpiresIn)
		a.Scope = access.Scope
		a.RedirectUri = access.RedirectURI
		a.CreatedAt = access.CreatedAt.UTC()
		if userData, ok := access.Extra.(string); ok {
			a.UserData = vocab.IRI(userData)
		}
		if len(access.Client) > 0 {
			a.Client = &osin.DefaultClient{Id: access.Client}
		}
		if loadDeps {
			if len(access.Authorize) > 0 {
				a.AuthorizeData = &osin.AuthorizeData{Code: access.Authorize}
			}
			if len(access.RefreshToken) > 0 {
				a.AccessData = &osin.AccessData{AccessToken: access.RefreshToken}
			}
		}
		return nil
	}
}

func (r *repo) loadTxnAccess(a *osin.AccessData, token string, loadDeps bool) func(tx *badger.Txn) error {
	fullPath := r.accessPath(token)
	return func(tx *badger.Txn) error {
		it, err := tx.Get(fullPath)
		if err != nil {
			return errors.NewNotFound(err, "Invalid path %s", fullPath)
		}
		if err = it.Value(loadRawAccess(a, loadDeps)); err != nil {
			return err
		}

		if a.Client != nil && len(a.Client.GetId()) > 0 {
			id := a.Client.GetId()
			client := osin.DefaultClient{}
			if err = r.loadTxnClient(&client, id)(tx); err == nil {
				a.Client = &client
			}
		}
		if loadDeps {
			if a.AuthorizeData != nil && len(a.AuthorizeData.Code) > 0 {
				auth := osin.AuthorizeData{}
				if err = r.loadTxnAuthorize(&auth, a.AuthorizeData.Code)(tx); err == nil {
					a.AuthorizeData = &auth
				}
			}
			if a.RefreshToken != "" {
				rf := osin.AccessData{}
				if err = r.loadTxnAccess(&rf, a.RefreshToken, false)(tx); err == nil {
					a.AccessData = &rf
				} else {
					a.AccessData = nil
				}
			}
		}
		return nil
	}
}

// LoadAccess
func (r *repo) LoadAccess(code string) (*osin.AccessData, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	if code == "" {
		return nil, errors.NotFoundf("empty access code")
	}

	result := new(osin.AccessData)
	err := r.root.View(r.loadTxnAccess(result, code, true))
	if err != nil {
		return nil, errors.Annotatef(err, "access code not found")
	}

	return result, err
}

// RemoveAccess
func (r *repo) RemoveAccess(token string) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	tx := r.root.NewWriteBatch()
	if err := tx.Delete(r.accessPath(token)); err != nil {
		return err
	}
	return tx.Flush()
}

func (r *repo) refreshPath(refresh string) []byte {
	return badgerItemPath(refreshBucket, refresh)
}

// LoadRefresh
func (r *repo) LoadRefresh(token string) (*osin.AccessData, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	if token == "" {
		return nil, errors.NotFoundf("Empty refresh token")
	}

	refresh := ref{}
	err := r.root.View(func(tx *badger.Txn) error {
		path := r.refreshPath(token)
		it, err := tx.Get(path)
		if err != nil {
			return errors.NewNotFound(err, "Invalid path %s", path)
		}
		return it.Value(func(val []byte) error {
			return decodeFn(val, &refresh)
		})
	})
	if err != nil {
		return nil, errors.NewNotFound(err, "Refresh token not found")
	}
	if refresh.Access == "" {
		return nil, errors.NotFoundf("Refresh token not found")
	}
	return r.LoadAccess(refresh.Access)
}

// RemoveRefresh revokes or deletes refresh AccessData.
func (r *repo) RemoveRefresh(token string) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}

	tx := r.root.NewWriteBatch()
	if err := tx.Delete(r.refreshPath(token)); err != nil {
		return err
	}
	return tx.Flush()
}

func (r *repo) saveRefresh(txn *badger.WriteBatch, refresh, access string) (err error) {
	ref := ref{
		Access: access,
	}
	raw, err := encodeFn(ref)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal refresh token object")
	}
	return txn.Set(r.refreshPath(refresh), raw)
}
