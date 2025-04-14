package badger

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"time"

	"github.com/dgraph-io/badger/v4"
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
	Client      string
	Code        string
	ExpiresIn   time.Duration
	Scope       string
	RedirectURI string
	State       string
	CreatedAt   time.Time
	Extra       interface{}
}

type acc struct {
	Client       string
	Authorize    string
	Previous     string
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
	if err := r.close(); err != nil {
		r.errFn("error closing the badger db: %+s", err)
	}
}

// Clone
func (r *repo) Clone() osin.Storage {
	r.Close()
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
	if id == "" {
		return nil, errors.NotFoundf("Empty client id")
	}

	c := new(osin.DefaultClient)
	if err := r.d.View(r.loadTxnClient(c, id)); err != nil {
		return nil, err
	}
	return c, nil
}

func (r *repo) ListClients() ([]osin.Client, error) {
	clients := make([]osin.Client, 0)
	err := r.d.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = badgerItemPath(clientsBucket)
		it := tx.NewIterator(opts)
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			item := it.Item()

			c := osin.DefaultClient{}
			item.Value(loadRawClient(&c))

			clients = append(clients, &c)
		}
		return nil
	})
	return clients, err
}

// UpdateClient updates the client (identified by it's id) and replaces the values with the values of client.
func (r *repo) UpdateClient(c osin.Client) error {
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
	return r.d.NewWriteBatch().Set(r.clientPath(c.GetId()), raw)
}

// CreateClient stores the client in the database and returns an error, if something went wrong.
func (r *repo) CreateClient(c osin.Client) error {
	return r.UpdateClient(c)
}

// RemoveClient removes a client (identified by id) from the database. Returns an error if something went wrong.
func (r *repo) RemoveClient(id string) error {
	return r.d.NewWriteBatch().Delete(r.clientPath(id))
}

func (r *repo) authorizePath(code string) []byte {
	return badgerItemPath(authorizeBucket, code)
}

// SaveAuthorize
func (r *repo) SaveAuthorize(data *osin.AuthorizeData) error {
	auth := auth{
		Client:      data.Client.GetId(),
		Code:        data.Code,
		ExpiresIn:   time.Duration(data.ExpiresIn),
		Scope:       data.Scope,
		RedirectURI: data.RedirectUri,
		State:       data.State,
		CreatedAt:   data.CreatedAt.UTC(),
		Extra:       data.UserData,
	}
	raw, err := encodeFn(auth)
	if err != nil {
		return errors.Annotatef(err, "Unable to marshal authorization object")
	}
	return r.d.NewWriteBatch().Set(r.authorizePath(data.Code), raw)
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
		if a.Client == nil {
			client := new(osin.DefaultClient)
			if err := r.loadTxnClient(client, a.Client.GetId())(tx); err != nil {
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
		a.UserData = auth.Extra
		if len(auth.Code) > 0 {
			a.Client = &osin.DefaultClient{Id: auth.Code}
		}
		if a.ExpireAt().Before(time.Now().UTC()) {
			return errors.Errorf("Token expired at %s.", a.ExpireAt().String())
		}
		return nil
	}
}

// LoadAuthorize
func (r *repo) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	if code == "" {
		return nil, errors.NotFoundf("Empty authorize code")
	}
	data := osin.AuthorizeData{}

	err := r.d.View(r.loadTxnAuthorize(&data, code))
	if err != nil {
		return nil, err
	}
	if data.Client != nil {
	}
	return &data, err
}

// RemoveAuthorize
func (r *repo) RemoveAuthorize(code string) error {
	return r.d.Update(func(tx *badger.Txn) error {
		return tx.Delete(r.authorizePath(code))
	})
}

func (r *repo) accessPath(code string) []byte {
	return badgerItemPath(accessBucket, code)
}

// SaveAccess
func (r *repo) SaveAccess(data *osin.AccessData) error {
	prev := ""
	authorizeData := &osin.AuthorizeData{}

	if data.AccessData != nil {
		prev = data.AccessData.AccessToken
	}

	if data.AuthorizeData != nil {
		authorizeData = data.AuthorizeData
	}

	db := r.d.NewWriteBatch()
	if data.RefreshToken != "" {
		if err := r.saveRefresh(db, data.RefreshToken, data.AccessToken); err != nil {
			r.errFn("Failed saving refresh token for client id %s: %+s", data.Client.GetId(), err)
			return err
		}
		return nil
	}

	if data.Client == nil {
		return errors.Newf("data.Client must not be nil")
	}

	acc := acc{
		Client:       data.Client.GetId(),
		Authorize:    authorizeData.Code,
		Previous:     prev,
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
	return db.Set(r.accessPath(acc.AccessToken), raw)
}

func loadRawAccess(a *osin.AccessData) func(raw []byte) error {
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
		a.UserData = access.Extra
		if len(access.Authorize) > 0 {
			a.AuthorizeData = &osin.AuthorizeData{Code: access.Authorize}
		}
		if len(access.Previous) > 0 {
			a.AccessData = &osin.AccessData{AccessToken: access.Previous}
		}
		return nil
	}
}

func (r *repo) loadTxnAccess(a *osin.AccessData, token string) func(tx *badger.Txn) error {
	fullPath := r.accessPath(token)
	return func(tx *badger.Txn) error {
		it, err := tx.Get(fullPath)
		if err != nil {
			return errors.NewNotFound(err, "Invalid path %s", fullPath)
		}
		return it.Value(loadRawAccess(a))
	}
}

// LoadAccess
func (r *repo) LoadAccess(code string) (*osin.AccessData, error) {
	if code == "" {
		return nil, errors.NotFoundf("empty access code")
	}

	result := new(osin.AccessData)
	err := r.d.View(r.loadTxnAccess(result, code))
	if err != nil {
		return nil, errors.Annotatef(err, "access code not found")
	}

	if result.Client != nil && len(result.Client.GetId()) > 0 {
		client := new(osin.DefaultClient)
		if err = r.d.View(r.loadTxnClient(client, result.Client.GetId())); err == nil {
			result.Client = client
		}
	}
	if result.AuthorizeData != nil && len(result.AuthorizeData.Code) > 0 {
		auth := new(osin.AuthorizeData)
		if err = r.d.View(r.loadTxnAuthorize(auth, result.AuthorizeData.Code)); err == nil {
			result.AuthorizeData = auth
		}
	}
	if result.AccessData != nil && len(result.AccessData.AccessToken) > 0 {
		prev := new(osin.AccessData)
		if err = r.d.View(r.loadTxnAccess(prev, result.AuthorizeData.Code)); err == nil {
			result.AccessData = prev
		}
	}

	return result, err
}

// RemoveAccess
func (r *repo) RemoveAccess(token string) error {
	return r.d.NewWriteBatch().Delete(r.accessPath(token))
}

func (r *repo) refreshPath(refresh string) []byte {
	return badgerItemPath(refreshBucket, refresh)
}

// LoadRefresh
func (r *repo) LoadRefresh(token string) (*osin.AccessData, error) {
	if token == "" {
		return nil, errors.NotFoundf("Empty refresh token")
	}
	return nil, nil
}

// RemoveRefresh revokes or deletes refresh AccessData.
func (r *repo) RemoveRefresh(token string) error {
	return r.d.NewWriteBatch().Delete(r.refreshPath(token))
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
