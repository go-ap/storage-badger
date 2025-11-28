package badger

import (
	"bytes"
	"crypto"
	"crypto/dsa"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"golang.org/x/crypto/bcrypt"
)

type Metadata struct {
	Pw         []byte `jsonld:"pw,omitempty"`
	PrivateKey []byte `jsonld:"key,omitempty"`
}

const metaDataKey = "__meta_data"

func getMetadataKey(p []byte) []byte {
	return bytes.Join([][]byte{p, []byte(metaDataKey)}, sep)
}

// PasswordSet
func (r *repo) PasswordSet(iri vocab.IRI, pw []byte) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	if pw == nil {
		return errors.Newf("could not generate hash for nil pw")
	}
	path := itemPath(iri)
	if len(path) == 0 {
		return errors.NotFoundf("not found")
	}

	m := Metadata{}
	err := r.root.Update(func(tx *badger.Txn) error {
		path = getMetadataKey(path)
		i, err := tx.Get(path)
		if err == nil {
			err = i.Value(func(raw []byte) error {
				return decodeFn(raw, &m)
			})
			if err != nil {
				return err
			}
		}

		pw, err = bcrypt.GenerateFromPassword(pw, -1)
		if err != nil {
			return errors.Annotatef(err, "Could not encrypt the pw")
		}

		m.Pw = pw
		entryBytes, err := encodeFn(m)
		if err != nil {
			return errors.Annotatef(err, "Could not marshal metadata")
		}
		err = tx.Set(path, entryBytes)
		if err != nil {
			return errors.Annotatef(err, "Could not insert entry: %s", path)
		}
		return nil
	})

	return err
}

// PasswordCheck
func (r *repo) PasswordCheck(iri vocab.IRI, pw []byte) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	path := itemPath(iri)

	m := Metadata{}
	err := r.root.View(func(tx *badger.Txn) error {
		i, err := tx.Get(getMetadataKey(path))
		if err != nil {
			return errors.NewNotFound(err, "not found")
		}
		err = i.Value(func(raw []byte) error {
			if err = decodeFn(raw, &m); err != nil {
				return errors.Annotatef(err, "Could not unmarshal metadata")
			}
			return nil
		})
		if err != nil {
			return err
		}
		if err := bcrypt.CompareHashAndPassword(m.Pw, pw); err != nil {
			return errors.NewUnauthorized(err, "Invalid pw")
		}
		return nil
	})
	return err
}

// LoadMetadata
func (r *repo) LoadMetadata(iri vocab.IRI, m any) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	path := itemPath(iri)

	return r.root.View(func(tx *badger.Txn) error {
		i, err := tx.Get(getMetadataKey(path))
		if err != nil {
			return errors.NewNotFound(err, "not found")
		}
		return i.Value(func(raw []byte) error {
			return decodeFn(raw, &m)
		})
	})
}

// SaveMetadata
func (r *repo) SaveMetadata(iri vocab.IRI, m any) error {
	if r == nil || r.root == nil {
		return errNotOpen
	}
	if m == nil {
		return errors.Newf("Could not save nil metadata")
	}
	path := itemPath(iri)

	return r.root.Update(func(tx *badger.Txn) error {
		entryBytes, err := encodeFn(m)
		if err != nil {
			return errors.Annotatef(err, "Could not marshal metadata")
		}
		err = tx.Set(getMetadataKey(path), entryBytes)
		if err != nil {
			return errors.Annotatef(err, "Could not insert entry: %s", path)
		}
		return nil
	})
}

// LoadKey loads a private key for an actor found by its IRI
func (r *repo) LoadKey(iri vocab.IRI) (crypto.PrivateKey, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil {
		return nil, err
	}
	b, _ := pem.Decode(m.PrivateKey)
	if b == nil {
		return nil, errors.Errorf("failed decoding pem")
	}
	prvKey, err := x509.ParsePKCS8PrivateKey(b.Bytes)
	if err != nil {
		return nil, err
	}
	return prvKey, nil
}

// SaveKey saves a private key for an actor found by its IRI
func (r *repo) SaveKey(iri vocab.IRI, key crypto.PrivateKey) (*vocab.PublicKey, error) {
	if r == nil || r.root == nil {
		return nil, errNotOpen
	}
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	if m.PrivateKey != nil {
		r.logFn("actor %s already has a private key", iri)
	}
	prvEnc, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}

	m.PrivateKey = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: prvEnc,
	})
	if err = r.SaveMetadata(iri, m); err != nil {
		return nil, err
	}

	var pub crypto.PublicKey
	switch prv := key.(type) {
	case *ecdsa.PrivateKey:
		pub = prv.Public()
	case *rsa.PrivateKey:
		pub = prv.Public()
	case *dsa.PrivateKey:
		pub = &prv.PublicKey
	case ed25519.PrivateKey:
		pub = prv.Public()
	default:
		r.errFn("received key %T does not match any of the known private key types", key)
		return nil, nil
	}
	pubEnc, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		r.errFn("unable to x509.MarshalPKIXPublicKey() the private key %T for %s", pub, iri)
		return nil, err
	}
	pubEncoded := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	return &vocab.PublicKey{
		ID:           vocab.IRI(fmt.Sprintf("%s#main", iri)),
		Owner:        iri,
		PublicKeyPem: string(pubEncoded),
	}, nil
}
