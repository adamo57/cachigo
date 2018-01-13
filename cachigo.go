package cachigo

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/boltdb/bolt"
)

//CachingSevice is the db.
type CachingService struct {
	db *bolt.DB
}

var (
	bucketname = []byte("cachigo")

	//ErrEmptyKey is an error that should only happen when an empty key is
	//given to be deleted from the db
	ErrEmptyKey = errors.New("Empty key")

	//ErrBadValue is an error that should only happen when
	//there is a bad value trying to be stored.
	ErrBadValue = errors.New("Bad value")
)

//Open handles the opening of the bolt instance. Path is the full path to the database file.
// 0640 opens the file with -rw-r----- permissions
func Open(path string) (*CachingService, error) {
	boltOptions := &bolt.Options{
		Timeout: 15 * time.Second,
	}

	if db, err := bolt.Open(path, 0640, boltOptions); err != nil {
		return nil, err
	} else {
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucketname)
			return err
		})
		if err != nil {
			return nil, err
		} else {
			return &CachingService{db: db}, nil
		}
	}
}

//Close closes the db
func (cs *CachingService) Close() error {
	return cs.db.Close()
}

//Put inserts a key and a value into the data store
func (cs *CachingService) Put(key string, value interface{}) error {
	if value == nil {
		return ErrBadValue
	} else {
		cs.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketname)
			enc, err := json.Marshal(value)
			if err != nil {
				log.Fatal(err)
			}
			err = b.Put([]byte(key), enc)
			return err
		})
		return nil
	}
}

//Delete removes the key from the data store.
func (cs *CachingService) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	} else {
		cs.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketname)
			err := b.Delete([]byte(key))
			if err != nil {
				return err
			}
			return nil
		})
	}
	return nil
}

//Get will lookup a key and return the keys value if found.
func (cs *CachingService) Get(needle string) ([]byte, error) {
	var ret []byte
	cs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketname)
		c := b.Cursor()
		bn := []byte(needle)

		for k, v := c.First(); k != nil && bytes.Contains(bn, k); k, v = c.Next() {
			ret = v
			return nil
		}
		return nil
	})
	return ret, nil
}
