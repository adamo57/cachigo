package cachigo

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/boltdb/bolt"
)

//CachingSevice
type CachingService struct {
	db *bolt.DB
}

var (
	bucketname = []byte("cachigo")

	//ErrBadValue is an error that should only happen when
	//there is a bad value trying to be stored.
	ErrBadValue = errors.New("Bad key")
)

//Open handles the opening of the bolt instance. Path is the full path to the database file.
// 0640 opend the file with -rw-r----- permissions
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
	}

	return nil
}
