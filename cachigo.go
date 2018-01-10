package cachigo

import (
	"time"

	"github.com/boltdb/bolt"
)

//CachingSevice
type CachingService struct {
	db *bolt.DB
}

var bucketname = []byte("cachigo")

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
