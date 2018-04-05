// scusiStore store module
package store

import (
	"encoding/json"
	"fmt"
	"github.com/dchest/blake2b"
	"github.com/dchest/blake2s"
	"github.com/peterbourgon/diskv"
	"path/filepath"
)

type Store struct {
	Name        string
	Description string
	Path        string
	blobstore   *diskv.Diskv
	metastore   *diskv.Diskv
}

// create a new store that consists of an blobstore and a metastore
func New(path string) (store *Store) {
	name := filepath.Base(path)
	store = &Store{
		Name:      name,
		Path:      path,
		blobstore: newBlobStore(path),
		metastore: newMetaStore(path),
	}
	return
}

// create a new blob store
func newBlobStore(basePath string) (blobStore *diskv.Diskv) {
	blobStorePath := filepath.Join(basePath, "blobstore")
	blobStore = diskv.New(diskv.Options{
		BasePath:     blobStorePath,
		CacheSizeMax: 1 << 30, // 1 GB
	})
	return
}

// create a new meta store
func newMetaStore(basePath string) (metaStore *diskv.Diskv) {
	metaStorePath := filepath.Join(basePath, "metastore")
	metaStore = diskv.New(diskv.Options{
		BasePath:     metaStorePath,
		CacheSizeMax: 1024 * 1024 * 5, // 5 MB
	})
	return
}

// add a file to the store
func (s *Store) AddFile(filename string, data []byte) (fileID string, err error) {
	filename = filepath.Base(filename)
	m := GenMeta(filename, data)
	j, err := Marshal(*m)
	if err != nil {
		return
	}
	err = s.metastore.Write(m.ID, j)
	if err != nil {
		return
	}
	err = s.blobstore.Write(m.ID, data)
	if err != nil {
		return
	}
	return m.ID, err
}

func (s *Store) GetMeta(fileID string) (meta Metadata, err error) {
	metaJSON, err := s.metastore.Read(fileID)
	if err != nil {
		return
	}
	err = json.Unmarshal(metaJSON, &meta)
	if err != nil {
		return
	}
	return
}
func (s *Store) GetFile(fileID string) (meta Metadata, blob []byte, err error) {
	blob, err = s.blobstore.Read(fileID)
	if err != nil {
		return
	}
	metaJSON, err := s.metastore.Read(fileID)
	if err != nil {
		return
	}
	err = json.Unmarshal(metaJSON, &meta)
	if err != nil {
		return
	}
	return
}

func (s *Store) RemoveFile(fileID string) (err error) {
	err = s.blobstore.Erase(fileID)
	if err != nil {
		return
	}
	err = s.metastore.Erase(fileID)
	if err != nil {
		return
	}
	return
}

func (s *Store) List() (metaList []Metadata, err error) {
	d := s.metastore
	closingChan := make(chan struct{})
	keyChan := d.Keys(closingChan)
	i := 0
	for s := range keyChan {
		//log.Debugf("%04d: %s\n", i, s)
		j, err := d.Read(s)
		if err != nil {
			return metaList, err
		}
		m := new(Metadata)
		err = json.Unmarshal(j, &m)
		if err != nil {
			return metaList, err
		}
		metaList = append(metaList, *m)
		i++
	}
	//log.Debugf("%d keys found.\n", i)
	return
}

func GenBlake2b32(data []byte) (c string) {
	b := blake2b.New256()
	b.Write(data)
	bsum := b.Sum(nil)
	return fmt.Sprintf("%x", bsum)
}

func GenBlake2s4(data []byte) (c string, err error) {
	hash, err := blake2s.New(&blake2s.Config{
		Size:   4,
		Person: []byte("scusi.v1"),
	})
	if err != nil {
		return "", err
	}
	_, err = hash.Write(data)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil

}

type Metadata struct {
	ID        string
	Filenames []string
	Size      int64
	Blake2b   string
}

func GenMeta(filename string, data []byte) (metaData *Metadata) {
	fileID, _ := GenBlake2s4(data)
	if fileID == "" {
		fileID = GenBlake2b32(data)
	}
	metaData = &Metadata{ID: fileID}
	metaData.Filenames = append(metaData.Filenames, filename)
	metaData.Size = int64(len(data))
	metaData.Blake2b = GenBlake2b32(data)
	return metaData
}

func Marshal(m Metadata) (jsonBytes []byte, err error) {
	jsonBytes, err = json.Marshal(m)
	if err != nil {
		return
	}
	return

}
