// scusiStore - a file storeage based on diskv.
//
// a scusiStore actualy consists of two diskv stores.
// a blobstore for the actual file content and a metastore for file metadata,
// such as filenames, size, checksum,...
package store

import (
	"encoding/json"
	"fmt"
	"github.com/dchest/blake2b"
	"github.com/dchest/blake2s"
	"github.com/peterbourgon/diskv"
	"path/filepath"
	"io"
)

// Store - datastruct for a scusiStore Store
type Store struct {
	Name        string
	Description string
	Path        string
	blobstore   *diskv.Diskv
	metastore   *diskv.Diskv
}

// New - creates a new store that consists of an blobstore and a metastore
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

// AddFile - add a file to the storedd
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

// GetMeta - retrieves just the metadata for a given file from store.
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

// GetFile - retrives a file and its metadata from store
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

// GetFileReader - retrieves a reader for a given file
func (s *Store) GetFileReader(fileID string) (reader io.ReadCloser, err error) {
		reader, err = s.blobstore.ReadStream(fileID, true)
		if err != nil {
			return
		}
		return
}

// RemoveFile - removes a file and its metadata from store
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

// List - lists all metadata for all files in store
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

// GenBlake2b32 - genertes a blake2b 32 byte checksum over given data.
// aka long ID
func GenBlake2b32(data []byte) (c string) {
	b := blake2b.New256()
	b.Write(data)
	bsum := b.Sum(nil)
	return fmt.Sprintf("%x", bsum)
}

// GenBlake2s4 - generates a blake2s 4 byte checksum over given data.
// aka short ID
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

// Metadata - datastruct for metadata of a given file
type Metadata struct {
	ID        string
	Filenames []string
	Size      int64
	Blake2b   string
	Custom    interface{}
}

func (s *Store) SetCustom(fileID string, custom interface{}) (err error) {
	meta, err := s.GetMeta(fileID)
	if err != nil {
		return
	}
	meta.Custom = custom
	j, err := Marshal(meta)
	if err != nil {
		return
	}
	err = s.metastore.Write(meta.ID, j)
	if err != nil {
		return
	}
	return
}

func (s *Store) GetCustom(fileID string) (custom interface{}, err error) {
	meta, err := s.GetMeta(fileID)
	if err != nil {
		return
	}
	return meta.Custom, nil
}

// GenMeta - function to generate metadata for given bytes
func GenMeta(filename string, data []byte) (metaData *Metadata) {
	var blake2b32Available bool
	fileID, err := GenBlake2s4(data)
	if err != nil {
		fileID = GenBlake2b32(data)
		blake2b32Available = true
	}
	metaData = &Metadata{ID: fileID}
	metaData.Filenames = append(metaData.Filenames, filename)
	metaData.Size = int64(len(data))
	if blake2b32Available == true {
		metaData.Blake2b = fileID
	} else {
		metaData.Blake2b = GenBlake2b32(data)
	}
	return metaData
}

// Marshal - marshals metadata from JSON to datastruct.
func Marshal(m Metadata) (jsonBytes []byte, err error) {
	jsonBytes, err = json.Marshal(m)
	if err != nil {
		return
	}
	return
}
