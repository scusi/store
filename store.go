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
	"io"
	"os"
	"path/filepath"
	"time"
	//"log"
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

// AddFile - add a file to the store
//
// NOTE: this function is only suiteable for relative small files, since it
//       reads the whole file into memory.
//       User WriteStream for large files instead
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

// WriteStream - like AddFile but for large files
func (s *Store) WriteStream(filename string, r io.Reader, sync bool) (fileID string, err error) {
	filename = filepath.Base(filename)
	m, r, err := GenMetaStream(filename, r)
	if err != nil {
		return
	}
	j, err := Marshal(*m)
	if err != nil {
		return
	}
	err = s.metastore.Write(m.ID, j)
	if err != nil {
		return
	}
	err = s.blobstore.WriteStream(m.ID, r, true)
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
//
// NOTE: this function is only suiteable for relative small files, since it
//       reads the whole file into memory.
// 	 Use GetFileReader for large files!
//
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

// TouchFile - touches the metadata of a file in store
// With a touch the last accessed time is changed.
// In Systems that use this timestamp to determine when
// a file in store times out it will effectifly lead
// to longer storage periods for that file.
func (s *Store) Touch(fileID string) (err error) {
	filePath := filepath.Join(s.metastore.BasePath, fileID)
	//filePath := filepath.Join(dataDir, "metastore", fileID)
	now := time.Now()
	err = os.Chtimes(filePath, now, now)
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

func GenBlake2b32Reader(r io.Reader) (c string, err error) {
	b := blake2b.New256()
	if _, err := io.Copy(b, r); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b.Sum(nil)), nil
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

// GenBlake2s4Reader - generates a blake2s 4 byte checksum over given data.
// aka short ID
func GenBlake2s4Reader(r io.Reader) (c string, err error) {
	hash, err := blake2s.New(&blake2s.Config{
		Size:   4,
		Person: []byte("scusi.v1"),
	})
	if err != nil {
		return "", err
	}

	if _, err := io.Copy(hash, r); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// Metadata - datastruct for metadata of a given file
type Metadata struct {
	ID        string      // ID holds a unique ID for that file
	Filenames []string    // Filenames, a list of filenames for that file, the first filename is used by default
	Size      int64       // Size of the file in bytes
	Blake2b   string      // 32 byte blake2b checksum of the file content
	Custom    interface{} // a custom interface for custom fields
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

// GenMetaStream - function to generate metadata for given stream of bytes
func GenMetaStream(filename string, r io.Reader) (metaData *Metadata, nr io.Reader, err error) {
	// gen a reader for blake2s
	b2s, err := blake2s.New(&blake2s.Config{
		Size:   4,
		Person: []byte("scusi.v1"),
	})
	if err != nil {
		return metaData, r, err
	}
	// gen a reader for blake2b-256
	b2b := blake2b.New256()
	// gen a reader for size
	sizeReader := io.Discard
	// create a MultiWriter
	mw := io.MultiWriter(b2s, b2b, sizeReader)
	// copy input reader to multiWriter 
	r = io.TeeReader(r, mw)
	size, err := io.Copy(mw, r)
	if err != nil {
		return metaData, r, err
	}
	metaData = &Metadata{ID: fmt.Sprintf("%x", b2s.Sum(nil))}
	metaData.Filenames = append(metaData.Filenames, filename)
	metaData.Size = size
	metaData.Blake2b = fmt.Sprintf("%x", b2b.Sum(nil))

	return metaData, r, err
}

// Marshal - marshals metadata from JSON to datastruct.
func Marshal(m Metadata) (jsonBytes []byte, err error) {
	jsonBytes, err = json.Marshal(m)
	if err != nil {
		return
	}
	return
}
