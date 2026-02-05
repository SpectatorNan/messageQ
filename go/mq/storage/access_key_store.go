package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type (
	// AccessKeyRecord stores an access key record.
	AccessKeyRecord struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Hash      string `json:"hash"`
		CreatedAt int64  `json:"createdAt"`
	}

	AccessKeyInfo struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		CreatedAt int64  `json:"createdAt"`
	}

	AccessKeyStore struct {
		mu   sync.RWMutex
		aks  map[string]AccessKeyRecord
		path string
	}
)

func NewAccessKeyStore(path string) (*AccessKeyStore, error) {
	store := &AccessKeyStore{
		aks:  make(map[string]AccessKeyRecord),
		path: path,
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *AccessKeyStore) List() []AccessKeyInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	infos := make([]AccessKeyInfo, 0, len(s.aks))
	for _, rec := range s.aks {
		infos = append(infos, AccessKeyInfo{
			ID:        rec.ID,
			Name:      rec.Name,
			CreatedAt: rec.CreatedAt,
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].CreatedAt < infos[j].CreatedAt })
	return infos
}

func (s *AccessKeyStore) Add(name, ak string) (AccessKeyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := uuid.New().String()
	hash := s.hashKey(ak)
	record := AccessKeyRecord{
		ID:        id,
		Name:      name,
		Hash:      hash,
		CreatedAt: time.Now().Unix(),
	}
	s.aks[id] = record
	return record, s.persist()
}

func (s *AccessKeyStore) Remove(id string) error {
	if id == "" {
		return errors.New("empty id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.aks, id)
	return s.persist()
}

func (s *AccessKeyStore) Has(ak string) bool {
	if ak == "" {
		return false
	}
	keyHash := s.hashKey(ak)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rec := range s.aks {
		if rec.Hash == keyHash {
			return true
		}
	}
	return false
}

func (s *AccessKeyStore) load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var list []AccessKeyRecord
	if err := json.Unmarshal(b, &list); err != nil {
		return err
	}
	for _, rec := range list {
		if rec.ID != "" && rec.Hash != "" {
			s.aks[rec.ID] = rec
		}
	}
	return nil
}

func (s *AccessKeyStore) persist() error {
	list := s.listRecords()
	b, err := json.Marshal(list)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if f, err := os.OpenFile(tmp, os.O_RDWR, 0o644); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (s *AccessKeyStore) listRecords() []AccessKeyRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	records := make([]AccessKeyRecord, 0, len(s.aks))
	for _, rec := range s.aks {
		records = append(records, rec)
	}
	return records
}

func (s *AccessKeyStore) hashKey(ak string) string {
	sum := sha256.Sum256([]byte(ak))
	return hex.EncodeToString(sum[:])
}
