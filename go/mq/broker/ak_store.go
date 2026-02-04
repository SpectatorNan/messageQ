package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// AKStore manages access keys with persistence.
type AKStore struct {
	mu   sync.RWMutex
	aks  map[string]AKRecord
	path string
}

// AKRecord stores a hashed access key.
type AKRecord struct {
	ID        string    `json:"id"`
	Hash      string    `json:"hash"`
	CreatedAt time.Time `json:"created_at"`
}

// AKInfo is returned to clients (no hash exposure).
type AKInfo struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

func newAKStore(path string) (*AKStore, error) {
	store := &AKStore{
		aks:  make(map[string]AKRecord),
		path: path,
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *AKStore) load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var list []AKRecord
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

func (s *AKStore) persist() error {
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

func (s *AKStore) Add(ak string) (string, error) {
	if ak == "" {
		return "", fmt.Errorf("empty ak")
	}
	keyHash := hashAK(ak)

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.aks {
		if rec.Hash == keyHash {
			return rec.ID, nil
		}
	}

	id := uuid.NewString()
	s.aks[id] = AKRecord{
		ID:        id,
		Hash:      keyHash,
		CreatedAt: time.Now(),
	}
	return id, s.persist()
}

func (s *AKStore) Remove(id string) error {
	if id == "" {
		return fmt.Errorf("empty id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.aks, id)
	return s.persist()
}

func (s *AKStore) Has(ak string) bool {
	if ak == "" {
		return false
	}
	keyHash := hashAK(ak)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rec := range s.aks {
		if rec.Hash == keyHash {
			return true
		}
	}
	return false
}

func (s *AKStore) List() []AKInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]AKInfo, 0, len(s.aks))
	for _, rec := range s.aks {
		out = append(out, AKInfo{ID: rec.ID, CreatedAt: rec.CreatedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *AKStore) listRecords() []AKRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]AKRecord, 0, len(s.aks))
	for _, rec := range s.aks {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func hashAK(ak string) string {
	sum := sha256.Sum256([]byte(ak))
	return hex.EncodeToString(sum[:])
}
