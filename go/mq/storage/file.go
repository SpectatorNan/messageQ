// Deprecated: FileStorage is deprecated — use WALStorage (mq/storage/wal.go) instead for persistence.
package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type FileStorage struct {
	baseDir string
}

func NewFileStorage(baseDir string) *FileStorage {
	_ = os.MkdirAll(baseDir, 0o755)
	return &FileStorage{baseDir: baseDir}
}

func (f *FileStorage) topicLogPath(topic string) string {
	return filepath.Join(f.baseDir, topic+".log")
}

func (f *FileStorage) topicAckPath(topic string) string {
	return filepath.Join(f.baseDir, topic+".acked")
}

func (f *FileStorage) Append(topic string, msg Message) error {
	p := f.topicLogPath(topic)
	fi, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer fi.Close()
	b, _ := json.Marshal(msg)
	_, err = fi.Write(append(b, '\n'))
	return err
}

// Load reads all messages from the topic log and filters out acked ids
func (f *FileStorage) Load(topic string) ([]Message, error) {
	p := f.topicLogPath(topic)
	file, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return []Message{}, nil
		}
		return nil, err
	}
	defer file.Close()

	acked := map[int64]struct{}{}
	ackp := f.topicAckPath(topic)
	if af, err := os.Open(ackp); err == nil {
		s := bufio.NewScanner(af)
		for s.Scan() {
			if id, err := strconv.ParseInt(strings.TrimSpace(s.Text()), 10, 64); err == nil {
				acked[id] = struct{}{}
			}
		}
		af.Close()
	}

	res := make([]Message, 0)
	s := bufio.NewScanner(file)
	for s.Scan() {
		line := s.Text()
		var m Message
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			// skip malformed lines but continue
			continue
		}
		if _, ok := acked[m.ID]; ok {
			continue
		}
		res = append(res, m)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func (f *FileStorage) Ack(topic string, id int64) error {
	p := f.topicAckPath(topic)
	fi, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer fi.Close()
	_, err = fi.WriteString(fmt.Sprintln(id))
	return err
}
