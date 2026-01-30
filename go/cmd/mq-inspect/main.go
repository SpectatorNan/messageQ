package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type LogType byte

const (
	LogProduce LogType = 1
	LogAck     LogType = 2
	LogNack    LogType = 3
	LogRetry   LogType = 4
	LogDLQ     LogType = 5
)

type Message struct {
	ID    int64  `json:"id"`
	Body  string `json:"body"`
	Retry int    `json:"retry"`
	// Timestamp omitted for inspect simplicity
}

type LogEntry struct {
	Type  LogType `json:"type"`
	Topic string  `json:"topic"`
	Msg   Message `json:"msg"`
}

func listSegments(dir string, topic string) ([]string, error) {
	topicDir := filepath.Join(dir, topic)
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, err
	}
	segs := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".wal" {
			continue
		}
		segs = append(segs, filepath.Join(topicDir, e.Name()))
	}
	sort.Strings(segs)
	return segs, nil
}

func typName(t LogType) string {
	switch t {
	case LogProduce:
		return "PRODUCE"
	case LogAck:
		return "ACK"
	case LogNack:
		return "NACK"
	case LogRetry:
		return "RETRY"
	case LogDLQ:
		return "DLQ"
	default:
		return fmt.Sprintf("TYPE(%d)", t)
	}
}

func inspectSegment(path string, showPayload bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Printf("Segment: %s\n", path)
	idx := 0
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		var tbuf [1]byte
		if _, err := io.ReadFull(f, tbuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		typ := LogType(tbuf[0])
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		idx++
		if showPayload {
			var entry LogEntry
			if err := json.Unmarshal(payload, &entry); err != nil {
				fmt.Printf("  %d: %s id=? (malformed payload)\n", idx, typName(typ))
				continue
			}
			fmt.Printf("  %d: %s id=%d body_len=%d\n", idx, typName(typ), entry.Msg.ID, len(entry.Msg.Body))
		} else {
			fmt.Printf("  %d: %s payload_len=%d\n", idx, typName(typ), n)
		}
	}
	return nil
}

func main() {
	dir := flag.String("dir", "./testdata", "base data directory")
	topic := flag.String("topic", "test", "topic to inspect")
	show := flag.Bool("show", false, "show parsed payloads")
	flag.Parse()
	segs, err := listSegments(*dir, *topic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing segments: %v\n", err)
		os.Exit(2)
	}
	if len(segs) == 0 {
		fmt.Printf("no segments found for topic %s in %s\n", *topic, *dir)
		return
	}
	for _, s := range segs {
		if err := inspectSegment(s, *show); err != nil {
			fmt.Fprintf(os.Stderr, "error inspecting %s: %v\n", s, err)
		}
	}
}
