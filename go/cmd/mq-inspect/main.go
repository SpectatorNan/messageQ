package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
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
	ID        int64
	Body      string
	Retry     int
	Timestamp time.Time
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
		total := binary.BigEndian.Uint32(lenBuf[:])
		payload := make([]byte, total)
		if _, err := io.ReadFull(f, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		idx++
		off := 0
		crc := binary.BigEndian.Uint32(payload[off : off+4])
		off += 4
		typ := LogType(payload[off])
		off += 1
		msgID := int64(binary.BigEndian.Uint64(payload[off : off+8]))
		off += 8
		retry := int(binary.BigEndian.Uint16(payload[off : off+2]))
		off += 2
		ts := int64(binary.BigEndian.Uint64(payload[off : off+8]))
		off += 8
		bodyLen := int(binary.BigEndian.Uint32(payload[off : off+4]))
		off += 4
		if bodyLen < 0 || off+bodyLen > len(payload) {
			fmt.Printf("  %d: %s invalid bodyLen=%d\n", idx, typName(typ), bodyLen)
			continue
		}
		body := payload[off : off+bodyLen]
		crcOK := (bodyLen == 0) || (crc32.ChecksumIEEE(body) == crc)
		if showPayload {
			fmt.Printf("  %d: %s id=%d retry=%d ts=%s body_len=%d crc_ok=%v\n", idx, typName(typ), msgID, retry, time.Unix(0, ts).Format(time.RFC3339Nano), bodyLen, crcOK)
		} else {
			fmt.Printf("  %d: %s body_len=%d crc_ok=%v\n", idx, typName(typ), bodyLen, crcOK)
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
