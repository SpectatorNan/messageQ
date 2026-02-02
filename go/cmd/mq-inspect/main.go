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

	"github.com/google/uuid"
)

type Message struct {
	ID        string
	Body      string
	Tag       string
	Retry     int
	Timestamp time.Time
}

const cqEntrySize = 20

func listSegments(dir string, topic string, queueID int) ([]string, error) {
	commitDir := filepath.Join(dir, "commitlog", topic, fmt.Sprintf("%d", queueID))
	entries, err := os.ReadDir(commitDir)
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
		segs = append(segs, filepath.Join(commitDir, e.Name()))
	}
	sort.Strings(segs)
	return segs, nil
}

func inspectSegment(path string, showPayload bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Printf("Segment: %s\n", path)

	// read and validate WAL header
	var hdr [8]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		return err
	}
	if string(hdr[0:4]) != "MQW1" {
		return fmt.Errorf("invalid wal magic")
	}

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
		idBytes := payload[off : off+16]
		off += 16
		uid, err := uuid.FromBytes(idBytes)
		if err != nil {
			fmt.Printf("  %d: invalid uuid\n", idx)
			continue
		}
		msgID := uid.String()
		retry := int(binary.BigEndian.Uint16(payload[off : off+2]))
		off += 2
		ts := int64(binary.BigEndian.Uint64(payload[off : off+8]))
		off += 8
		tagLen := int(binary.BigEndian.Uint16(payload[off : off+2]))
		off += 2
		if tagLen < 0 || off+tagLen > len(payload) {
			fmt.Printf("  %d: invalid tagLen=%d\n", idx, tagLen)
			continue
		}
		tag := string(payload[off : off+tagLen])
		off += tagLen
		bodyLen := int(binary.BigEndian.Uint32(payload[off : off+4]))
		off += 4
		if bodyLen < 0 || off+bodyLen > len(payload) {
			fmt.Printf("  %d: invalid bodyLen=%d\n", idx, bodyLen)
			continue
		}
		body := payload[off : off+bodyLen]
		crcOK := (bodyLen == 0) || (crc32.ChecksumIEEE(body) == crc)
		if showPayload {
			fmt.Printf("  %d: id=%s tag=%s retry=%d ts=%s body_len=%d crc_ok=%v\n", idx, msgID, tag, retry, time.Unix(0, ts).Format(time.RFC3339Nano), bodyLen, crcOK)
		} else {
			fmt.Printf("  %d: tag=%s body_len=%d crc_ok=%v\n", idx, tag, bodyLen, crcOK)
		}
	}
	return nil
}

func inspectConsumeQueue(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Printf("ConsumeQueue: %s\n", path)
	idx := 0
	for {
		var buf [cqEntrySize]byte
		if _, err := io.ReadFull(f, buf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		idx++
		segID := binary.BigEndian.Uint32(buf[0:4])
		pos := binary.BigEndian.Uint64(buf[4:12])
		size := binary.BigEndian.Uint32(buf[12:16])
		tagHash := binary.BigEndian.Uint32(buf[16:20])
		fmt.Printf("  %d: seg=%08d pos=%d size=%d tagHash=%08x\n", idx, segID, pos, size, tagHash)
	}
	return nil
}

func inspectOffsets(dir, group, topic string, queueID int) error {
	path := filepath.Join(dir, "offsets", group, topic, fmt.Sprintf("%d.offset", queueID))
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(b) < 8 {
		return fmt.Errorf("invalid offset file")
	}
	offset := binary.BigEndian.Uint64(b[:8])
	fmt.Printf("Offset: group=%s topic=%s queue=%d offset=%d\n", group, topic, queueID, offset)
	return nil
}

func consumeQueuePath(dir, topic string, queueID int) string {
	return filepath.Join(dir, "consumequeue", topic, fmt.Sprintf("%d", queueID), "00000001.cq")
}

func main() {
	dir := flag.String("dir", "./testdata", "base data directory")
	topic := flag.String("topic", "test", "topic to inspect")
	queueID := flag.Int("queue", 0, "queue id")
	show := flag.Bool("show", false, "show parsed payloads")
	showCQ := flag.Bool("cq", false, "inspect consumequeue")
	showOffsets := flag.Bool("offsets", false, "inspect consumer offsets")
	group := flag.String("group", "", "consumer group (for offsets)")
	flag.Parse()

	if *showOffsets {
		if *group == "" {
			fmt.Fprintf(os.Stderr, "group is required for -offsets\n")
			os.Exit(2)
		}
		if err := inspectOffsets(*dir, *group, *topic, *queueID); err != nil {
			fmt.Fprintf(os.Stderr, "error inspecting offsets: %v\n", err)
			os.Exit(2)
		}
		return
	}
	if *showCQ {
		cqPath := consumeQueuePath(*dir, *topic, *queueID)
		if err := inspectConsumeQueue(cqPath); err != nil {
			fmt.Fprintf(os.Stderr, "error inspecting consumequeue: %v\n", err)
			os.Exit(2)
		}
		return
	}

	segs, err := listSegments(*dir, *topic, *queueID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing segments: %v\n", err)
		os.Exit(2)
	}
	if len(segs) == 0 {
		fmt.Printf("no segments found for topic %s queue %d in %s\n", *topic, *queueID, *dir)
		return
	}
	for _, s := range segs {
		if err := inspectSegment(s, *show); err != nil {
			fmt.Fprintf(os.Stderr, "error inspecting %s: %v\n", s, err)
		}
	}
}
