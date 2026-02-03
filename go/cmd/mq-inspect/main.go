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
	"strings"
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

type DelayedMessage struct {
	Message   Message
	Topic     string
	QueueID   int
	ExecuteAt time.Time
}

const cqEntrySize = 20
const SystemDelayTopic = "__DELAY_TOPIC__"

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

func inspectDelayMessages(dir string) error {
	// Read from system delay topic
	segs, err := listSegments(dir, SystemDelayTopic, 0)
	if err != nil {
		return fmt.Errorf("failed to list delay topic segments: %v", err)
	}
	if len(segs) == 0 {
		fmt.Println("No delayed messages found")
		return nil
	}

	// Read the latest segment (contains the current state)
	latestSeg := segs[len(segs)-1]
	f, err := os.Open(latestSeg)
	if err != nil {
		return fmt.Errorf("failed to open segment: %v", err)
	}
	defer f.Close()

	fmt.Printf("Delay Messages (from %s):\n", SystemDelayTopic)
	fmt.Println(strings.Repeat("=", 80))

	// Read and validate WAL header
	var hdr [8]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}
	if string(hdr[0:4]) != "MQW1" {
		return fmt.Errorf("invalid wal magic")
	}

	// Read all messages and find the last one with __DELAY_META__ tag
	var lastMetaPayload []byte
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			break
		}
		total := binary.BigEndian.Uint32(lenBuf[:])
		payload := make([]byte, total)
		if _, err := io.ReadFull(f, payload); err != nil {
			break
		}

		// Parse message to check tag
		off := 4 + 16 + 2 + 8 // skip crc + uuid + retry + ts
		if off+2 > len(payload) {
			continue
		}
		tagLen := int(binary.BigEndian.Uint16(payload[off : off+2]))
		off += 2
		if off+tagLen > len(payload) {
			continue
		}
		tag := string(payload[off : off+tagLen])
		
		if tag == "__DELAY_META__" {
			lastMetaPayload = payload
		}
	}

	if lastMetaPayload == nil {
		fmt.Println("No delay meta message found")
		return nil
	}

	// Parse the meta message to extract delayed messages
	off := 4 + 16 + 2 + 8 // skip crc + uuid + retry + ts
	tagLen := int(binary.BigEndian.Uint16(lastMetaPayload[off : off+2]))
	off += 2 + tagLen
	bodyLen := int(binary.BigEndian.Uint32(lastMetaPayload[off : off+4]))
	off += 4
	if off+bodyLen > len(lastMetaPayload) {
		return fmt.Errorf("invalid body length")
	}
	body := lastMetaPayload[off : off+bodyLen]

	// Parse delay messages from body
	if len(body) < 4 {
		fmt.Println("No delayed messages")
		return nil
	}

	count := binary.BigEndian.Uint32(body[0:4])
	offset := 4

	fmt.Printf("Total Delayed Messages: %d\n", count)
	fmt.Println(strings.Repeat("-", 80))

	for i := uint32(0); i < count; i++ {
		// ExecuteAt
		if offset+8 > len(body) {
			break
		}
		executeAt := time.Unix(int64(binary.BigEndian.Uint64(body[offset:offset+8])), 0)
		offset += 8

		// Topic
		if offset+2 > len(body) {
			break
		}
		topicLen := binary.BigEndian.Uint16(body[offset : offset+2])
		offset += 2
		if offset+int(topicLen) > len(body) {
			break
		}
		topic := string(body[offset : offset+int(topicLen)])
		offset += int(topicLen)

		// QueueID
		if offset+4 > len(body) {
			break
		}
		queueID := int(binary.BigEndian.Uint32(body[offset : offset+4]))
		offset += 4

		// Message: [id_len:2][id][retry:2][ts:8][tag_len:2][tag][body_len:4][body]
		if offset+2 > len(body) {
			break
		}
		msgIDLen := binary.BigEndian.Uint16(body[offset : offset+2])
		offset += 2
		if offset+int(msgIDLen) > len(body) {
			break
		}
		msgID := string(body[offset : offset+int(msgIDLen)])
		offset += int(msgIDLen)

		if offset+2 > len(body) {
			break
		}
		retry := binary.BigEndian.Uint16(body[offset : offset+2])
		offset += 2

		if offset+8 > len(body) {
			break
		}
		ts := time.Unix(int64(binary.BigEndian.Uint64(body[offset:offset+8])), 0)
		offset += 8

		if offset+2 > len(body) {
			break
		}
		msgTagLen := binary.BigEndian.Uint16(body[offset : offset+2])
		offset += 2
		if offset+int(msgTagLen) > len(body) {
			break
		}
		msgTag := string(body[offset : offset+int(msgTagLen)])
		offset += int(msgTagLen)

		if offset+4 > len(body) {
			break
		}
		msgBodyLen := binary.BigEndian.Uint32(body[offset : offset+4])
		offset += 4
		if offset+int(msgBodyLen) > len(body) {
			break
		}
		msgBody := string(body[offset : offset+int(msgBodyLen)])
		offset += int(msgBodyLen)

		// Calculate time until execution
		now := time.Now()
		remaining := executeAt.Sub(now)
		status := "⏰ Pending"
		if remaining < 0 {
			status = "✅ Ready"
			remaining = -remaining
		}

		fmt.Printf("\n[%d] %s\n", i+1, status)
		fmt.Printf("  Message ID:   %s\n", msgID)
		fmt.Printf("  Target Topic: %s (Queue %d)\n", topic, queueID)
		fmt.Printf("  Execute At:   %s\n", executeAt.Format(time.RFC3339))
		if remaining > 0 {
			if status == "⏰ Pending" {
				fmt.Printf("  Remaining:    %s\n", formatDuration(remaining))
			} else {
				fmt.Printf("  Overdue:      %s\n", formatDuration(remaining))
			}
		}
		fmt.Printf("  Tag:          %s\n", msgTag)
		fmt.Printf("  Retry:        %d\n", retry)
		fmt.Printf("  Created:      %s\n", ts.Format(time.RFC3339))
		fmt.Printf("  Body:         %s\n", truncateString(msgBody, 100))
	}

	fmt.Println(strings.Repeat("=", 80))
	return nil
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	} else if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	} else if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func main() {
	dir := flag.String("dir", "./testdata", "base data directory")
	topic := flag.String("topic", "test", "topic to inspect")
	queueID := flag.Int("queue", 0, "queue id")
	show := flag.Bool("show", false, "show parsed payloads")
	showCQ := flag.Bool("cq", false, "inspect consumequeue")
	showOffsets := flag.Bool("offsets", false, "inspect consumer offsets")
	showDelay := flag.Bool("delay", false, "inspect delayed messages")
	group := flag.String("group", "", "consumer group (for offsets)")
	flag.Parse()

	if *showDelay {
		if err := inspectDelayMessages(*dir); err != nil {
			fmt.Fprintf(os.Stderr, "error inspecting delay messages: %v\n", err)
			os.Exit(2)
		}
		return
	}

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
