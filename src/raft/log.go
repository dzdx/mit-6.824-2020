package raft

import (
	"../labgob"
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync"
)

type entryType int

const (
	commandType entryType = iota
)

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command interface{}
	Type    entryType
}

type LogStore struct {
	mutex sync.Mutex
	logs  map[uint64]LogEntry
}

func (s *LogStore) empty(inner func()) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.logs = make(map[uint64]LogEntry)
	inner()
}

func (s *LogStore) deleteLogEntriesRange(start, end uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	min := uint64(math.MaxUint64)
	max := uint64(0)
	for i := range s.logs {
		if min > i {
			min = i
		}
		if max < i {
			max = i
		}
	}
	if start < min {
		start = min
	}
	if end > max {
		end = max
	}
	for i := start; i <= end; i++ {
		delete(s.logs, i)
	}
}

func (s *LogStore) encode(enc *labgob.LabEncoder) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	enc.Encode(s.logs)
}

func (s *LogStore) getFirstEntryByTerm(term uint64) *LogEntry {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var ret *LogEntry
	for i := range s.logs {
		entry := s.logs[i]
		if entry.Term == term {
			if ret == nil {
				ret = &entry
			} else if entry.Index < ret.Index {
				ret = &entry
			}
		}
	}
	return ret
}
func (s *LogStore) get(index uint64) *LogEntry {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if l, ok := s.logs[index]; ok {
		return &l
	}
	return nil
}

func (s *LogStore) put(logEntry *LogEntry) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.logs[logEntry.Index] = *logEntry
}
func (s *LogStore) lastLogMeta() (uint64, uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var maxIndex uint64
	for k := range s.logs {
		if k > maxIndex {
			maxIndex = k
		}
	}
	if lastLog, ok := s.logs[maxIndex]; ok {
		return lastLog.Index, lastLog.Term
	}
	return 0, 0
}
func (s *LogStore) overview() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	buf := bytes.Buffer{}
	entrys := make([]LogEntry, 0, len(s.logs))
	for _, entry := range s.logs {
		entrys = append(entrys, entry)
	}
	sort.Slice(entrys, func(i, j int) bool {
		return entrys[i].Index < entrys[j].Index
	})
	indexBuf := bytes.Buffer{}
	termBuf := bytes.Buffer{}
	valueBuf := bytes.Buffer{}
	indexBuf.WriteString("index: ")
	termBuf.WriteString("term:  ")
	valueBuf.WriteString("value: ")
	for _, entry := range entrys {
		indexBuf.WriteString(fmt.Sprintf("%5d", entry.Index))
		termBuf.WriteString(fmt.Sprintf("%5d", entry.Term))
		valueBuf.WriteString(fmt.Sprintf("%5v", entry.Command))
	}
	buf.Write(indexBuf.Bytes())
	buf.WriteString("\n")
	buf.Write(termBuf.Bytes())
	buf.WriteString("\n")
	buf.Write(valueBuf.Bytes())
	buf.WriteString("\n")
	return buf.String()
}

func newLogStore() *LogStore {
	return &LogStore{
		logs: make(map[uint64]LogEntry),
	}
}
