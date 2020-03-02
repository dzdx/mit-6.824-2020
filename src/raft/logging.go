package raft

import (
	"bytes"
	"fmt"
	"sort"
	"time"
)

type logLevel int

func (l logLevel) String() string {
	switch l {
	case errorLevel:
		return "error"
	case warnLevel:
		return "warn"
	case infoLevel:
		return "info"
	case debugLevel:
		return "debug"
	case traceLevel:
		return "trace"
	}
	return "unknown"
}

func (l logLevel) color() string {
	switch l {
	case errorLevel:
		return "\033[1;31m%s\033[0m"
	case warnLevel:
		return "\033[1;33m%s\033[0m"
	case infoLevel:
		return "\033[1;34m%s\033[0m"
	case debugLevel:
		return "\033[0;36m%s\033[0m"
	case traceLevel:
		return "\033[0;37m%s\033[0m"
	}
	return "%s"
}

const colorful = true
const (
	errorLevel logLevel = iota
	warnLevel
	infoLevel
	debugLevel
	traceLevel
)

func (rf *Raft) logging(level logLevel, format string, a ...interface{}) {
	if level <= rf.logLevel {
		lastLogIndex, lastLogTerm := rf.getLastLog()
		prefix := fmt.Sprintf("%s [%5s] [id %d] [role %10s] [term %4d] [lastlogindex %4d] [lastlogterm %4d]: ", time.Now().Format("15:04:05.000000"), level, rf.me, rf.getRole(), rf.currentTerm, lastLogIndex, lastLogTerm)
		colorFormat := "%s\n"
		if colorful {
			colorFormat = level.color() + "\n"
		}
		fmt.Printf(colorFormat, prefix+fmt.Sprintf(format, a...))
	}
}

func (rf *Raft) overview() {
	rf.mu.Lock()
	buf := bytes.Buffer{}
	buf.WriteString("\n\n")
	buf.WriteString(fmt.Sprintf("server: %d, role: %s term: %d\n", rf.me, rf.role, rf.currentTerm))
	entrys := make([]LogEntry, 0, len(rf.logs))
	for _, entry := range rf.logs {
		entrys = append(entrys, entry)
	}
	rf.mu.Unlock()

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
	fmt.Print(buf.String())
}
