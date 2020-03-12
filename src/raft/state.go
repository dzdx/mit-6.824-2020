package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

type raftState struct {
	currentTerm uint64

	votedFor     int
	votedForTerm uint64

	commitIndex  uint64
	lastApplied  uint64
	lastLogIndex uint64
	lastLogTerm  uint64

	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
	stateMutex        sync.Mutex
}

func (rf *Raft) getRole() raftRole {
	roleAddr := (*uint32)(&rf.role)
	return raftRole(atomic.LoadUint32(roleAddr))
}
func (rf *Raft) setLeader(leader int) {
	atomic.StoreInt32(&rf.leaderID, int32(leader))
}

func (rf *Raft) setRole(role raftRole) {
	oldRole := rf.getRole()
	if oldRole != role {
		roleAddr := (*uint32)(&rf.role)
		atomic.StoreUint32(roleAddr, uint32(role))
	}
}
func (rf *Raft) setLastContact(server int) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	rf.setLeader(server)
	rf.lastContactLeader = time.Now()
}
func (rf *Raft) getCommitIndex() uint64 {
	return atomic.LoadUint64(&rf.commitIndex)
}
func (rf *Raft) setLastLogMeta(index, term uint64) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	rf.lastLogIndex = index
	rf.lastLogTerm = term
}

func (rf *Raft) getLastLogMeta() (uint64, uint64) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	return rf.lastLogIndex, rf.lastLogTerm
}

func (rf *Raft) setLastSnapshotMeta(index, term uint64) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = term
}

func (rf *Raft) getLastSnapshotMeta() (uint64, uint64) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	return rf.lastSnapshotIndex, rf.lastSnapshotTerm
}
