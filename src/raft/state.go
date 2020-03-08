package raft

import (
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setLeader(server)
	rf.lastContactLeader = time.Now()
}
func (rf *Raft) getCommitIndex() uint64 {
	return atomic.LoadUint64(&rf.commitIndex)
}

