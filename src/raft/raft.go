package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"../labrpc"
	"../logging"
	"bytes"
	"container/list"
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid   bool
	Command        interface{}
	CommandIndex   int
	SnapshotData   []byte
	SnapshotRespCh chan error
}

type snapshotEntry struct {
	Data             []byte
	LastIncludeIndex uint64
	LastIncludeTerm  uint64
}

type SnapshotFuture struct {
	Data             []byte // snapshot (fsm dump)
	LastIncludeIndex uint64
	RespCh           chan error
}

type installSnapshotFuture struct {
	Data             []byte // snapshotEntry
	RespCh           chan error
	LastIncludeIndex uint64
	LastIncludeTerm  uint64
}

const (
	electionTimeout = 1000 * time.Millisecond
	commitTimeout   = 5 * time.Millisecond
)

type raftRole uint32

func (r raftRole) String() string {
	switch r {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	}
	return "unknown"
}

const (
	follower raftRole = iota
	candidate
	leader
)

type commitment struct {
	mutex         sync.Mutex
	matchIndexMap map[int]uint64
	commitIndex   uint64
	commitCh      chan struct{}
}

func newCommitment(peers []*labrpc.ClientEnd) *commitment {
	matchIndexMap := make(map[int]uint64, len(peers))
	for i := range peers {
		matchIndexMap[i] = 0
	}
	return &commitment{
		matchIndexMap: matchIndexMap,
		commitIndex:   0,
		commitCh:      make(chan struct{}, 1),
	}
}

func (c *commitment) setMatchIndex(server int, index uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.matchIndexMap[server] = index
	c.refreshCommitIndex()
}

func (c *commitment) refreshCommitIndex() {
	indexes := make([]uint64, 0, len(c.matchIndexMap))
	for serverID := range c.matchIndexMap {
		indexes = append(indexes, c.matchIndexMap[serverID])
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] > indexes[j]
	})
	commitIndex := indexes[(len(indexes)-1)/2]
	if commitIndex > c.commitIndex {
		c.commitIndex = commitIndex
		AsyncNotify(c.commitCh)
	}
}

type leaderState struct {
	ctx    context.Context
	cancel context.CancelFunc

	dispatchedIndex      uint64
	followerReplications map[int]*followerReplication

	startBuffer *list.List

	startCh chan struct{}

	inflightingEntries  sync.Map
	commitment          *commitment
	followersCommitting uint32
}

type followerReplication struct {
	serverID       int
	lastContact    time.Time
	nextIndex      uint64
	triggerCh      chan struct{}
	mutex          sync.Mutex
	retries        int
	inSendSnapshot bool
}

func (f *followerReplication) updateLastContract() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.lastContact = time.Now()
}
func (f *followerReplication) getLastContract() time.Time {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.lastContact
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	raftState
	leaderReadyCh chan struct{}
	leaderState   leaderState
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()

	wg                sync.WaitGroup
	role              raftRole
	lastContactLeader time.Time
	leaderID          int32
	applyCh           chan ApplyMsg
	notifyApplyCh     chan struct{}
	snapshotCh        chan *SnapshotFuture
	installSnapshotCh chan *installSnapshotFuture
	ctx               context.Context
	cancel            context.CancelFunc
	logger            *logging.Logger
	logstore          *LogStore
	snapshotMutex     sync.RWMutex

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return int(rf.getCurrentTerm()), rf.getRole() == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.getCurrentTerm())
	e.Encode(rf.votedFor)
	e.Encode(rf.votedForTerm)
	rf.logstore.encode(e)
	return w.Bytes()
}
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm uint64
	var votedFor int
	var votedForTerm uint64
	logs := make(map[uint64]LogEntry)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&votedForTerm) != nil ||
		d.Decode(&logs) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.votedForTerm = votedForTerm
		rf.logstore.logs = logs
		rf.lastLogIndex, rf.lastLogTerm = rf.logstore.lastLogMeta()
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}
func (rf *Raft) readSnapshot(data []byte) error {
	if data == nil {
		return nil
	}
	d := labgob.NewDecoder(bytes.NewBuffer(data))
	snapshotEntry := &snapshotEntry{}
	if err := d.Decode(snapshotEntry); err != nil {
		rf.logger.Errorf("decode snapshot failed: %v", err)
		return err
	}
	snapshotRespCh := make(chan error, 1)
	msg := ApplyMsg{
		SnapshotData:   snapshotEntry.Data,
		SnapshotRespCh: snapshotRespCh,
	}
	rf.applyCh <- msg
	<-snapshotRespCh
	rf.commitIndex = snapshotEntry.LastIncludeIndex
	rf.lastApplied = snapshotEntry.LastIncludeIndex
	rf.setLastSnapshotMeta(snapshotEntry.LastIncludeIndex, snapshotEntry.LastIncludeTerm)
	return nil

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
	ServerID    int
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
	LogEntries   []LogEntry
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	CommitIndex   uint64
}

func (rf *Raft) getLeader() int {
	return int(atomic.LoadInt32(&rf.leaderID))
}

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          int
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term uint64
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	defer func() {
		reply.Term = rf.getCurrentTerm()
	}()
	if args.Term < rf.getCurrentTerm() {
		return
	}
	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.setRole(follower)
	}
	future := &installSnapshotFuture{Data: args.Data, RespCh: make(chan error, 1), LastIncludeIndex: args.LastIncludedIndex, LastIncludeTerm: args.LastIncludedTerm}
	rf.installSnapshotCh <- future
	<-future.RespCh
	rf.setLastContact(args.LeaderID)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer func() {
		reply.Term = rf.getCurrentTerm()
		reply.ServerID = rf.me
	}()
	leaderID := rf.getValidLeader()
	if leaderID >= 0 && leaderID != args.CandidateID {
		rf.logger.Warnf("refused request vote from %d: (now has another leader)", args.CandidateID)
		return
	}
	if args.Term < rf.getCurrentTerm() {
		rf.logger.Warnf("refused request vote from %d: (term smaller)", args.CandidateID)
		return
	}
	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.setRole(follower)
	}
	if args.LastLogTerm < rf.lastLogTerm {
		rf.logger.Warnf("refused request vote from %d: (lastlog term smaller)", args.CandidateID)
		return
	}
	lastLogIndex, lastLogTerm := rf.getLastLogMeta()
	if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		rf.logger.Warnf("refused request vote from %d: (lastlog index smaller)", args.CandidateID)
		return
	}

	if rf.vote(args.CandidateID, args.Term) {
		reply.VoteGranted = true
	} else {
		rf.logger.Warnf("refused request vote from %d: (has voted to %d)", args.CandidateID, rf.votedFor)
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) vote(server int, term uint64) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	votedFor := -1
	if rf.votedForTerm == term {
		votedFor = rf.votedFor
	}
	if votedFor < 0 || votedFor == server {
		rf.logger.Debugf("vote for %d", server)
		rf.votedFor = server
		rf.votedForTerm = term
		rf.persist()
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer func() {
		reply.Term = rf.getCurrentTerm()
		reply.CommitIndex = rf.getCommitIndex()
	}()
	if args.Term < rf.getCurrentTerm() {
		rf.logger.Warnf("refused append entries from %d: (term smaller)", args.LeaderID)
		return
	}
	if args.Term > rf.getCurrentTerm() || rf.getRole() != follower {
		rf.setCurrentTerm(args.Term)
		rf.setRole(follower)
	}
	lastLogIndex, _ := rf.getLastLogMeta()
	if args.PrevLogIndex > 0 {
		rf.snapshotMutex.RLock()
		snapshotIndex, snapshotTerm := rf.getLastSnapshotMeta()
		if args.PrevLogIndex == snapshotIndex {
			if args.PrevLogTerm != snapshotTerm {
				rf.snapshotMutex.RUnlock()
				return
			}
			rf.snapshotMutex.RUnlock()
		} else {
			prevLog := rf.logstore.get(args.PrevLogIndex)
			rf.snapshotMutex.RUnlock()

			if prevLog == nil {
				reply.ConflictIndex = lastLogIndex + 1
				rf.logger.Warnf("refused append entries from %d: (prev log %d not existed)", args.LeaderID, args.PrevLogIndex)
				return
			}
			if args.PrevLogTerm != prevLog.Term {
				reply.ConflictIndex = rf.logstore.getFirstEntryByTerm(prevLog.Term).Index
				rf.logger.Warnf("refused append entries from %d: (prev log %d conflict)", args.LeaderID, args.PrevLogIndex)
				return
			}
		}
	}
	if len(args.LogEntries) > 0 {
		deleteFromIndex := lastLogIndex + 1
		for _, newLogEntry := range args.LogEntries {
			oldLogEntry := rf.logstore.get(newLogEntry.Index)
			if oldLogEntry == nil || newLogEntry.Term != oldLogEntry.Term {
				deleteFromIndex = newLogEntry.Index
				break
			}
		}
		if lastLogIndex >= deleteFromIndex {
			rf.logger.Debugf("delete log entry [%d ... %d]", deleteFromIndex, lastLogIndex)
		}
		rf.logstore.deleteLogEntriesRange(deleteFromIndex, lastLogIndex)
		for _, newLogEntry := range args.LogEntries {
			if newLogEntry.Index >= deleteFromIndex {
				rf.logger.Debugf("append log entry %d %#v", newLogEntry.Index, newLogEntry.Command)
				rf.logstore.put(&newLogEntry)
				rf.setLastLogMeta(newLogEntry.Index, newLogEntry.Term)
				rf.persist()
			}
		}
	}
	if args.LeaderCommit > 0 {
		lastLogIndex, _ := rf.getLastLogMeta()
		rf.commitTo(MinUint64(lastLogIndex, args.LeaderCommit))
	}
	reply.Success = true
	rf.setLastContact(args.LeaderID)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.getRole() != leader {
		return -1, -1, false
	}
	<-rf.leaderReadyCh
	rf.mu.Lock()
	rf.leaderState.dispatchedIndex++
	logEntry := &LogEntry{
		Term:    rf.getCurrentTerm(),
		Index:   rf.leaderState.dispatchedIndex,
		Command: command,
		Type:    commandType,
	}
	rf.leaderState.startBuffer.PushBack(logEntry)
	rf.mu.Unlock()
	rf.logger.Debugf("dispatch log entry %d", logEntry.Index)

	AsyncNotify(rf.leaderState.startCh)
	// Your code here (2B).

	return int(logEntry.Index), int(logEntry.Term), true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.cancel()
	rf.logger.Infof("Raft exit")
	rf.wg.Wait()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) goFunc(f func()) {
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		f()
	}()
}

func (rf *Raft) getLastContactLeader() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastContactLeader
}

func (rf *Raft) runFollower() {
	for rf.getRole() == follower {
		select {
		case <-rf.ctx.Done():
			return
		case <-time.After(randomDuration(electionTimeout)):
			if time.Since(rf.getLastContactLeader()) > electionTimeout {
				rf.setRole(candidate)
				return
			}
		}
	}
}

func (rf *Raft) electionLeader() chan *RequestVoteReply {
	voteChan := make(chan *RequestVoteReply, len(rf.peers))
	for i := range rf.peers {
		server := i
		go func() {
			if server == rf.me {
				rf.vote(rf.me, rf.getCurrentTerm())
				voteChan <- &RequestVoteReply{
					Term:        rf.getCurrentTerm(),
					VoteGranted: true,
					ServerID:    rf.me,
				}
			} else {
				lastLogIndex, lastLogTerm := rf.getLastLogMeta()
				req := &RequestVoteArgs{
					Term:         rf.getCurrentTerm(),
					CandidateID:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := &RequestVoteReply{}
				rf.sendRequestVote(server, req, reply)
				voteChan <- reply
			}
		}()
	}
	return voteChan
}

func (rf *Raft) quorumSize() int {
	return len(rf.peers)/2 + 1
}
func (rf *Raft) getValidLeader() int {
	leaderID := rf.getLeader()
	if leaderID < 0 {
		return leaderID
	}
	if rf.role == leader {
		return rf.me
	}
	if time.Since(rf.lastContactLeader) < electionTimeout {
		return leaderID
	}
	return -1
}

func (rf *Raft) runCandidate() {
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)

	timer := time.NewTimer(randomDuration(electionTimeout))
	defer timer.Stop()
	quorumSize := rf.quorumSize()
	voteChan := rf.electionLeader()
	votes := 0
	for {
		select {
		case <-timer.C:
			return
		case reply := <-voteChan:
			if reply.Term > rf.getCurrentTerm() {
				rf.logger.Warnf("candidate switch to follower: (peer %d term %d)", reply.ServerID, reply.Term)
				rf.setRole(follower)
				rf.setCurrentTerm(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes >= quorumSize {
					rf.leaderReadyCh = make(chan struct{})
					rf.setRole(leader)
					return
				}
			}
		}
	}
}

func (rf *Raft) runHeartbeat(f *followerReplication) {
	ticker := time.NewTicker(electionTimeout / 10)
	defer ticker.Stop()
	heartbeat := func() {
		args := &AppendEntriesArgs{
			Term:     rf.getCurrentTerm(),
			LeaderID: rf.me,
		}
		reply := &AppendEntriesReply{}
		before := time.Now()
		if rf.sendAppendEntries(f.serverID, args, reply) {
			if reply.Term > rf.getCurrentTerm() {
				rf.logger.Warnf("leader switch to follower: (peer %d term %d)", f.serverID, reply.Term)
				rf.setCurrentTerm(reply.Term)
				rf.setRole(follower)
				rf.leaderState.cancel()
				return
			}
			if reply.Success {
				f.updateLastContract()
			}
		}
		rf.logger.Tracef("heartbeat to %d %s %v %v", f.serverID, before.Format("15:04:05.000000"), time.Since(before), reply.Success)
	}
	go heartbeat()
	for rf.getRole() == leader {
		select {
		case <-rf.leaderState.ctx.Done():
			return
		case <-ticker.C:
			go heartbeat()
		}
	}
}

func (rf *Raft) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&rf.currentTerm)
}
func (rf *Raft) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&rf.currentTerm, term)
	rf.persist()
}

func (rf *Raft) replicateSnapshot(f *followerReplication) bool {
	index, term := rf.getLastSnapshotMeta()
	data := rf.persister.ReadSnapshot()
	args := &InstallSnapshotArgs{
		Term:              rf.getCurrentTerm(),
		LeaderID:          rf.me,
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Data:              data,
	}
	rf.logger.Debugf("replicate snapshot to %d [...%d]", f.serverID, index)
	reply := &InstallSnapshotReply{}
	if rf.sendInstallSnapshot(f.serverID, args, reply) {
		f.retries = 0
		if reply.Term > rf.getCurrentTerm() {
			rf.logger.Warnf("leader switch to follower: (peer %d term %d)", f.serverID, reply.Term)
			rf.setCurrentTerm(reply.Term)
			rf.setRole(follower)
			rf.leaderState.cancel()
			return false
		}
		f.nextIndex = index + 1
		rf.leaderState.commitment.setMatchIndex(f.serverID, index)
		f.inSendSnapshot = false
		return true
	} else {
		f.retries++
		select {
		case <-time.After(backoffDuration(10*time.Millisecond, f.retries)):
		case <-rf.leaderState.ctx.Done():
		}
		return false
	}
}
func (rf *Raft) getTermByIndex(index uint64) uint64 {
	snapshotIndex, snapshotTerm := rf.getLastSnapshotMeta()
	if index == snapshotIndex {
		return snapshotTerm
	} else {
		entry := rf.logstore.get(index)
		if entry == nil {
			rf.logger.Errorf("log entry %d not existed", index)
		}
		return entry.Term
	}
}

func (rf *Raft) replicateLogEntries(f *followerReplication) bool {
	entries := make([]LogEntry, 0)
	var prevLogTerm uint64
	var prevLogIndex uint64
	prevLogIndex = f.nextIndex - 1
	lastLogIndex, _ := rf.getLastLogMeta()
	if lastLogIndex >= f.nextIndex {
		rf.logger.Debugf("replicate to %d [%d ... %d]", f.serverID, f.nextIndex, lastLogIndex)
	} else {
		rf.logger.Debugf("replicate to %d []", f.serverID)
	}

	rf.snapshotMutex.RLock()
	for nextIndex := f.nextIndex; nextIndex <= lastLogIndex; nextIndex++ {
		entry := rf.logstore.get(nextIndex)
		if entry == nil {
			rf.logger.Debugf("log entry %d not existed, change to send snapshot", nextIndex)
			f.inSendSnapshot = true
			rf.snapshotMutex.RUnlock()
			return false
		}
		entries = append(entries, *entry)
	}

	if prevLogIndex > 0 {
		prevLogTerm = rf.getTermByIndex(prevLogIndex)
	}
	rf.snapshotMutex.RUnlock()

	args := &AppendEntriesArgs{
		Term:         rf.getCurrentTerm(),
		LeaderID:     rf.me,
		LogEntries:   entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.getCommitIndex(),
	}
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(f.serverID, args, reply) {
		f.retries = 0
		if reply.Term > rf.getCurrentTerm() {
			rf.logger.Warnf("leader switch to follower: (peer %d term %d)", f.serverID, reply.Term)
			rf.setCurrentTerm(reply.Term)
			rf.setRole(follower)
			rf.leaderState.cancel()
			return false
		}
		if reply.Success {
			if len(args.LogEntries) > 0 {
				logIndex := args.LogEntries[len(args.LogEntries)-1].Index
				f.nextIndex = logIndex + 1
				rf.leaderState.commitment.setMatchIndex(f.serverID, logIndex)
			}
			return true
		} else {
			rf.logger.Warnf("follower %d conflict log at %d", f.serverID, reply.ConflictIndex)
			if reply.ConflictIndex > 0 {
				f.nextIndex = MinUint64(f.nextIndex-1, reply.ConflictIndex)
			} else {
				f.nextIndex = f.nextIndex - 1
			}
			return false
		}
	} else {
		f.retries++
		select {
		case <-time.After(backoffDuration(10*time.Millisecond, f.retries)):
		case <-rf.leaderState.ctx.Done():
		}
		return false
	}
}

func (rf *Raft) replicateOnce(f *followerReplication) {
	f.retries = 0
	for rf.getRole() == leader {
		select {
		case <-rf.leaderState.ctx.Done():
			return
		default:
		}
		if f.inSendSnapshot {
			if rf.replicateSnapshot(f) {
				return
			}
		} else {
			if rf.replicateLogEntries(f) {
				return
			}
		}
	}
}
func (rf *Raft) runReplicate(f *followerReplication) {
	for rf.getRole() == leader {
		select {
		case <-rf.ctx.Done():
			return
		default:
		}
		select {
		case <-f.triggerCh:
			rf.replicateOnce(f)
		case <-rf.leaderState.ctx.Done():
			return
		}
	}
}

func (rf *Raft) checkLease() {
	now := time.Now()
	count := 0
	for _, fs := range rf.leaderState.followerReplications {
		if now.Sub(fs.getLastContract()) < electionTimeout {
			count++
		}
	}
	if count+1 < rf.quorumSize() {
		rf.logger.Warnf("leader lease expire")
		rf.setRole(follower)
		rf.leaderState.cancel()
		return
	}
}

func (rf *Raft) dispatch() {
	rf.mu.Lock()
	buf := rf.leaderState.startBuffer
	rf.leaderState.startBuffer = list.New()
	rf.mu.Unlock()

	for elem := buf.Front(); elem != nil; elem = elem.Next() {
		logEntry := elem.Value.(*LogEntry)
		rf.leaderState.inflightingEntries.Store(logEntry.Index, logEntry)
		rf.logger.Debugf("append log entry %d %#v", logEntry.Index, logEntry.Command)
		rf.logstore.put(logEntry)
		rf.setLastLogMeta(logEntry.Index, logEntry.Term)
		rf.persist()
		rf.leaderState.commitment.setMatchIndex(rf.me, logEntry.Index)
	}
	for _, f := range rf.leaderState.followerReplications {
		AsyncNotify(f.triggerCh)
	}
}

func (rf *Raft) runLeader() {
	rf.setLeader(rf.me)
	rf.leaderState.followerReplications = make(map[int]*followerReplication, len(rf.peers))
	rf.leaderState.ctx, rf.leaderState.cancel = context.WithCancel(rf.ctx)
	rf.leaderState.startBuffer = list.New()
	rf.leaderState.startCh = make(chan struct{}, 1)
	rf.leaderState.commitment = newCommitment(rf.peers)
	rf.leaderState.inflightingEntries = sync.Map{}
	lastLogIndex, _ := rf.getLastLogMeta()
	rf.leaderState.dispatchedIndex = lastLogIndex

	for server := range rf.peers {
		if server != rf.me {
			f := &followerReplication{
				serverID:  server,
				triggerCh: make(chan struct{}, 1),
				nextIndex: lastLogIndex + 1,
			}
			rf.leaderState.followerReplications[server] = f
			go rf.runHeartbeat(f)
			go rf.runReplicate(f)
		}
	}

	close(rf.leaderReadyCh)

	defer func() {
		rf.leaderState.cancel()
		rf.leaderReadyCh = make(chan struct{})
		rf.logger.Warnf("exit leader")
	}()
	leaderLeaseTimer := time.NewTimer(electionTimeout)
	defer leaderLeaseTimer.Stop()

	for rf.getRole() == leader {
		select {
		case <-rf.leaderState.ctx.Done():
			return
		case <-leaderLeaseTimer.C:
			leaderLeaseTimer.Reset(electionTimeout)
			rf.checkLease()
		case <-rf.leaderState.startCh:
			rf.dispatch()
		case <-rf.leaderState.commitment.commitCh:
			rf.leaderCommit(rf.leaderState.commitment.commitIndex)
		}
	}
}

func (rf *Raft) leaderCommit(index uint64) {
	rf.snapshotMutex.RLock()
	term := rf.getTermByIndex(index)
	rf.snapshotMutex.RUnlock()
	if term == rf.getCurrentTerm() {
		rf.commitTo(index)
		go func() {
			if atomic.SwapUint32(&rf.leaderState.followersCommitting, 1) == 1 {
				return
			}
			select {
			case <-rf.leaderState.ctx.Done():
				return
			case <-time.After(commitTimeout):
			}
			atomic.StoreUint32(&rf.leaderState.followersCommitting, 0)
			for _, f := range rf.leaderState.followerReplications {
				AsyncNotify(f.triggerCh)
			}
		}()
	}
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.ctx.Done():
			// fast restart election leader
			rf.setLeader(-1)
			return
		default:
		}
		rf.logger.Infof("switch role")
		switch rf.getRole() {
		case follower:
			rf.runFollower()
		case candidate:
			rf.runCandidate()
		case leader:
			rf.runLeader()
		}
	}
}
func (rf *Raft) commitTo(index uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.getCommitIndex() {
		atomic.StoreUint64(&rf.commitIndex, index)
		AsyncNotify(rf.notifyApplyCh)
	}
}

func (rf *Raft) StartSnapshot(future *SnapshotFuture) {
	rf.snapshotCh <- future
}

func (rf *Raft) takeSnapshot(future *SnapshotFuture) {
	logEntry := rf.logstore.get(future.LastIncludeIndex)
	if logEntry == nil {
		return
	}
	rf.logger.Infof("start take snapshot to %d", logEntry.Index)
	snapshotEntry := &snapshotEntry{
		Data:             future.Data,
		LastIncludeIndex: logEntry.Index,
		LastIncludeTerm:  logEntry.Term,
	}
	var buf bytes.Buffer
	e := labgob.NewEncoder(&buf)
	e.Encode(snapshotEntry)
	rf.snapshotMutex.Lock()
	rf.logstore.deleteLogEntriesRange(0, logEntry.Index)
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), buf.Bytes())
	rf.setLastSnapshotMeta(snapshotEntry.LastIncludeIndex, snapshotEntry.LastIncludeTerm)
	rf.snapshotMutex.Unlock()
	rf.logger.Infof("end take snapshot to %d", logEntry.Index)
	future.RespCh <- nil
}

func (rf *Raft) installSnapshot(future *installSnapshotFuture) {
	logEntry := rf.logstore.get(future.LastIncludeIndex)
	if logEntry != nil && logEntry.Term == future.LastIncludeTerm {
		rf.snapshotMutex.Lock()
		rf.logstore.deleteLogEntriesRange(0, future.LastIncludeIndex)
		rf.persister.SaveStateAndSnapshot(rf.encodeState(), future.Data)
		rf.snapshotMutex.Unlock()
		future.RespCh <- nil
		return
	}
	d := labgob.NewDecoder(bytes.NewBuffer(future.Data))
	snapshotEntry := &snapshotEntry{}
	d.Decode(snapshotEntry)
	snapshotRespCh := make(chan error, 1)
	msg := ApplyMsg{
		SnapshotData:   snapshotEntry.Data,
		SnapshotRespCh: snapshotRespCh,
	}
	select {
	case rf.applyCh <- msg:
	case <-rf.ctx.Done():
		return
	}
	<-snapshotRespCh
	rf.snapshotMutex.Lock()
	rf.logstore.empty(func() {
		rf.setLastLogMeta(future.LastIncludeIndex, future.LastIncludeTerm)
		rf.setLastSnapshotMeta(future.LastIncludeIndex, future.LastIncludeTerm)
		rf.lastApplied = future.LastIncludeIndex
		rf.commitIndex = future.LastIncludeIndex
	})
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), future.Data)
	rf.snapshotMutex.Unlock()
	future.RespCh <- nil
}

func (rf *Raft) runApplyMsg() {
	for {
		select {
		case <-rf.ctx.Done():
			return
		case future := <-rf.snapshotCh:
			rf.takeSnapshot(future)
		case future := <-rf.installSnapshotCh:
			rf.installSnapshot(future)
		case <-rf.notifyApplyCh:
			commitIndex := rf.getCommitIndex()
			if rf.lastApplied < commitIndex {
				rf.logger.Debugf("apply [%d ... %d]", rf.lastApplied+1, commitIndex)
			}
			for rf.lastApplied < commitIndex {
				logEntry := rf.logstore.get(rf.lastApplied + 1)
				if logEntry.Type == commandType {
					msg := ApplyMsg{
						CommandValid: true,
						Command:      logEntry.Command,
						CommandIndex: int(logEntry.Index),
					}
					select {
					case rf.applyCh <- msg:
					case <-rf.ctx.Done():
						return
					}
					//time.Sleep(10 * time.Millisecond)
				}
				rf.leaderState.inflightingEntries.Delete(logEntry.Index)
				rf.lastApplied++
			}
		}
	}
}
func (rf *Raft) overview() {
	buf := bytes.Buffer{}
	buf.WriteString("\n\n")
	buf.WriteString(fmt.Sprintf("server: %d, role: %s term: %d\n", rf.me, rf.role, rf.currentTerm))
	fmt.Print(buf.String())
	fmt.Print(rf.logstore.overview())
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	labgob.Register(snapshotEntry{})
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = follower
	rf.votedFor = -1
	rf.leaderID = -1
	rf.applyCh = applyCh
	rf.logstore = newLogStore()
	rf.notifyApplyCh = make(chan struct{}, 1)
	rf.snapshotCh = make(chan *SnapshotFuture, 1)
	rf.installSnapshotCh = make(chan *installSnapshotFuture)
	rf.ctx, rf.cancel = context.WithCancel(context.Background())
	rf.logger = logging.NewLogger(logging.WarnLevel, true, func() string {
		lastLogIndex, lastLogTerm := rf.getLastLogMeta()
		return fmt.Sprintf("[Raft] [id %d] [role %10s] [term %4d] [lastlogindex %4d] [lastlogterm %4d]", rf.me, rf.getRole(), rf.currentTerm, lastLogIndex, lastLogTerm)
	})

	// Your initialization code here (2A, 2B, 2C).

	rf.goFunc(rf.runApplyMsg)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if err := rf.readSnapshot(persister.ReadSnapshot()); err != nil {
		os.Exit(-1)
	}
	rf.goFunc(rf.run)
	return rf
}
