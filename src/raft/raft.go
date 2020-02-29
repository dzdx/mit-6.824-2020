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
	"../labrpc"
	"context"
	"fmt"
	"math/rand"
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
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

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

const (
	errorLevel logLevel = iota
	warnLevel
	infoLevel
	debugLevel
	traceLevel
)

const (
	electionTimeout = 1000 * time.Millisecond
	commitTimeout   = 10 * time.Millisecond
)

type raftRole int

func (r raftRole) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

const (
	Follower raftRole = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command interface{}
}

type leaderState struct {
	ctx    context.Context
	cancel context.CancelFunc

	dispatchedIndex      uint64
	leaderReady          chan struct{}
	followerReplications map[int]*followerReplication

	startBuffer []LogEntry
	startCh     chan struct{}

	inflightingEntries sync.Map

	matchIndexMap map[int]uint64
}

type raftState struct {
	currentTerm uint64

	votedFor     int
	votedForTerm uint64

	commitIndex  uint64
	lastApplied  uint64
	lastLogIndex uint64
	lastLogTerm  uint64
}

func (rf *Raft) setLastLog(index, term uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastLogIndex = index
	rf.lastLogTerm = term
}

type followerReplication struct {
	serverID    int
	lastContact time.Time
	nextIndex   uint64
	triggerCh   chan struct{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	raftState
	leaderState leaderState
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()

	wg                sync.WaitGroup
	role              raftRole
	lastContactLeader time.Time
	leaderID          int
	applyCh           chan ApplyMsg
	commitCh          chan struct{}

	logs     map[uint64]LogEntry
	ctx      context.Context
	cancel   context.CancelFunc
	logLevel logLevel

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return int(rf.currentTerm), rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
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
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer func() {
		reply.Term = rf.currentTerm
	}()
	if rf.leaderID != args.CandidateID && time.Since(rf.lastContactLeader) < electionTimeout {
		rf.logging(debugLevel, "refused request vote from %d: (now has another leader)", args.CandidateID)
		return
	}
	if args.Term < rf.currentTerm {
		rf.logging(debugLevel, "refused request vote from %d: (term smaller)", args.CandidateID)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
	}
	if args.LastLogTerm < rf.lastLogTerm {
		rf.logging(debugLevel, "refused request vote from %d: (lastlog term smaller)", args.CandidateID)
		return
	}
	if args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex < rf.lastLogIndex {
		rf.logging(debugLevel, "refused request vote from %d: (lastlog index smaller)", args.CandidateID)
		return
	}

	if rf.vote(args.CandidateID) {
		reply.VoteGranted = true
	} else {
		rf.logging(debugLevel, "refused request vote from %d: (has voted to %d)", args.CandidateID, rf.votedFor)
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) vote(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	votedFor := -1
	currentTerm := rf.currentTerm
	if rf.votedForTerm == currentTerm {
		votedFor = rf.votedFor
	}
	if votedFor < 0 || votedFor == server {
		rf.logging(debugLevel, "vote for %d", server)
		rf.votedFor = server
		rf.votedForTerm = currentTerm
		return true
	}
	return false
}

func (rf *Raft) setLastContact(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leaderID = server
	rf.lastContactLeader = time.Now()
}

func (rf *Raft) deleteLogEntriesRange(start, end uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := start; i < end; i++ {
		delete(rf.logs, i)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer func() {
		reply.Term = rf.currentTerm
	}()
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.mu.Unlock()
	}
	if args.PrevLogIndex > 0 {
		prevLog := rf.GetLogEntry(args.PrevLogIndex)
		if prevLog == nil {
			reply.ConflictIndex = rf.lastLogIndex + 1
			return
		}
		if args.PrevLogTerm != prevLog.Term {
			reply.ConflictIndex = prevLog.Index
			return
		}
	}
	if len(args.LogEntries) > 0 {
		deleteFromIndex := rf.lastLogIndex + 1
		for _, newLogEntry := range args.LogEntries {
			oldLogEntry := rf.GetLogEntry(newLogEntry.Index)
			if oldLogEntry == nil || newLogEntry.Term != oldLogEntry.Term {
				deleteFromIndex = newLogEntry.Index
				break
			}
		}
		rf.deleteLogEntriesRange(deleteFromIndex, rf.lastLogIndex+1)
		if rf.lastLogIndex >= deleteFromIndex {
			rf.logging(debugLevel, "delete log entry [%d ... %d]", deleteFromIndex, rf.lastLogIndex)
		}
		for _, newLogEntry := range args.LogEntries {
			if newLogEntry.Index >= deleteFromIndex {
				rf.putLogEntry(&newLogEntry)
				rf.setLastLog(newLogEntry.Index, newLogEntry.Term)
			}
		}
	}
	if args.LeaderCommit > 0 {
		rf.setCommitIndex(MinUint64(rf.lastLogIndex, args.LeaderCommit))
	}
	reply.Success = true
	rf.setLastContact(args.LeaderID)
	rf.role = Follower
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
	if rf.role != Leader {
		return -1, -1, false
	}
	<-rf.leaderState.leaderReady
	rf.mu.Lock()
	rf.leaderState.dispatchedIndex++
	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Index:   rf.leaderState.dispatchedIndex,
		Command: command,
	}
	rf.leaderState.startBuffer = append(rf.leaderState.startBuffer, logEntry)
	rf.logging(debugLevel, "dispatch log entry %d", logEntry.Index)
	rf.mu.Unlock()

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
func randomDuration(base time.Duration) time.Duration {
	return time.Duration(rand.Float64()*float64(base)) + base
}
func (rf *Raft) runFollower() {
	for rf.role == Follower {
		select {
		case <-rf.ctx.Done():
			return
		case <-time.After(randomDuration(electionTimeout)):
			if time.Since(rf.lastContactLeader) > electionTimeout {
				rf.role = Candidate
				return
			}
		}
	}
}

func (rf *Raft) electionLeader() chan *RequestVoteReply {
	voteChan := make(chan *RequestVoteReply, len(rf.peers))
	for i := range rf.peers {
		server := i
		rf.goFunc(func() {
			if server == rf.me {
				rf.vote(rf.me)
				voteChan <- &RequestVoteReply{
					Term:        rf.currentTerm,
					VoteGranted: true,
				}
			} else {
				req := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: rf.lastLogIndex,
					LastLogTerm:  rf.lastLogTerm,
				}
				reply := &RequestVoteReply{}
				rf.sendRequestVote(server, req, reply)
				voteChan <- reply
			}
		})
	}
	return voteChan
}

func (rf *Raft) quorumSize() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) runCandidate() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.mu.Unlock()

	timer := time.NewTimer(randomDuration(electionTimeout))
	quorumSize := rf.quorumSize()
	voteChan := rf.electionLeader()
	votes := 0
	for {
		select {
		case <-timer.C:
			return
		case reply := <-voteChan:
			if reply.VoteGranted {
				votes++
				if votes >= quorumSize {
					rf.role = Leader
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
			Term:     rf.currentTerm,
			LeaderID: rf.me,
		}
		reply := &AppendEntriesReply{}
		before := time.Now()
		if rf.sendAppendEntries(f.serverID, args, reply) {
			if reply.Term > rf.currentTerm {
				rf.logging(debugLevel, "follower %d term %d > leader term", f.serverID, reply.Term)
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.leaderState.cancel()
			}
			if reply.Success {
				f.lastContact = time.Now()
			}
		}
		rf.logging(traceLevel, "heartbeat to %d %s %v %v", f.serverID, before.Format("15:04:05.000000"), time.Since(before), reply.Success)
	}
	go heartbeat()
	for rf.role == Leader {
		select {
		case <-rf.leaderState.ctx.Done():
			return
		case <-ticker.C:
			go heartbeat()
		}
	}
}

func (rf *Raft) GetLogEntryCached(index uint64) *LogEntry {
	if ientry, ok := rf.leaderState.inflightingEntries.Load(index); ok {
		return ientry.(*LogEntry)
	}
	return rf.GetLogEntry(index)
}

func (rf *Raft) GetLogEntry(index uint64) *LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if l, ok := rf.logs[index]; ok {
		return &l
	}
	return nil
}

func (rf *Raft) refreshCommitIndex() {
	indexes := make([]uint64, 0, len(rf.peers))
	for serverID := range rf.peers {
		indexes = append(indexes, rf.leaderState.matchIndexMap[serverID])
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] > indexes[j]
	})
	commitIndex := indexes[rf.quorumSize()-1]
	if commitIndex > rf.commitIndex {
		rf.commitIndex = commitIndex
		AsyncNotify(rf.commitCh)
	}
}

func (rf *Raft) setCommitIndex(index uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.commitIndex {
		rf.commitIndex = index
		AsyncNotify(rf.commitCh)
	}
}
func backoffDuration(base time.Duration, retries int) time.Duration {
	if retries > 5 {
		retries = 5
	}
	return base * time.Duration(1<<(retries-1))
}
func (rf *Raft) setMatchIndex(serverID int, index uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leaderState.matchIndexMap[serverID] = index
	rf.refreshCommitIndex()
}

func (rf *Raft) replicateOnce(f *followerReplication) {
	retries := 0
	for rf.role == Leader {
		select {
		case <-rf.leaderState.ctx.Done():
			return
		default:
		}

		entries := make([]LogEntry, 0)
		var prevLogTerm uint64
		var prevLogIndex uint64
		prevLogIndex = f.nextIndex - 1
		if rf.lastLogIndex-f.nextIndex >= 0 {
			rf.logging(debugLevel, "replicate to %d [%d ... %d]", f.serverID, f.nextIndex, rf.lastLogIndex)
		} else {
			rf.logging(debugLevel, "replicate to %d []", f.serverID)
		}
		for nextIndex := f.nextIndex; nextIndex <= rf.lastLogIndex; nextIndex++ {
			entry := rf.GetLogEntryCached(nextIndex)
			entries = append(entries, *entry)
		}
		if prevLogIndex > 0 {
			prevLogIndex := f.nextIndex - 1
			entry := rf.GetLogEntryCached(prevLogIndex)
			prevLogIndex = entry.Index
			prevLogTerm = entry.Term
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			LogEntries:   entries,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(f.serverID, args, reply) {
			retries = 0
			if reply.Term > rf.currentTerm {
				rf.logging(debugLevel, "follower %d term %d > leader term", f.serverID, reply.Term)
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.leaderState.cancel()
				return
			}
			if reply.Success {
				if len(args.LogEntries) > 0 {
					logIndex := args.LogEntries[len(args.LogEntries)-1].Index
					f.nextIndex = logIndex + 1
					rf.setMatchIndex(f.serverID, logIndex)
				}
				return
			} else {
				if reply.ConflictIndex > 0 {
					f.nextIndex = MinUint64(f.nextIndex-1, reply.ConflictIndex)
				} else {
					f.nextIndex = f.nextIndex - 1
				}
			}
		} else {
			retries++
			select {
			case <-time.After(backoffDuration(10*time.Millisecond, retries)):
			case <-rf.leaderState.ctx.Done():
				return
			}
		}
	}
}
func (rf *Raft) runReplicate(f *followerReplication) {
	commitTimer := time.NewTimer(time.Hour)
	defer commitTimer.Stop()
	for rf.role == Leader {
		select {
		case <-rf.ctx.Done():
			return
		default:
		}
		select {
		case <-commitTimer.C:
			rf.replicateOnce(f)
			commitTimer.Reset(time.Hour)
		case <-f.triggerCh:
			rf.replicateOnce(f)
			commitTimer.Reset(commitTimeout)
		case <-rf.leaderState.ctx.Done():
			return
		}
	}
}

func (rf *Raft) putLogEntry(logEntry *LogEntry) {
	rf.logging(debugLevel, "append log entry %d", logEntry.Index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs[logEntry.Index] = *logEntry
}

func (rf *Raft) runLeader() {
	rf.leaderID = rf.me
	rf.leaderState.leaderReady = make(chan struct{})
	rf.leaderState.followerReplications = make(map[int]*followerReplication, len(rf.peers))
	rf.leaderState.ctx, rf.leaderState.cancel = context.WithCancel(rf.ctx)
	rf.leaderState.startBuffer = make([]LogEntry, 0, 10)
	rf.leaderState.startCh = make(chan struct{}, 1)
	rf.leaderState.matchIndexMap = make(map[int]uint64)
	rf.leaderState.dispatchedIndex = rf.lastLogIndex

	for server := range rf.peers {
		if server != rf.me {
			f := &followerReplication{
				serverID:  server,
				triggerCh: make(chan struct{}, 1),
				nextIndex: rf.lastLogIndex + 1,
			}
			rf.leaderState.followerReplications[server] = f
			rf.goFunc(func() {
				rf.runHeartbeat(f)
			})
			rf.goFunc(func() {
				rf.runReplicate(f)
			})
		}
	}
	close(rf.leaderState.leaderReady)

	defer func() {
		rf.leaderState.cancel()
		rf.leaderState.leaderReady = make(chan struct{})
		rf.logging(infoLevel, "exit leader")
	}()
	for {
		select {
		case <-rf.leaderState.ctx.Done():
			return
		case <-time.After(electionTimeout):
			now := time.Now()
			count := 0
			for _, fs := range rf.leaderState.followerReplications {
				if now.Sub(fs.lastContact) < electionTimeout {
					count++
				}
			}
			if count+1 < rf.quorumSize() {
				rf.logging(infoLevel, "leader lease expire")
				rf.role = Follower
				rf.leaderState.cancel()
				return
			}
		case <-rf.leaderState.startCh:
			rf.mu.Lock()
			buf := rf.leaderState.startBuffer
			rf.leaderState.startBuffer = make([]LogEntry, 0, 10)
			rf.mu.Unlock()

			for i := range buf {
				logEntry := &buf[i]
				rf.leaderState.inflightingEntries.Store(logEntry.Index, logEntry)
				rf.putLogEntry(logEntry)
				rf.setLastLog(logEntry.Index, logEntry.Term)
				rf.setMatchIndex(rf.me, logEntry.Index)
			}
			for _, f := range rf.leaderState.followerReplications {
				AsyncNotify(f.triggerCh)
			}
		}
	}
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.ctx.Done():
			return
		default:
		}
		rf.logging(infoLevel, "switch role")
		switch rf.role {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		}
	}
}

func (rf *Raft) runApplyMsg() {
	for {
		select {
		case <-rf.ctx.Done():
			return
		case <-rf.commitCh:
			if rf.lastApplied < rf.commitIndex {
				rf.logging(debugLevel, "commit [%d ... %d]", rf.lastApplied+1, rf.commitIndex)
			}
			for rf.lastApplied < rf.commitIndex {
				logEntry := rf.GetLogEntry(rf.lastApplied + 1)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      logEntry.Command,
					CommandIndex: int(logEntry.Index),
				}
				rf.lastApplied++
			}
		}
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.logs = make(map[uint64]LogEntry, 0)
	rf.commitCh = make(chan struct{}, 1)
	rf.ctx, rf.cancel = context.WithCancel(context.Background())
	rf.logLevel = debugLevel

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.goFunc(rf.runApplyMsg)
	rf.goFunc(rf.run)
	return rf
}

func (rf *Raft) logging(level logLevel, format string, a ...interface{}) {
	if level <= rf.logLevel {
		prefix := fmt.Sprintf("%s [%5s] [id %d] [role %10s] [term %4d]: ", time.Now().Format("15:04:05.000000"), level, rf.me, rf.role, rf.currentTerm)
		fmt.Println(prefix + fmt.Sprintf(format, a...))
	}
}
