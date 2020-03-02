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
	"bytes"
	"container/list"
	"context"
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

const (
	electionTimeout = 1000 * time.Millisecond
	commitTimeout   = 10 * time.Millisecond
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

type entryType int

const (
	commandType entryType = iota
	noopType
)

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command interface{}
	Type    entryType
}

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

	inflightingEntries sync.Map
	commitment         *commitment
}

func (rf *Raft) getLastLog() (uint64, uint64) {
	rf.lastLogLock.Lock()
	defer rf.lastLogLock.Unlock()
	return rf.lastLogIndex, rf.lastLogTerm
}

func (rf *Raft) setLastLog(index, term uint64) {
	rf.lastLogLock.Lock()
	defer rf.lastLogLock.Unlock()
	rf.lastLogIndex = index
	rf.lastLogTerm = term
}

type followerReplication struct {
	serverID    int
	lastContact time.Time
	nextIndex   uint64
	commitIndex uint64
	triggerCh   chan struct{}
	mutex       sync.Mutex
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

	lastLogLock       sync.Mutex
	wg                sync.WaitGroup
	role              raftRole
	lastContactLeader time.Time
	leaderID          int32
	applyCh           chan ApplyMsg
	notifyApplyCh     chan struct{}
	logs              map[uint64]LogEntry
	ctx               context.Context
	cancel            context.CancelFunc
	logLevel          logLevel

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
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.getCurrentTerm())
	e.Encode(rf.votedFor)
	e.Encode(rf.votedForTerm)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
		rf.logs = logs
		var maxIndex uint64
		for k := range rf.logs {
			if k > maxIndex {
				maxIndex = k
			}
		}
		if lastLog, ok := rf.logs[maxIndex]; ok {
			rf.lastLogIndex = lastLog.Index
			rf.lastLogTerm = lastLog.Term
		}
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
	CommitIndex   uint64
}

func (rf *Raft) getLeader() int {
	return int(atomic.LoadInt32(&rf.leaderID))
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer func() {
		reply.Term = rf.getCurrentTerm()
	}()
	leaderID := rf.getLeader()
	if leaderID != -1 && leaderID != args.CandidateID {
		rf.logging(warnLevel, "refused request vote from %d: (now has another leader)", args.CandidateID)
		return
	}
	if args.Term < rf.getCurrentTerm() {
		rf.logging(warnLevel, "refused request vote from %d: (term smaller)", args.CandidateID)
		return
	}
	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.setRole(follower)
	}
	if args.LastLogTerm < rf.lastLogTerm {
		rf.logging(warnLevel, "refused request vote from %d: (lastlog term smaller)", args.CandidateID)
		return
	}
	lastLogIndex, lastLogTerm := rf.getLastLog()
	if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		rf.logging(warnLevel, "refused request vote from %d: (lastlog index smaller)", args.CandidateID)
		return
	}

	if rf.vote(args.CandidateID, args.Term) {
		reply.VoteGranted = true
	} else {
		rf.logging(warnLevel, "refused request vote from %d: (has voted to %d)", args.CandidateID, rf.votedFor)
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
		rf.logging(debugLevel, "vote for %d", server)
		rf.votedFor = server
		rf.votedForTerm = term
		rf.persist()
		return true
	}
	return false
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
		reply.Term = rf.getCurrentTerm()
		reply.CommitIndex = rf.getCommitIndex()
	}()
	if args.Term < rf.getCurrentTerm() {
		rf.logging(warnLevel, "refused append entries from %d: (term smaller)", args.LeaderID)
		return
	}
	if args.Term > rf.getCurrentTerm() || rf.getRole() != follower {
		rf.setCurrentTerm(args.Term)
		rf.setRole(follower)
	}
	lastLogIndex, _ := rf.getLastLog()
	if args.PrevLogIndex > 0 {
		prevLog := rf.getLogEntry(args.PrevLogIndex)
		if prevLog == nil {
			reply.ConflictIndex = lastLogIndex + 1
			rf.logging(warnLevel, "refused append entries from %d: (prev log %d not existed)", args.LeaderID, args.PrevLogIndex)
			return
		}
		if args.PrevLogTerm != prevLog.Term {
			reply.ConflictIndex = rf.getFirstEntryByTerm(prevLog.Term).Index
			rf.logging(warnLevel, "refused append entries from %d: (prev log %d conflict)", args.LeaderID, args.PrevLogIndex)
			return
		}
	}
	if len(args.LogEntries) > 0 {
		deleteFromIndex := lastLogIndex + 1
		for _, newLogEntry := range args.LogEntries {
			oldLogEntry := rf.getLogEntry(newLogEntry.Index)
			if oldLogEntry == nil || newLogEntry.Term != oldLogEntry.Term {
				deleteFromIndex = newLogEntry.Index
				break
			}
		}
		if lastLogIndex >= deleteFromIndex {
			rf.logging(debugLevel, "delete log entry [%d ... %d]", deleteFromIndex, lastLogIndex)
		}
		rf.deleteLogEntriesRange(deleteFromIndex, lastLogIndex+1)
		for _, newLogEntry := range args.LogEntries {
			if newLogEntry.Index >= deleteFromIndex {
				rf.putLogEntry(&newLogEntry)
				rf.setLastLog(newLogEntry.Index, newLogEntry.Term)
			}
		}
		rf.persist()
	}
	if args.LeaderCommit > 0 {
		lastLogIndex, _ := rf.getLastLog()
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
	rf.logging(debugLevel, "dispatch log entry %d", logEntry.Index)

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
				}
			} else {
				lastLogIndex, lastLogTerm := rf.getLastLog()
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
				rf.logging(warnLevel, "switch to follower: (follower %d term %d > leader term)", f.serverID, reply.Term)
				rf.setCurrentTerm(reply.Term)
				rf.setRole(follower)
				rf.leaderState.cancel()
			}
			if reply.Success {
				f.updateLastContract()
			}
		}
		rf.logging(traceLevel, "heartbeat to %d %s %v %v", f.serverID, before.Format("15:04:05.000000"), time.Since(before), reply.Success)
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

func (rf *Raft) getLogEntry(index uint64) *LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if l, ok := rf.logs[index]; ok {
		return &l
	}
	return nil
}
func (rf *Raft) getFirstEntryByTerm(term uint64) *LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var ret *LogEntry
	for i := range rf.logs {
		entry := rf.logs[i]
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

func (rf *Raft) replicateOnce(f *followerReplication) {
	retries := 0
	for rf.getRole() == leader {
		select {
		case <-rf.leaderState.ctx.Done():
			return
		default:
		}

		entries := make([]LogEntry, 0)
		var prevLogTerm uint64
		var prevLogIndex uint64
		prevLogIndex = f.nextIndex - 1
		lastLogIndex, _ := rf.getLastLog()
		if lastLogIndex >= f.nextIndex {
			rf.logging(debugLevel, "replicate to %d [%d ... %d]", f.serverID, f.nextIndex, lastLogIndex)
		} else {
			rf.logging(debugLevel, "replicate to %d []", f.serverID)
		}
		for nextIndex := f.nextIndex; nextIndex <= lastLogIndex; nextIndex++ {
			entry := rf.getLogEntry(nextIndex)
			entries = append(entries, *entry)
		}
		if prevLogIndex > 0 {
			prevLogIndex := f.nextIndex - 1
			entry := rf.getLogEntry(prevLogIndex)
			prevLogIndex = entry.Index
			prevLogTerm = entry.Term
		}

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
			retries = 0
			if reply.Term > rf.getCurrentTerm() {
				rf.logging(warnLevel, "switch to follower: (follower %d term %d > leader term)", f.serverID, reply.Term)
				rf.setCurrentTerm(reply.Term)
				rf.setRole(follower)
				rf.leaderState.cancel()
				return
			}
			if reply.Success {
				if len(args.LogEntries) > 0 {
					logIndex := args.LogEntries[len(args.LogEntries)-1].Index
					f.nextIndex = logIndex + 1
					rf.leaderState.commitment.setMatchIndex(f.serverID, logIndex)
				}
				f.commitIndex = reply.CommitIndex
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

func (rf *Raft) putLogEntry(logEntry *LogEntry) {
	rf.logging(debugLevel, "append log entry %d", logEntry.Index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs[logEntry.Index] = *logEntry
}

func (rf *Raft) runLeader() {
	rf.setLeader(rf.me)
	rf.leaderState.followerReplications = make(map[int]*followerReplication, len(rf.peers))
	rf.leaderState.ctx, rf.leaderState.cancel = context.WithCancel(rf.ctx)
	rf.leaderState.startBuffer = list.New()
	rf.leaderState.startCh = make(chan struct{}, 1)
	rf.leaderState.commitment = newCommitment(rf.peers)
	rf.leaderState.inflightingEntries = sync.Map{}
	lastLogIndex, _ := rf.getLastLog()
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
		rf.logging(warnLevel, "exit leader")
	}()
	leaderLeaseTimer := time.NewTimer(electionTimeout)
	defer leaderLeaseTimer.Stop()

	for rf.getRole() == leader {
		select {
		case <-rf.leaderState.ctx.Done():
			return
		case <-leaderLeaseTimer.C:
			leaderLeaseTimer.Reset(electionTimeout)
			now := time.Now()
			count := 0
			for _, fs := range rf.leaderState.followerReplications {
				if now.Sub(fs.getLastContract()) < electionTimeout {
					count++
				}
			}
			if count+1 < rf.quorumSize() {
				rf.logging(warnLevel, "leader lease expire")
				rf.setRole(follower)
				rf.leaderState.cancel()
				return
			}
		case <-rf.leaderState.startCh:
			rf.mu.Lock()
			buf := rf.leaderState.startBuffer
			rf.leaderState.startBuffer = list.New()
			rf.mu.Unlock()

			for elem := buf.Front(); elem != nil; elem = elem.Next() {
				logEntry := elem.Value.(*LogEntry)
				rf.leaderState.inflightingEntries.Store(logEntry.Index, logEntry)
				rf.putLogEntry(logEntry)
				rf.setLastLog(logEntry.Index, logEntry.Term)
				rf.persist()
				rf.leaderState.commitment.setMatchIndex(rf.me, logEntry.Index)
			}
			for _, f := range rf.leaderState.followerReplications {
				AsyncNotify(f.triggerCh)
			}
		case <-rf.leaderState.commitment.commitCh:
			rf.leaderCommit(rf.leaderState.commitment.commitIndex)
		}
	}
}

func (rf *Raft) leaderCommit(index uint64) {
	entry := rf.getLogEntry(index)
	if entry.Term == rf.getCurrentTerm() {
		rf.commitTo(index)
		go func() {
			select {
			case <-rf.leaderState.ctx.Done():
				return
			case <-time.After(commitTimeout):
			}
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
			return
		default:
		}
		rf.logging(infoLevel, "switch role")
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

func (rf *Raft) runApplyMsg() {
	for {
		select {
		case <-rf.ctx.Done():
			return
		case <-rf.notifyApplyCh:
			commitIndex := rf.getCommitIndex()
			if rf.lastApplied < commitIndex {
				rf.logging(debugLevel, "apply [%d ... %d]", rf.lastApplied+1, commitIndex)
			}
			for rf.lastApplied < commitIndex {
				logEntry := rf.getLogEntry(rf.lastApplied + 1)
				if logEntry.Type == commandType {
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      logEntry.Command,
						CommandIndex: int(logEntry.Index),
					}
				}
				rf.leaderState.inflightingEntries.Delete(logEntry.Index)
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
	rf.role = follower
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.logs = make(map[uint64]LogEntry, 0)
	rf.notifyApplyCh = make(chan struct{}, 1)
	rf.ctx, rf.cancel = context.WithCancel(context.Background())
	rf.logLevel = infoLevel

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.goFunc(rf.runApplyMsg)
	rf.goFunc(rf.run)
	return rf
}
