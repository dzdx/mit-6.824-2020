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
	"math/rand"
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
	electionTimeout = 150 * time.Millisecond
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

type leaderState struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
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

type replicateState struct {
	LastContact time.Time
	NextIndex   uint64
	MatchIndex  uint64
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

	wg              sync.WaitGroup
	role            raftRole
	lastContact     time.Time
	leaderID        int
	replicateStates map[int]*replicateState
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
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

func (rf *Raft) setVotedFor(server int) {
	DPrintf("%d voted for %d in term %d \n", rf.me, server, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = server
	rf.votedForTerm = rf.currentTerm
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.votedForTerm == rf.currentTerm {
		return rf.votedFor
	}
	return -1
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer func() {
		reply.Term = rf.currentTerm
	}()

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
	}
	if args.LastLogIndex < rf.lastLogIndex {
		return
	}
	if rf.leaderID != args.CandidateID && time.Now().Sub(rf.lastContact) < electionTimeout {
		return
	}
	votedFor := rf.getVotedFor()
	if votedFor < 0 || votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.setVotedFor(args.CandidateID)
	}
	// Your code here (2A, 2B).
}
func (rf *Raft) setLastContact(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leaderID = server
	rf.lastContact = time.Now()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
		case <-time.After(randomDuration(electionTimeout)):
			if time.Now().Sub(rf.lastContact) > electionTimeout {
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
				rf.setVotedFor(rf.me)
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

func (rf *Raft) runHeartbeat() {
	ticker := time.NewTicker(electionTimeout / 10)
	defer ticker.Stop()
	heartbeat := func() {
		for i := range rf.peers {
			server := i
			rf.goFunc(func() {
				if server != rf.me {
					args := &AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderID: rf.me,
					}
					reply := &AppendEntriesReply{}
					rf.sendAppendEntries(server, args, reply)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
					}
					if reply.Success {
						rf.replicateStates[server].LastContact = time.Now()
					}
				}
			})
		}
	}
	heartbeat()
	for rf.role == Leader {
		select {
		case <-rf.leaderState.ctx.Done():
			return
		case <-ticker.C:
			heartbeat()
		}
	}
}

func (rf *Raft) runLeader() {
	rf.leaderID = rf.me
	rf.replicateStates = make(map[int]*replicateState, len(rf.peers))
	rf.leaderState.ctx, rf.leaderState.cancel = context.WithCancel(context.Background())
	for server := range rf.peers {
		rf.replicateStates[server] = &replicateState{}
	}
	rf.goFunc(func() {
		rf.leaderState.wg.Add(1)
		defer rf.leaderState.wg.Done()
		rf.runHeartbeat()
	})

	defer func() {
		rf.leaderState.cancel()
		rf.leaderState.wg.Wait()
	}()
	for {
		select {
		case <-time.After(electionTimeout):
			now := time.Now()
			count := 0
			for server, fs := range rf.replicateStates {
				if server == rf.me || now.Sub(fs.LastContact) < electionTimeout {
					count++
				}
			}
			if count < rf.quorumSize() {
				DPrintf("%d leader lease expire\n", rf.me)
				rf.role = Follower
				return
			}
		}
	}
}

func (rf *Raft) run() {
	for {
		DPrintf("server: %d role: %s term: %d\n", rf.me, rf.role, rf.currentTerm)
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
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.goFunc(rf.run)
	return rf
}
