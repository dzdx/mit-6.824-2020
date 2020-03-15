package shardmaster

import (
	"../logging"
	"../raft"
	"context"
	"fmt"
	"math"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	inflightFutures sync.Map

	configs  []Config // indexed by config num
	ctx      context.Context
	cancel   context.CancelFunc
	requests map[int64]int64
	logger   *logging.Logger
}

type Op struct {
	// Your data here.
	ClientID int64
	SeqID    int64
	Cmd      string // Join, Leave, Move, Query

	// Join
	Servers map[int][]string

	// Leave
	GIDs []int

	// Move
	Shard int
	GID   int

	// Query
	Num int
}

type requestFuture struct {
	index        int
	respCh       chan interface{}
	respClientID int64
	respSeqID    int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command := Op{
		ClientID: args.ClientID,
		SeqID:    args.SeqID,
		Cmd:      "Join",
		Servers:  args.Servers,
	}
	future := &requestFuture{
		respCh: make(chan interface{}, 1),
	}
	index, _, isLeader := sm.rf.Start(command)
	sm.inflightFutures.Store(index, future)
	defer sm.inflightFutures.Delete(index)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.logger.Debugf("start %d Join %v", index, args.Servers)
	select {
	case <-future.respCh:
		if !(future.respClientID == args.ClientID && future.respSeqID == args.SeqID) {
			reply.WrongLeader = true
		}
	case <-sm.ctx.Done():
		reply.WrongLeader = true
	case <-time.After(time.Second):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op{
		ClientID: args.ClientID,
		SeqID:    args.SeqID,
		Cmd:      "Leave",
		GIDs:     args.GIDs,
	}
	future := &requestFuture{
		respCh: make(chan interface{}, 1),
	}
	index, _, isLeader := sm.rf.Start(command)
	sm.inflightFutures.Store(index, future)
	defer sm.inflightFutures.Delete(index)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.logger.Debugf("start %d Leave %v", index, args.GIDs)
	select {
	case <-future.respCh:
		if !(future.respClientID == args.ClientID && future.respSeqID == args.SeqID) {
			reply.WrongLeader = true
		}
	case <-sm.ctx.Done():
		reply.WrongLeader = true
	case <-time.After(time.Second):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command := Op{
		ClientID: args.ClientID,
		SeqID:    args.SeqID,
		Cmd:      "Move",
		Shard:    args.Shard,
		GID:      args.GID,
	}
	future := &requestFuture{
		respCh: make(chan interface{}, 1),
	}
	index, _, isLeader := sm.rf.Start(command)
	sm.inflightFutures.Store(index, future)
	defer sm.inflightFutures.Delete(index)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.logger.Debugf("start %d Move Shard(%d)=>Gid(%d)", index, args.Shard, args.GID)
	select {
	case <-future.respCh:
		if !(future.respClientID == args.ClientID && future.respSeqID == args.SeqID) {
			reply.WrongLeader = true
		}
	case <-sm.ctx.Done():
		reply.WrongLeader = true
	case <-time.After(time.Second):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	command := Op{
		ClientID: args.ClientID,
		SeqID:    args.SeqID,
		Cmd:      "Query",
		Num:      args.Num,
	}
	future := &requestFuture{
		respCh: make(chan interface{}, 1),
	}
	index, _, isLeader := sm.rf.Start(command)
	sm.inflightFutures.Store(index, future)
	defer sm.inflightFutures.Delete(index)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.logger.Debugf("start %d Query %d", index, args.Num)
	select {
	case obj := <-future.respCh:
		if !(future.respClientID == args.ClientID && future.respSeqID == args.SeqID) {
			reply.WrongLeader = true
		} else {
			reply.Config = obj.(Config)
		}
	case <-sm.ctx.Done():
		reply.WrongLeader = true
	case <-time.After(time.Second):
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.cancel()
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}
func toGid2shards(config *Config) map[int][]int {
	gid2shards := make(map[int][]int, 0)
	for gid := range config.Groups {
		gid2shards[gid] = []int{}
	}
	for shard, gid := range config.Shards {
		gid2shards[gid] = append(gid2shards[gid], shard)
	}
	return gid2shards
}

func maxShardGid(gid2shards map[int][]int) int {
	max := -1
	var ret int
	for gid, shards := range gid2shards {
		if max < len(shards) {
			max = len(shards)
			ret = gid
		}
	}
	return ret
}
func minShardGid(gid2shards map[int][]int) int {
	min := math.MaxInt64
	var ret int
	for gid, shards := range gid2shards {
		if min > len(shards) {
			min = len(shards)
			ret = gid
		}
	}
	return ret
}
func (sm *ShardMaster) rebalanceShards(config *Config, isJoin bool, gid int) {
	gid2shards := toGid2shards(config)
	if isJoin {
		avg := NShards / len(config.Groups)
		for i := 0; i < avg; i++ {
			maxGid := maxShardGid(gid2shards)
			config.Shards[gid2shards[maxGid][0]] = gid
			gid2shards[maxGid] = gid2shards[maxGid][1:]
		}
	} else {
		leaveShards, ok := gid2shards[gid]
		if !ok {
			return
		}
		delete(gid2shards, gid)
		if len(config.Groups) == 0 {
			config.Shards = [NShards]int{}
			return
		}
		for _, shard := range leaveShards {
			minGid := minShardGid(gid2shards)
			config.Shards[shard] = minGid
			gid2shards[minGid] = append(gid2shards[minGid], shard)
		}
	}
}

func (sm *ShardMaster) processApplyCommand(msg raft.ApplyMsg) {
	command := msg.Command
	op := command.(Op)
	var resp interface{}
	if op.Cmd == "Query" {
		index := op.Num
		length := len(sm.configs)
		if index < 0 || index >= length {
			index = length - 1
		}
		resp = sm.configs[index]
	} else {
		maxSeq, ok := sm.requests[op.ClientID]
		if !ok || op.SeqID > maxSeq {
			sm.requests[op.ClientID] = maxSeq
			length := len(sm.configs)
			lastConfig := sm.configs[length-1]
			config := copyConfig(lastConfig)
			config.Num = length
			switch op.Cmd {
			case "Join":
				for gid, newServers := range op.Servers {
					config.Groups[gid] = newServers
					sm.rebalanceShards(&config, true, gid)
				}
			case "Leave":
				for _, gid := range op.GIDs {
					delete(config.Groups, gid)
					sm.rebalanceShards(&config, false, gid)
				}
			case "Move":
				config.Shards[op.Shard] = op.GID
			}
			sm.configs = append(sm.configs, config)
		}
	}
	if obj, ok := sm.inflightFutures.Load(msg.CommandIndex); ok {
		future := obj.(*requestFuture)
		future.respClientID = op.ClientID
		future.respSeqID = op.SeqID
		sm.logger.Debugf("respond %d %+v", msg.CommandIndex, resp)
		future.respCh <- resp
	}
}
func (sm *ShardMaster) processApply() {
	for {
		select {
		case <-sm.ctx.Done():
			return
		case msg := <-sm.applyCh:
			sm.processApplyCommand(msg)
		}
	}
}

func copyConfig(c Config) Config {
	cp := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range c.Groups {
		cv := make([]string, len(v))
		copy(cv, v)
		cp.Groups[k] = cv
	}
	return cp
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.logger = logging.NewLogger(logging.InfoLevel, true, func() string {
		return fmt.Sprintf("[ShardMaster] [id %d]", sm.me)
	})

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.requests = make(map[int64]int64)
	sm.ctx, sm.cancel = context.WithCancel(context.Background())
	go sm.processApply()
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	// Your code here.

	return sm
}
