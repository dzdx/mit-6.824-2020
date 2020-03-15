package shardkv

import (
	"../logging"
	"../shardmaster"
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"
)
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	SeqID    int64
	Cmd      string
	Key      string
	Value    string
}

type noop struct{} // only for check leader
type requestFuture struct {
	index        int
	resp         interface{}
	errCh        chan error
	respClientID int64
	respSeqID    int64
}

type ShardKV struct {
	mu              sync.Mutex
	me              int
	rf              *raft.Raft
	applyCh         chan raft.ApplyMsg
	make_end        func(string) *labrpc.ClientEnd
	gid             int
	masters         []*labrpc.ClientEnd
	maxraftstate    int // snapshot if log grows this big
	mck             *shardmaster.Clerk
	data            map[string]string
	inflightFutures sync.Map
	requests        map[int64]int64
	ctx             context.Context
	cancel          context.CancelFunc
	logger          *logging.Logger
	persister       *raft.Persister
	snapshoting     int32
	config          shardmaster.Config

	// Your definitions here.
}

func (kv *ShardKV) matchShard(key string) bool {
	shard := key2shard(key)
	gid := kv.config.Shards[shard]
	if gid != kv.gid {
		kv.logger.Warnf("wrong group key: %s belong to %d, config: %+v", key, gid, kv.config.Shards)
		return false
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	if !kv.matchShard(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	// Your code here.
	command := Op{
		ClientID: args.ClientID,
		SeqID:    args.SeqID,
		Cmd:      "Get",
		Key:      args.Key,
	}
	future := &requestFuture{errCh: make(chan error, 1)}
	index, _, isLeader := kv.rf.Start(command)
	kv.inflightFutures.Store(index, future)
	defer kv.inflightFutures.Delete(index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.logger.Debugf("start %d %+v", index, command)
	select {
	case err := <-future.errCh:
		if err != nil {
			reply.Err = Err(err.Error())
			return
		}
		if future.respSeqID == args.SeqID && future.respClientID == args.ClientID {
			reply.Value = future.resp.(string)
			reply.Err = OK
		} else {
			kv.logger.Warnf("conflicted: %d", index)
			reply.Err = ErrWrongLeader
		}
	case <-kv.ctx.Done():
		kv.logger.Warnf("canceled: %d", index)
		reply.Err = ErrWrongLeader
	case <-time.After(time.Second):
		kv.logger.Warnf("timeout: %d", index)
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.matchShard(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	command := Op{
		ClientID: args.ClientID,
		SeqID:    args.SeqID,
		Cmd:      args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	future := &requestFuture{errCh: make(chan error, 1)}

	index, _, isLeader := kv.rf.Start(command)
	kv.inflightFutures.Store(index, future)
	defer kv.inflightFutures.Delete(index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.logger.Debugf("start %+v", command)
	select {
	case err := <-future.errCh:
		if err != nil {
			reply.Err = Err(err.Error())
			return
		}
		if future.respSeqID == args.SeqID && future.respClientID == args.ClientID {
			reply.Err = OK
		} else {
			kv.logger.Warnf("conflicted: %d", index)
			reply.Err = ErrWrongLeader
		}
	case <-kv.ctx.Done():
		kv.logger.Warnf("canceled: %d", index)
		reply.Err = ErrWrongLeader
	case <-time.After(time.Second):
		kv.logger.Warnf("timeout: %d", index)
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) processApply() {
	for {
		select {
		case <-kv.ctx.Done():
			return
		case msg := <-kv.applyCh:
			if msg.SnapshotData != nil {
				kv.processApplySnapshot(msg)
			} else {
				kv.processApplyCommand(msg)
			}
		}
	}
}

func (kv *ShardKV) processApplyCommand(msg raft.ApplyMsg) {
	command := msg.Command
	var resp interface{}
	var err error = nil
	op := command.(Op)
	if op.Cmd == "Get" {
		resp = kv.data[op.Key]
	} else {
		kv.mu.Lock()
		maxSeq, ok := kv.requests[op.ClientID]
		if !ok || op.SeqID > maxSeq {
			kv.requests[op.ClientID] = maxSeq
			if op.Cmd == "Append" {
				kv.data[op.Key] = kv.data[op.Key] + op.Value
			} else {
				kv.data[op.Key] = op.Value
			}
		}
		kv.mu.Unlock()
		kv.checkSnapshot(msg.CommandIndex)
	}
	if obj, ok := kv.inflightFutures.Load(msg.CommandIndex); ok {
		future := obj.(*requestFuture)
		future.respClientID = op.ClientID
		future.respSeqID = op.SeqID
		kv.logger.Debugf("respond %d %+v", msg.CommandIndex, resp)
		future.resp = resp
		future.errCh <- err
	}
}

func (kv *ShardKV) processApplySnapshot(msg raft.ApplyMsg) {
	d := labgob.NewDecoder(bytes.NewBuffer(msg.SnapshotData))
	kv.data = make(map[string]string)
	d.Decode(&kv.data)
	d.Decode(&kv.requests)
	msg.SnapshotRespCh <- nil
}

func (kv *ShardKV) checkSnapshot(index int) {
	if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate*4/5 {
		if atomic.SwapInt32(&kv.snapshoting, 1) == 0 {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.data)
			e.Encode(kv.requests)
			future := &raft.SnapshotFuture{
				Data:             w.Bytes(),
				LastIncludeIndex: uint64(index),
				RespCh:           make(chan error, 1),
			}
			kv.rf.StartSnapshot(future)
			go func() {
				<-future.RespCh
				atomic.StoreInt32(&kv.snapshoting, 0)
			}()
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.cancel()
	// Your code here, if desired.
}

func (kv *ShardKV) transfer(newConfig shardmaster.Config) {
	oldConfig := kv.config
	newGids := make(map[int]struct{})
	for shard, gid := range oldConfig.Shards {
		if gid == kv.gid && newConfig.Shards[shard] != kv.gid {
			newGid := newConfig.Shards[shard]
			newGids[newGid] = struct{}{}
		}
	}
	if _, isLeader := kv.rf.GetState(); isLeader {
		for newGid := range newGids {
			kv.logger.Infof("transfer data to %+v", newGid)
		}
	}
	kv.mu.Lock()
	kv.config = newConfig
	kv.mu.Unlock()

}

func (kv *ShardKV) reconfigureLoop() {
	for {
		config := kv.mck.Query(-1)
		if config.Num != kv.config.Num {
			kv.transfer(config)
		}
		select {
		case <-time.After(time.Millisecond * 80):
		case <-kv.ctx.Done():
			return
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(noop{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.requests = make(map[int64]int64)

	kv.ctx, kv.cancel = context.WithCancel(context.Background())
	kv.logger = logging.NewLogger(logging.DebugLevel, true, func() string {
		return fmt.Sprintf("[ShardKV] [gid %d] [id %d]", kv.gid, kv.me)
	})
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	go kv.processApply()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.reconfigureLoop()
	return kv
}
