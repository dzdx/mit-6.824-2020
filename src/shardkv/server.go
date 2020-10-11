package shardkv

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dzdx/mit-6.824-2020/src/labgob"
	"github.com/dzdx/mit-6.824-2020/src/labrpc"
	"github.com/dzdx/mit-6.824-2020/src/logging"
	"github.com/dzdx/mit-6.824-2020/src/raft"
	"github.com/dzdx/mit-6.824-2020/src/shardmaster"
)

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
	makeEnd         func(string) *labrpc.ClientEnd
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(key)
	gid := kv.config.Shards[shard]
	if gid != kv.gid {
		kv.logger.Warnf("wrong group key: %s belong to %d, config: %+v", key, gid, kv.config.Shards)
		return false
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	// Your code here.
	command := Op{
		ClientID: args.ClientID,
		SeqID:    args.SeqID,
		Cmd:      "Get",
		Key:      args.Key,
	}
	future := &requestFuture{errCh: make(chan error, 1)}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if !kv.matchShard(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	kv.inflightFutures.Store(index, future)
	defer kv.inflightFutures.Delete(index)
	kv.logger.Debugf("start %d shard(%d) %+v", index, key2shard(args.Key), command)
	select {
	case err := <-future.errCh:
		if err != nil {
			reply.Err = Err(err.Error())
			return
		}
		if future.respSeqID == args.SeqID && future.respClientID == args.ClientID {
			reply.Value = future.resp.(string)
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
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.inflightFutures.Store(index, future)
	defer kv.inflightFutures.Delete(index)
	kv.logger.Debugf("start %d shard(%d) %+v", index, key2shard(args.Key), command)
	select {
	case err := <-future.errCh:
		if err != nil {
			reply.Err = Err(err.Error())
			return
		}
		if future.respSeqID == args.SeqID && future.respClientID == args.ClientID {
			reply.Err = ""
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
func (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) {

	command := Op{
		Cmd:   "ShardMigration",
		Value: strconv.Itoa(args.Shard),
	}
	future := &requestFuture{errCh: make(chan error, 1)}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.inflightFutures.Store(index, future)
	defer kv.inflightFutures.Delete(index)
	kv.logger.Debugf("start get shard %s", command.Value)
	select {
	case err := <-future.errCh:
		if err != nil {
			reply.Err = Err(err.Error())
			return
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
	switch op.Cmd {
	case "Get":
		resp = kv.data[op.Key]
	case "Append":
		kv.mu.Lock()
		maxSeq, ok := kv.requests[op.ClientID]
		if !ok || op.SeqID > maxSeq {
			kv.requests[op.ClientID] = maxSeq
			kv.data[op.Key] = kv.data[op.Key] + op.Value
		}
		kv.mu.Unlock()
		kv.checkSnapshot(msg.CommandIndex)
	case "Put":
		kv.mu.Lock()
		maxSeq, ok := kv.requests[op.ClientID]
		if !ok || op.SeqID > maxSeq {
			kv.requests[op.ClientID] = maxSeq
			kv.data[op.Key] = kv.data[op.Key] + op.Value
		}
		kv.mu.Unlock()
		kv.checkSnapshot(msg.CommandIndex)
	case "UpdateConfig":
		buf := bytes.NewBufferString(op.Value)
		dec := gob.NewDecoder(buf)
		updateConfig := &UpdateConfig{}
		_ = dec.Decode(updateConfig)
		kv.ApplyNewConfig(updateConfig)
	case "ShardMigration":
		reply := &ShardMigrationReply{}
		reply.Data = map[string]string{}
		shard, _ := strconv.Atoi(op.Value)
		for k, v := range kv.data {
			if key2shard(k) == shard {
				reply.Data[k] = v
			}
		}
		reply.Requests = kv.requests
		resp = reply
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
func (kv *ShardKV) ApplyNewConfig(args *UpdateConfig) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for k, v := range args.Data {
		kv.data[k] = v
	}
	for k, v := range args.Requests {
		if v > kv.requests[k] {
			kv.requests[k] = v
		}
	}
	kv.config = args.Config
}

func (kv *ShardKV) processApplySnapshot(msg raft.ApplyMsg) {
	d := labgob.NewDecoder(bytes.NewBuffer(msg.SnapshotData))
	kv.data = make(map[string]string)
	_ = d.Decode(&kv.data)
	_ = d.Decode(&kv.requests)
	msg.SnapshotRespCh <- nil
}

func (kv *ShardKV) checkSnapshot(index int) {
	if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate*4/5 {
		if atomic.SwapInt32(&kv.snapshoting, 1) == 0 {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			_ = e.Encode(kv.data)
			_ = e.Encode(kv.requests)
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

func (kv *ShardKV) onRecvConfigUpdate(newConfig shardmaster.Config) {
	kv.logger.Debugf("on config update %v", newConfig)
	kv.mu.Lock()
	oldConfig := kv.config
	kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if newConfig.Num > oldConfig.Num {
		if isLeader {
			newShards := make([]int, 0)
			for i := 0; i < shardmaster.NShards; i++ {
				if oldConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
					newShards = append(newShards, i)
				}
			}
			kv.sendShardMigration(newConfig, oldConfig, newShards)
		}
	}
}

func (kv *ShardKV) sendShardMigration(newConfig shardmaster.Config, oldConfig shardmaster.Config, newShards []int) {
	mergedData := make(map[string]string)
	mergedRequests := make(map[int64]int64)
	if len(oldConfig.Groups) > 0 {

		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, shard := range newShards {
			wg.Add(1)
			go func(shard int) {
				defer wg.Done()
				for {
					servers := oldConfig.Groups[oldConfig.Shards[shard]]
					for _, server := range servers {
						peer := kv.makeEnd(server)
						args := &ShardMigrationArgs{Shard: shard}
						reply := &ShardMigrationReply{}
						ok := peer.Call("ShardKV.ShardMigration", args, &reply)
						if ok && reply.Err == "" {
							mu.Lock()
							for k, v := range reply.Data {
								mergedData[k] = v
							}
							for k, v := range reply.Requests {
								mergedRequests[k] = maxInt64(mergedRequests[k], v)
							}
							mu.Unlock()
							return
						}
					}
					kv.logger.Warnf("send shard migration failed")
					time.Sleep(100 * time.Millisecond)
				}
			}(shard)
		}
		wg.Wait()
	}
	if err := kv.migrate(newConfig, mergedData, mergedRequests); err != nil {
		kv.logger.Errorf("%v", err)
	}
}

func (kv *ShardKV) migrate(newConfig shardmaster.Config, data map[string]string, requests map[int64]int64) error {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return errors.New(ErrWrongLeader)
	}
	updateConfig := UpdateConfig{
		Config:   newConfig,
		Data:     data,
		Requests: requests,
	}
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	_ = enc.Encode(updateConfig)
	command := Op{
		Cmd:   "UpdateConfig",
		Value: buf.String(),
	}
	future := &requestFuture{errCh: make(chan error, 1)}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return errors.New(ErrWrongLeader)
	}
	kv.inflightFutures.Store(index, future)
	defer kv.inflightFutures.Delete(index)
	kv.logger.Debugf("start update config %d shards: %v, groups: %v", updateConfig.Config.Num, updateConfig.Config.Shards, updateConfig.Config.Groups)
	select {
	case err := <-future.errCh:
		if err != nil {
			return err
		}
	case <-kv.ctx.Done():
		kv.logger.Warnf("canceled: %d", index)
		return errors.New(ErrWrongLeader)
	case <-time.After(time.Second):
		kv.logger.Warnf("timeout: %d", index)
		return errors.New("timeout")
	}
	return nil
}

func (kv *ShardKV) reconfigureLoop() {
	for {
		newConfig := kv.mck.Query(-1)
		kv.logger.Debugf("query newConfig %d", newConfig.Num)
		if newConfig.Num != kv.config.Num {
			kv.onRecvConfigUpdate(newConfig)
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
	labgob.Register(UpdateConfig{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = make_end
	kv.gid = gid
	kv.masters = masters
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.requests = make(map[int64]int64)

	kv.ctx, kv.cancel = context.WithCancel(context.Background())
	kv.logger = logging.NewLogger(logging.DebugLevel, true, func() string {
		_, isLeader := kv.rf.GetState()
		return fmt.Sprintf("[ShardKV] [Leader %v] [gid %d] [id %d]", isLeader, kv.gid, kv.me)
	})
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	go kv.processApply()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.reconfigureLoop()
	return kv
}
