package kvraft

import (
	"../labgob"
	"../labrpc"
	"../logging"
	"../raft"
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	NotLeader = "not leader"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type requestFuture struct {
	index        int
	respCh       chan interface{}
	respClientID int64
	respSeqID    int64
}

type Op struct {
	ClientID int64
	SeqID    int64
	Cmd      string
	Key      string
	Value    string
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type seq struct {
	SeqID int64
	Time  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	inflightFutures sync.Map
	data            map[string]string
	requests        map[int64]*seq

	ctx         context.Context
	cancel      context.CancelFunc
	logger      *logging.Logger
	persister   *raft.Persister
	snapshoting int32
}

func (kv *KVServer) checkSnapshot(index int) {
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{
		ClientID: args.ClientID,
		SeqID:    args.SeqID,
		Cmd:      "Get",
		Key:      args.Key,
	}
	future := &requestFuture{
		respCh: make(chan interface{}, 1),
	}

	index, _, isLeader := kv.rf.Start(command)
	kv.inflightFutures.Store(index, future)
	defer kv.inflightFutures.Delete(index)
	if !isLeader {
		reply.Err = NotLeader
		return
	}
	kv.logger.Debugf("start %#v", command)
	select {
	case obj := <-future.respCh:
		if future.respSeqID == args.SeqID && future.respClientID == args.ClientID {
			reply.Value = obj.(string)
		} else {
			kv.logger.Warnf("conflicted: %d", index)
			reply.Err = "conflicted"
		}
	case <-kv.ctx.Done():
		kv.logger.Warnf("canceled: %d", index)
		reply.Err = "canceled"
	case <-time.After(time.Second):
		kv.logger.Warnf("timeout: %d", index)
		reply.Err = "timeout"
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{
		ClientID: args.ClientID,
		SeqID:    args.SeqID,
		Cmd:      args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	future := &requestFuture{
		respCh: make(chan interface{}, 1),
	}

	index, _, isLeader := kv.rf.Start(command)
	kv.inflightFutures.Store(index, future)
	defer kv.inflightFutures.Delete(index)
	if !isLeader {
		reply.Err = NotLeader
		return
	}
	kv.logger.Debugf("start %#v", command)
	select {
	case <-future.respCh:
		if !(future.respSeqID == args.SeqID && future.respClientID == args.ClientID) {
			kv.logger.Warnf("conflicted: %d", index)
			reply.Err = "conflicted"
		}
	case <-kv.ctx.Done():
		kv.logger.Warnf("canceled: %d", index)
		reply.Err = "canceled"
	case <-time.After(time.Second):
		kv.logger.Warnf("timeout: %d", index)
		reply.Err = "timeout"
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.logger.Infof("KVServer exit")
	kv.cancel()
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) cleanRequests() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-kv.ctx.Done():
			return
		case <-ticker.C:
			kv.mu.Lock()
			for k, call := range kv.requests {
				if time.Now().Unix()-call.Time > 10 {
					delete(kv.requests, k)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) processApplyCommand(msg raft.ApplyMsg) {
	command := msg.Command
	var resp interface{}
	op := command.(Op)
	if op.Cmd == "Get" {
		resp = kv.data[op.Key]
	} else {
		kv.mu.Lock()
		maxSeq, ok := kv.requests[op.ClientID]
		if !ok || op.SeqID > maxSeq.SeqID {
			kv.requests[op.ClientID] = &seq{
				SeqID: op.SeqID,
				Time:  time.Now().Unix(),
			}
			if op.Cmd == "Append" {
				kv.data[op.Key] = kv.data[op.Key] + op.Value
			} else {
				kv.data[op.Key] = op.Value
			}
		} else {
			kv.requests[op.ClientID].Time = time.Now().Unix()
		}
		kv.mu.Unlock()
		kv.checkSnapshot(msg.CommandIndex)
	}
	if obj, ok := kv.inflightFutures.Load(msg.CommandIndex); ok {
		future := obj.(*requestFuture)
		future.respClientID = op.ClientID
		future.respSeqID = op.SeqID
		kv.logger.Debugf("respond %#v", command)
		future.respCh <- resp
	}
}

func (kv *KVServer) processApplySnapshot(msg raft.ApplyMsg) {
	d := labgob.NewDecoder(bytes.NewBuffer(msg.SnapshotData))
	kv.data = make(map[string]string)
	d.Decode(&kv.data)
	d.Decode(&kv.requests)
	msg.SnapshotRespCh <- nil
}

func (kv *KVServer) processApply() {
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

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(seq{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.ctx, kv.cancel = context.WithCancel(context.Background())

	kv.data = make(map[string]string)
	kv.requests = make(map[int64]*seq)
	kv.logger = logging.NewLogger(logging.InfoLevel, true, func() string {
		return fmt.Sprintf("[KVServer] [id %d]", kv.me)
	})
	go kv.processApply()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.cleanRequests()
	return kv
}
