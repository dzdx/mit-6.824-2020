package kvraft

import (
	"../labgob"
	"../labrpc"
	"../logging"
	"../raft"
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
	index  int
	respCh chan interface{}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type call struct {
	value interface{}
	time  time.Time
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
	calls           map[int64]*call

	ctx    context.Context
	cancel context.CancelFunc
	logger *logging.Logger
}

func (kv *KVServer) serve(command interface{}, reqID int64) (interface{}, error) {
	future := &requestFuture{
		respCh: make(chan interface{}, 1),
	}
	kv.inflightFutures.Store(reqID, future)
	defer kv.inflightFutures.Delete(reqID)
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return "", fmt.Errorf(NotLeader)
	}
	kv.logger.Debugf("start %#v", command)
	select {
	case obj := <-future.respCh:
		return obj, nil
	case <-kv.ctx.Done():
		kv.logger.Warnf("canceled: %d", reqID)
		return "", fmt.Errorf("canceled")
	case <-time.After(2 * time.Second):
		kv.logger.Warnf("timeout: %d", reqID)
		return "", fmt.Errorf("timeout")
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	resp, err := kv.serve(*args, args.ID)
	if err != nil {
		reply.Err = Err(err.Error())
		return
	}
	reply.Value = resp.(string)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, err := kv.serve(*args, args.ID)
	if err != nil {
		reply.Err = Err(err.Error())
	}
	return
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
	kv.cancel()
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) cleanCalls() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-kv.ctx.Done():
			return
		case <-ticker.C:
			kv.mu.Lock()
			for k, call := range kv.calls {
				if time.Since(call.time) > 10*time.Second {
					delete(kv.calls, k)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) processApply() {
	for {
		select {
		case <-kv.ctx.Done():
			return
		case msg := <-kv.applyCh:
			var resp interface{}
			var reqID int64
			switch args := msg.Command.(type) {
			case GetArgs:
				reqID = args.ID
				resp = kv.data[args.Key]
			case PutAppendArgs:
				kv.mu.Lock()
				if _, ok := kv.calls[args.ID]; !ok {
					if args.Op == "Append" {
						kv.data[args.Key] = kv.data[args.Key] + args.Value
					} else {
						kv.data[args.Key] = args.Value
					}
					kv.calls[args.ID] = &call{time: time.Now()}
				}
				kv.mu.Unlock()
				reqID = args.ID
			}
			if obj, ok := kv.inflightFutures.Load(reqID); ok {
				future := obj.(*requestFuture)
				kv.logger.Debugf("respond %#v", msg.Command)
				future.respCh <- resp
				break
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
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.calls = make(map[int64]*call)
	kv.ctx, kv.cancel = context.WithCancel(context.Background())
	kv.logger = logging.NewLogger(logging.InfoLevel, true, func() string {
		return fmt.Sprintf("[KVServer] [id %d]", kv.me)
	})

	// You may need initialization code here.
	go kv.processApply()
	go kv.cleanCalls()

	return kv
}
