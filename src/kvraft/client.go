package kvraft

import (
	"../labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// You will have to modify this function.
	args := &GetArgs{
		Key: key,
		ID:  nrand(),
	}
	count := len(ck.servers)
	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		for i := ck.leader; i < ck.leader+count; i++ {
			index := i % count
			reply := &GetReply{}
			if ok := ck.servers[index].Call("KVServer.Get", args, reply); ok {
				if len(reply.Err) == 0 {
					ck.leader = index
					return reply.Value
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return ""
}

//
// shared by Put and Append.
//
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ID:    nrand(),
	}
	count := len(ck.servers)
	t0 := time.Now()
	for time.Since(t0).Seconds() < 5 {
		for i := ck.leader; i < ck.leader+count; i++ {
			index := i % count
			reply := &PutAppendReply{}
			if ok := ck.servers[index].Call("KVServer.PutAppend", args, reply); ok {
				if len(reply.Err) == 0 {
					ck.leader = index
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
