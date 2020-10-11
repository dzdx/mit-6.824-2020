package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"github.com/dzdx/mit-6.824-2020/src/labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientID   int64
	seqID      int64
	lastLeader int
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
	ck.clientID = nrand()
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	seqID := atomic.AddInt64(&ck.seqID, 1)
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientID = ck.clientID
	args.SeqID = seqID
	i := ck.lastLeader
	for {
		// try each known server.
		for range ck.servers {
			srv := ck.servers[i]
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return reply.Config
			}
			i = (i + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	seqID := atomic.AddInt64(&ck.seqID, 1)
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientID = ck.clientID
	args.SeqID = seqID

	i := ck.lastLeader
	for {
		// try each known server.
		for  range ck.servers {
			srv := ck.servers[i]
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
			i = (i+1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	seqID := atomic.AddInt64(&ck.seqID, 1)
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientID = ck.clientID
	args.SeqID = seqID

	i := ck.lastLeader
	for {
		// try each known server.
		for range ck.servers {
			srv := ck.servers[i]
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
			i = (i+1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	seqID := atomic.AddInt64(&ck.seqID, 1)
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientID = ck.clientID
	args.SeqID = seqID

	i := ck.lastLeader
	for {
		// try each known server.
		for  range ck.servers {
			srv := ck.servers[i]
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader =i
				return
			}
			i = (i+1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
