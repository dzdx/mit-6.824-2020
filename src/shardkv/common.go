package shardkv

import (
	"github.com/dzdx/mit-6.824-2020/src/shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	SeqID    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	SeqID    int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardMigrationArgs struct {
	Shard int
}
type ShardMigrationReply struct {
	Err      Err
	Data     map[string]string
	Requests map[int64]int64
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b

}

type UpdateConfig struct {
	Config   shardmaster.Config
	Data     map[string]string
	Requests map[int64]int64
}
