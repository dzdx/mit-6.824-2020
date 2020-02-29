package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func AsyncNotify(ch chan struct{}){
	select {
	case ch<- struct{}{}:
	default:
	}
}
func MinUint64(a, b uint64) uint64{
	if a < b{
		return  a
	}
	return b
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}
