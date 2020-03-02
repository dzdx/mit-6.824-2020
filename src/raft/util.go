package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func AsyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
func randomDuration(base time.Duration) time.Duration {
	return time.Duration(rand.Float64()*float64(base)) + base
}
func backoffDuration(base time.Duration, retries int) time.Duration {
	if retries > 10 {
		retries = 10
	}
	return base * time.Duration(1<<(retries-1))
}
func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}
