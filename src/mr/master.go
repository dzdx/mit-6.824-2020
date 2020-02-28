package mr

import (
	"io/ioutil"
	"log"
	"regexp"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	taskUnstarted int = iota
	taskReserved
	taskFinished
)

type mapTask struct {
	file      string
	taskID    int
	state     int
	startTime time.Time
}
type reduceTask struct {
	taskID    int
	state     int
	startTime time.Time
}

type Master struct {
	// Your definitions here.
	mapTasks       []mapTask
	reduceTasks    []reduceTask
	stagingReduce  bool
	nReduce        int
	mutex          sync.Mutex
	mapFinished    bool
	done           bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (m *Master) TaskState(args *TaskStateRequest, reply *TaskStateReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if args.MapTask != nil {
		if args.Error == nil {
			m.mapTasks[args.MapTask.ID].state = taskFinished
		} else {
			m.mapTasks[args.MapTask.ID].state = taskUnstarted
		}
	}
	if args.ReduceTask != nil {
		if args.Error == nil {
			m.reduceTasks[args.ReduceTask.ID].state = taskFinished
		} else {
			m.reduceTasks[args.ReduceTask.ID].state = taskUnstarted
		}
	}
	return nil
}

func (m *Master) AcquireTask(args *AcquireTaskRequest, reply *AcquireTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if !m.mapFinished {
		var task *mapTask
		m.mapFinished = true
		func() {
			for i := range m.mapTasks {
				switch m.mapTasks[i].state {
				case taskUnstarted:
					m.mapTasks[i].state = taskReserved
					m.mapTasks[i].startTime = time.Now()
					task = &m.mapTasks[i]
					m.mapFinished = false
					return
				case taskReserved:
					m.mapFinished = false
				}
			}
		}()
		if task != nil {
			reply.MapTask = &MapTask{
				ID:      task.taskID,
				NReduce: m.nReduce,
				File:    task.file,
			}
		}
	} else {
		var task *reduceTask
		func() {
			for i := range m.reduceTasks {
				switch m.reduceTasks[i].state {
				case taskUnstarted:
					m.reduceTasks[i].state = taskReserved
					m.reduceTasks[i].startTime = time.Now()
					task = &m.reduceTasks[i]
					return
				}
			}
		}()
		if task != nil {
			reply.ReduceTask = &ReduceTask{
				ID: task.taskID,
			}
		}
	}
	return nil
}

func (m *Master) retryTaskLoop() {
	for {
		finish := true
		now := time.Now()
		m.mutex.Lock()
		for i := range m.mapTasks {
			if m.mapTasks[i].state != taskFinished {
				finish = false
			}
			if m.mapTasks[i].state == taskReserved && now.Sub(m.mapTasks[i].startTime) > 10*time.Second {
				m.mapTasks[i].state = taskUnstarted
			}
		}
		for i := range m.reduceTasks {
			if m.reduceTasks[i].state != taskFinished {
				finish = false
			}
			if m.reduceTasks[i].state == taskReserved && now.Sub(m.reduceTasks[i].startTime) > 10*time.Second {
				m.reduceTasks[i].state = taskUnstarted
			}
		}
		m.mutex.Unlock()
		if finish {
			m.done = true
		}
		time.Sleep(time.Second)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.

	return m.done
}

func cleanIntermediateFiles() {
	flist, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}
	reg := regexp.MustCompile("mr-\\d+-\\d+")
	for _, file := range flist {
		if reg.MatchString(file.Name()) {
			os.Remove(file.Name())
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	fileCount := len(files)
	mapTasks := make([]mapTask, len(files))
	reduceTasks := make([]reduceTask, nReduce)
	for i := 0; i < fileCount; i++ {
		mapTasks[i] = mapTask{
			file:   files[i],
			taskID: i,
			state:  taskUnstarted,
		}
	}
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = reduceTask{
			taskID: i,
			state:  taskUnstarted,
		}
	}
	m := Master{
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
		nReduce:     nReduce,
	}
	cleanIntermediateFiles()
	go m.retryTaskLoop()
	m.server()
	return &m
}
