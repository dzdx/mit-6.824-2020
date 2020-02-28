package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func processReduceTask(reduceTask *ReduceTask, reducef func(string, []string) string) error {
	taskID := reduceTask.ID
	filenames := make([]string, 0)
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return err
	}
	reg := regexp.MustCompile(fmt.Sprintf("mr-\\d+-%d", taskID))
	for _, file := range files {
		if reg.MatchString(file.Name()){
			filenames = append(filenames, file.Name())
		}
	}
	kvs := make([]KeyValue, 0)
	for _, filename := range filenames {
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(f)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				} else {
					fmt.Printf("json decode error: %v\n", err)
					return err
				}
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}
	sort.Sort(ByKey(kvs))
	oname := fmt.Sprintf("mr-out-%d", reduceTask.ID)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	ofile.Close()
	return nil
}

func processMapTask(mapTask *MapTask, mapf func(string, string) []KeyValue) error {


	filename := mapTask.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %s", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read %s", filename)
	}
	file.Close()
	mapID := mapTask.ID
	groups := make(map[int][]KeyValue)
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % mapTask.NReduce
		groups[reduceID] = append(groups[reduceID], kv)
	}
	for reduceID, kvs := range groups {
		intername := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
		f, err := os.Create(intername)
		if err != nil {
			log.Fatalf("create inter file failed: %v", err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range kvs {
			if err := enc.Encode(&kv); err != nil {
				fmt.Printf("json encode error: %v\n", err)
				return err
			}
		}
		f.Close()
	}
	return nil

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		req := &AcquireTaskRequest{}
		reply := &AcquireTaskReply{}
		call("Master.AcquireTask", req, reply)
		if reply.MapTask != nil {
			fmt.Printf("worker recv map %d\n", reply.MapTask.ID)
			var err error
			err = processMapTask(reply.MapTask, mapf)
			ackReq := &TaskStateRequest{
				MapTask: reply.MapTask,
				Error:   err,
			}
			ackReply := &TaskStateReply{}
			call("Master.TaskState", ackReq, ackReply)
		}
		if reply.ReduceTask != nil {
			fmt.Printf("worker recv reduce %d\n", reply.ReduceTask.ID)
			err := processReduceTask(reply.ReduceTask, reducef)
			ackReq := &TaskStateRequest{
				ReduceTask: reply.ReduceTask,
				Error:      err,
			}
			ackReply := &TaskStateReply{}
			call("Master.TaskState", ackReq, ackReply)
		}
		time.Sleep(time.Millisecond*100)
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
