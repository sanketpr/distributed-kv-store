package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int32
	votedFor    int
	logs        []struct {
		int32
		string
	}

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be commited
	// lastApplied int // Index of higest log entry applied to state machine

	// Volatile state on leader
	// nextIndex  []int // For each server, index of the next log entry to send to that server
	// matchIndex []int // For each server, index of highest log entry known to be replicated on server

	lastCommitIndex   int32
	lastHeartBeatTime time.Time
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int32
	CandiateId   int
	LastLogIndex int32
	LastLogTerm  int32
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int32
	Entries      []string
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("### GetState: Term: %d, Id: %d, VotedFor: %d\n", rf.currentTerm, rf.me, rf.votedFor)
	return int(rf.currentTerm), rf.votedFor == rf.me
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// fmt.Printf("Request From: %d, To: %d, Term: %d\n", args.CandiateId, rf.me, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = int(rf.currentTerm)
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Term = int(rf.currentTerm)
	// Check if we have already voted for someone else in this term
	if (rf.votedFor == -1 || rf.votedFor == args.CandiateId) && candidateLogIsUpToDate(args, rf) {
		rf.votedFor = args.CandiateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func candidateLogIsUpToDate(args *RequestVoteArgs, rf *Raft) bool {
	rfLastIndex := int32(len(rf.logs) - 1)
	rfLastTerm := int32(0)
	if rfLastIndex >= 0 {
		rfLastTerm = rf.logs[rfLastIndex].int32
	}
	if args.LastLogTerm > rfLastTerm {
		return true
	}
	if args.LastLogTerm == rfLastTerm && args.LastLogIndex >= rfLastIndex {
		return true
	}

	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: if rf.currentTerm == args.Term && rf.votedFor == args.LeaderId
	// {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartBeatTime = time.Now()
	// }

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
		// TODO: Add empty entries to logs if current node term is behind
		// } else if args.term > rf.currentTerm {
		// for range args.term - rf.currentTerm {
		// 	_ = append(rf.logs, []string{})
		// }
		// rf.currentTerm = int32(args.term)
	}
	// TODO: verify logs length
	// else if rf.currentTerm == args.prevLogTerm && len(rf.logs[rf.currentTerm-1]) == args.prevLogIndex+1 {
	// 	_ = append(rf.logs[rf.currentTerm-1], args.entries[args.leaderCommit])
	// }
	rf.lastCommitIndex = int32(math.Min(float64(args.Term), float64(args.PrevLogTerm)))
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	reply.Success = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// fmt.Printf("Response From: %d, To: %d, Term: %d, Reply: %t\n", rf.me, server, rf.currentTerm, reply.VoteGranted)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		ms := 50 + (rand.Int63() % 300)
		timeCheck := time.Now()
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (3A)
		_, isLeader := rf.GetState()

		if isLeader {
			// send heart beat
			sendHeartBeat(rf)
		} else {
			// change to candidate state and request votes to be a leader
			// Check if a leader election should be started.
			// fmt.Printf("Id: %d, Last heart beat: %s\n", rf.me, rf.lastHeartBeatTime)
			if rf.lastHeartBeatTime.Before(timeCheck) {
				startElectionProcess(rf)
			}
		}
	}
}

func startHeartBeating(rf *Raft) {
	for !rf.killed() {
		ms := 20
		time.Sleep(time.Duration(ms) * time.Millisecond)
		_, isLeader := rf.GetState()
		if isLeader {
			sendHeartBeat(rf)
		} else {
			break
		}
	}
}

func sendHeartBeat(rf *Raft) {
	rf.mu.Lock()
	rf.lastHeartBeatTime = time.Now()
	rf.mu.Unlock()
	for index, _ := range rf.peers {
		request := createAppendEntriesRpcArgsPayload(rf)
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(index, &request, &reply)
	}
}

func startElectionProcess(rf *Raft) {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	term := rf.currentTerm
	rf.mu.Unlock()

	votesCount := 1
	var mu sync.Mutex
	var wg sync.WaitGroup

	shouldCancelElection := false

	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		if shouldCancelElection {
			votesCount = math.MinInt32
			break
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := createRequestVoteRpcArgsPayload(rf)
			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > int(rf.currentTerm) {
					rf.currentTerm = int32(reply.Term)
					rf.votedFor = -1
					shouldCancelElection = true
					return
				}
				if reply.VoteGranted && term == int32(reply.Term) {
					mu.Lock()
					votesCount++
					mu.Unlock()
				}
			}
		}(index)
	}
	wg.Wait()
	if votesCount > int(math.Ceil(float64(len(rf.peers))/float64(2))) && !shouldCancelElection {
		// fmt.Printf("* Elected Id: %d, Term: %d, Votes count: %d\n", rf.me, rf.currentTerm, votesCount)
		// Become leader
		go startHeartBeating(rf)
	} else {
		rf.mu.Lock()
		rf.votedFor = -1
		rf.mu.Unlock()
		// fmt.Printf("* NOT Elected Id: %d, Term: %d, Votes count: %d\n", rf.me, rf.currentTerm, votesCount)
	}
}

func createRequestVoteRpcArgsPayload(rf *Raft) RequestVoteArgs {
	return RequestVoteArgs{
		Term:         rf.currentTerm,
		CandiateId:   rf.me,
		LastLogIndex: rf.lastCommitIndex,
		LastLogTerm:  0, // TODO
	}
}

func createAppendEntriesRpcArgsPayload(rf *Raft) AppendEntriesArgs {
	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.commitIndex,
		PrevLogTerm:  rf.currentTerm, // TODO: get the actual prev commit log term from logs.last()[1],
		Entries:      []string{},
		LeaderCommit: rf.commitIndex,
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

// Wait for WaitGroup with timeout
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true // completed normally
	case <-time.After(timeout):
		return false // timed out
	}
}
