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
	log             map[int][]interface{}
	votedFor        int
	lastHeartbeat   time.Time
	electionTimeout time.Duration
	votes           int

	CurrentTerm int
	IsLeader    bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	NewTerm     int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.IsLeader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.NewTerm < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if args.NewTerm > rf.CurrentTerm {
		rf.CurrentTerm = args.NewTerm
		rf.IsLeader = false
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now() // Reset for next timeout
		rf.electionTimeout = time.Duration(50+(rand.Int63()%300)) * time.Millisecond
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.CurrentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.IsLeader = false
		rf.votedFor = -1
	}

	rf.lastHeartbeat = time.Now() // Reset for next timeout
	rf.electionTimeout = time.Duration(50+(rand.Int63()%300)) * time.Millisecond

	reply.Term = rf.CurrentTerm
	reply.Success = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func ContestElection(rf *Raft) {
	if time.Since(rf.lastHeartbeat) > rf.electionTimeout {
		// Start election
		rf.mu.Lock()
		rf.votedFor = rf.me
		rf.CurrentTerm++

		rf.lastHeartbeat = time.Now() // Reset for next timeout
		rf.mu.Unlock()

		wg := sync.WaitGroup{}

		// Send RequestVote RPCs to all peers
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			currentTerm := rf.CurrentTerm
			candidateId := rf.me
			wg.Add(1)
			go func(server int, term int, candidate int) {
				defer wg.Done()
				args := RequestVoteArgs{
					NewTerm:     term,
					CandidateId: candidate,
				}
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.CurrentTerm != term || rf.IsLeader != false {
						return
					}

					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.IsLeader = false
						rf.votedFor = -1
						rf.lastHeartbeat = time.Now()
						rf.electionTimeout = time.Duration(50+(rand.Int63()%300)) * time.Millisecond
						return
					}

					if reply.VoteGranted {
						rf.votes++
						if rf.votes > len(rf.peers)/2 {
							rf.IsLeader = true
							rf.lastHeartbeat = time.Now()
							rf.electionTimeout = time.Duration(50+(rand.Int63()%300)) * time.Millisecond
						}
					}
				}
			}(i, currentTerm, candidateId)
		}
		wg.Wait()
	}
}

func SendHeartbeat(rf *Raft) {
	// Send heartbeat to all peers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := AppendEntriesArgs{
				Term:     rf.CurrentTerm,
				LeaderId: rf.me,
			}
			reply := AppendEntriesReply{}
			rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.IsLeader = false
				rf.votedFor = -1
				rf.lastHeartbeat = time.Now()
				rf.electionTimeout = time.Duration(50+(rand.Int63()%300)) * time.Millisecond
			}
		}(i)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.IsLeader {
		return -1, -1, false
	}

	rf.log[rf.CurrentTerm] = append(rf.log[rf.CurrentTerm], command)
	index := len(rf.log[rf.CurrentTerm])

	return index, rf.CurrentTerm, true
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
	for rf.killed() == false {
		_, isLeader := rf.GetState()

		sleepTime := time.Duration(0)
		if isLeader {
			SendHeartbeat(rf)
			sleepTime = time.Duration(100) * time.Millisecond
		} else {
			ContestElection(rf)
			sleepTime = rf.electionTimeout // time.Duration(50+(rand.Int63()%300)) * time.Millisecond
		}

		time.Sleep(sleepTime)
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
	rf.mu = sync.Mutex{}

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.IsLeader = false
	rf.log = make(map[int][]interface{})
	rf.log[0] = []interface{}{} // initialize term 0 with empty list
	rf.votedFor = -1
	rf.votes = 0
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(50+(rand.Int63()%300)) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
