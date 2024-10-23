package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	applyCh         chan ApplyMsg
	currentTerm     int
	votedFor        int
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	state           int
	counter         int
	total           int
	electionTimeout time.Duration
	receiveHB       bool
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	state := rf.state
	term = rf.currentTerm
	rf.mu.Unlock()
	if state == 2 {
		isleader = true
	}

	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		fmt.Println("Error decoding")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	} 

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	upToDate := args.LastLogTerm > lastLogTerm || (args.LastLogIndex >= lastLogIndex && args.LastLogTerm == lastLogTerm)
	if upToDate {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			rf.persist()
			reply.Term = rf.currentTerm
			rf.state = 0
			rf.receiveHB = true
			rf.mu.Unlock()
			return
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.state = 0
	rf.receiveHB = true
	rf.currentTerm = args.Term
	rf.persist()

	notInLog := args.PrevLogIndex >= len(rf.log)
	if notInLog {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	entries := []LogEntry{}
	for i := 0; i < len(args.Entries); i++ {
		if len(rf.log) <= args.PrevLogIndex+i+1 {
			entries = args.Entries[i:]
			break
		}
		if rf.log[args.PrevLogIndex+i+1].Command != args.Entries[i].Command || rf.log[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
			rf.log = rf.log[0 : args.PrevLogIndex+i+1]
			rf.persist()
			entries = args.Entries[i:]
			break
		}
	}
	rf.log = append(rf.log, entries...)
	rf.persist()


	LastEntryIndex := len(rf.log) - 1
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < LastEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = LastEntryIndex
		}
	}

	reply.Success = true
	rf.currentTerm = args.Term
	rf.persist()
	reply.Term = rf.currentTerm
	rf.state = 0
	rf.receiveHB = true
	rf.votedFor = args.LeaderID
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	if rf.state != 2 {
		term = rf.currentTerm
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}

	term = rf.currentTerm
	newEntry := LogEntry{command, term}
	rf.log = append(rf.log, newEntry)
	index = len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	rf.persist()
	rf.mu.Unlock()
	rf.sendHeartbeat()

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				msg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
				rf.applyCh <- msg
			}
		}

		if rf.state == 2 {
			Time := 150
			rf.electionTimeout = time.Millisecond * time.Duration(Time)
			rf.mu.Unlock()
		} else {
			randomTime := rand.Intn(150) + 300
			rf.electionTimeout = time.Millisecond * time.Duration(randomTime)
			rf.mu.Unlock()
		}

		time.Sleep(rf.electionTimeout)
		rf.mu.Lock()

		if (rf.state == 0 && rf.votedFor == -1 && !rf.receiveHB) {
			rf.currentTerm += 1
			rf.mu.Unlock()
			rf.becomeCandidate()
			rf.mu.Lock()
			if rf.counter > rf.total/2 {
				rf.matchIndex[rf.me] = len(rf.log) - 1
				if rf.matchIndex[rf.me] == -1 {
					rf.matchIndex[rf.me] = 0
				}
				rf.state = 2
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						rf.matchIndex[i] = 0
					}
					rf.nextIndex[i] = len(rf.log)
				}
				rf.mu.Unlock()
				rf.sendHeartbeat()
			} else {
				rf.state = 0
				rf.mu.Unlock()
			}
		} else if rf.state == 2 {
			rf.mu.Unlock()
			rf.sendHeartbeat()
		} else {
			rf.votedFor = -1
			rf.receiveHB = false
			rf.state = 0
			rf.mu.Unlock()
		}

		rf.mu.Lock()
		rf.counter = 0
		if rf.state == 0 {
			rf.votedFor = -1
			rf.receiveHB = false
		}
		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.state = 1
	rf.votedFor = rf.me
	rf.currentTerm += 1
	rf.persist()
	rf.counter = 1
	rf.receiveHB = true
	LastLogIndex := len(rf.log) - 1
	LastLogTerm := rf.log[LastLogIndex].Term
	args := RequestVoteArgs{rf.currentTerm, rf.me, LastLogIndex, LastLogTerm}
	rf.mu.Unlock()
	for i := 0; i < rf.total; i++ {
		if i != rf.me {
			go func (server int) {
				reply := RequestVoteReply{}
				reply.VoteGranted = false
				ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				if reply.VoteGranted {
					rf.counter += 1
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = 0
						rf.votedFor = -1
						rf.persist()
						rf.receiveHB = true
						rf.mu.Unlock()
						return
					}
				}
				rf.mu.Unlock()
			} (i)
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) sendHeartbeat() {
	for i := 0; i < rf.total; i++ {
		if i != rf.me {
			go func(i int) {
					rf.mu.Lock()
					prevLogIndex := rf.nextIndex[i] - 1
					prevTerm := rf.log[prevLogIndex].Term
					args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevTerm, []LogEntry{}, rf.commitIndex}
					if rf.matchIndex[rf.me] >= rf.nextIndex[i] {
						args.Entries = rf.log[rf.nextIndex[i]:]
					}
					reply := AppendEntriesReply{}
					reply.Success = false
					rf.mu.Unlock()
					ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
					if !ok {
						return
					}
					rf.mu.Lock()

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = 0
						rf.receiveHB = true
						rf.persist()
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						rf.matchIndex[i] = prevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
					} else {
						rf.nextIndex[i] = 1
					}
					rf.mu.Unlock()
			} (i)
		}
	}
	rf.mu.Lock()
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		c := 0
		for i := 0; i < rf.total; i++ {
			if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
				c += 1
			}
		}
		if c > rf.total/2 {
			rf.commitIndex = N
			rf.persist()
			break
		}
	}
	rf.mu.Unlock()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{nil, 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = 0
	rf.counter = 0
	rf.total = len(peers)
	randomTime := rand.Intn(150) + 300
	rf.electionTimeout = time.Millisecond * time.Duration(randomTime)
	rf.receiveHB = false

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
