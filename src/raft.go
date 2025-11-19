package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const HeartbeatInterval = 40
const ElectionTimeoutBase = 300
const RequestVoteBackOff = 100
const MaxNumEntriesSentPerEachHeartbeat = 100
const NumberOfStepsForFindingMatchedIndex = 1
const ApplierLoopInterval = 20

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	CurrentTerm       int
	VotedFor          int // nil is -1
	Log               []Entry
	lastIncludedIndex int
	lastIncludedTerm  int

	commitIndex            int
	lastApplied            int
	wasMatchedFollower     bool
	wasMatchedTermFollower int

	// Leader
	searchedIndex   []int
	wasTermSearched []bool
	matchedIndex    []int
	wasMatched      []bool

	roleState RoleState

	electionTimeoutState ElectionTimeoutState

	election ElectionSession

	exitSync ExitSyncContext

	applier chan raftapi.ApplyMsg

	installedSnapshotApplier chan InstalledSnapshotApplyMessage
}

type Entry struct {
	Term    int
	Command any
}

type RoleState struct {
	role           Role
	epochByRole    uint64
	roleTransition RoleTransitionContext
}

type RoleTransitionContext struct {
	knownEpochByRole   uint64
	roleTransitionChan chan RoleTransitionMsg
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
}

type RoleTransitionMsg struct {
	epochByRoleSnapshot uint64
	term                int
	oldRole             Role
	newRole             Role
}

type ElectionTimeoutState struct {
	resetFlag atomic.Uint32
}

type ElectionSession struct {
	voteCount int
	voting    map[int]Voter
}

type Voter struct {
	voted   bool
	granted bool
}

type ExitSyncContext struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type InstalledSnapshotApplyMessage struct {
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	replyChan         chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.roleState.role == Leader {
		return rf.CurrentTerm, true
	}
	// Your code here (3A).
	return rf.CurrentTerm, false
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persistNoLock(snapshot []byte) {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var err error

	if err = e.Encode(rf.CurrentTerm); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = e.Encode(rf.VotedFor); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = e.Encode(rf.Log); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = e.Encode(rf.lastIncludedIndex); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = e.Encode(rf.lastIncludedTerm); err != nil {
		log.Fatal("encode error:", err)
	}

	raftState := w.Bytes()

	if snapshot == nil {
		rf.persister.Save(raftState, rf.persister.ReadSnapshot())
	} else {
		rf.persister.Save(raftState, snapshot)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) bool {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return false
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTermPersisted int
	var votedForPersisted int
	var logPersisted []Entry
	var lastIncludedIndexPersisted int
	var lastIncludedTermPersisted int

	var err error

	if err = d.Decode(&currentTermPersisted); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = d.Decode(&votedForPersisted); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = d.Decode(&logPersisted); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = d.Decode(&lastIncludedIndexPersisted); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = d.Decode(&lastIncludedTermPersisted); err != nil {
		log.Fatal("encode error:", err)
	}

	rf.CurrentTerm = currentTermPersisted
	rf.VotedFor = votedForPersisted
	rf.Log = logPersisted
	rf.lastIncludedIndex = lastIncludedIndexPersisted
	rf.lastIncludedTerm = lastIncludedTermPersisted
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	return true
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.getLength() <= index {
		return
	}

	if rf.lastIncludedIndex >= index {
		return
	}

	term := rf.getEntryByIndex(index).Term

	tail := make([]Entry, rf.getLength()-index-1)

	j := 0
	for i := index + 1; i < rf.getLength(); i++ {

		tail[j] = rf.getEntryByIndex(i)
		j++
	}

	// atomic boundary => anyone failed, all rollbacked
	rf.Log = tail
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.persistNoLock(snapshot)

	log.Printf("<INFO> [Snapshotted] me: %v / last_included_index: %v / last_included_term: %v \n",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm,
	)
}

func (rf *Raft) SnapshotNoLock(index int, snapshot []byte) {
	if rf.getLength() <= index {
		return
	}

	if rf.lastIncludedIndex >= index {
		return
	}

	term := rf.getEntryByIndex(index).Term

	tail := make([]Entry, rf.getLength()-index-1)

	j := 0
	for i := index + 1; i < rf.getLength(); i++ {
		tail[j] = rf.getEntryByIndex(i)
		j++
	}

	// atomic boundary => anyone failed, all rollbacked
	rf.Log = tail
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.persistNoLock(snapshot)
}

func (rf *Raft) SnapshotByInstallNoLock(term int, index int, snapshot []byte) {
	if rf.getLength() <= index {
		if rf.lastIncludedIndex >= index {
			return
		}

		tail := make([]Entry, 0)

		rf.Log = tail
		rf.lastIncludedIndex = index
		rf.lastIncludedTerm = term
		rf.persistNoLock(snapshot)

	} else {
		rf.SnapshotNoLock(index, snapshot)
	}
}

func (rf *Raft) getLength() int {

	return len(rf.Log) + rf.lastIncludedIndex + 1

}

func (rf *Raft) getLastEntryTerm() int {

	if len(rf.Log) == 0 {
		return rf.lastIncludedTerm
	} else {
		return rf.Log[len(rf.Log)-1].Term
	}

}

func (rf *Raft) getLastEntryIndex() int {
	return rf.getLength() - 1
}

func (rf *Raft) getEntryByIndex(index int) Entry {

	if index <= rf.lastIncludedIndex || index >= rf.getLength() {
		panic(fmt.Sprintf("<ERROR> [Invalid use] me: %v / function_name: %v / length: %v / last_included_index: %v / accessed_index: %v  \n",
			rf.me, "getEntryByIndex", rf.getLength(), rf.lastIncludedIndex, index))

	}

	return rf.Log[rf.getIndexCompacted(index)]

}

func (rf *Raft) getIndexCompacted(index int) int {
	return index - rf.lastIncludedIndex - 1
}

func (rf *Raft) getTermByIndex(index int) int {

	if index < rf.lastIncludedIndex || index >= rf.getLength() {
		log.Fatalf("<ERROR> [Invalid use] me: %v, function_name: %v \n", rf.me, "getTermByIndex")
		return -1
	} else {
		if index == rf.lastIncludedIndex {
			return rf.lastIncludedTerm
		} else {
			return rf.Log[rf.getIndexCompacted(index)].Term
		}
	}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	if rf.roleState.role == Leader {
		isLeader = true

		index = rf.getLength()
		term = rf.CurrentTerm

		rf.Log = append(rf.Log, Entry{Term: term, Command: command})
		rf.persistNoLock(nil)

		log.Printf("<INFO> [Log was received from a client] me: %v / role: %v / term: %v / index: %v / command: %v \n",
			rf.me, rf.roleToStr(rf.roleState.role), rf.CurrentTerm, index, command)

	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {

	rf.exitInSync()
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) exitInSync() {

	log.Printf("<INFO> [Try to exit raft in sync] me: %v \n", rf.me)
	rf.exitSync.cancel()
	rf.exitSync.wg.Wait()
	log.Printf("<INFO> [Raft was exited in sync] me: %v\n", rf.me)
}

type RequestVoteArgs struct {
	Term         int
	CandidiateId int
	LastLogTerm  int
	LastLogIndex int
}

type RequestVoteReply struct {
	Term        int
	Voted       bool
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.Voted = false
		reply.VoteGranted = false
		return
	} else if rf.CurrentTerm == args.Term {
		reply.Term = rf.CurrentTerm
		reply.Voted = true

		if rf.roleState.role == Follower {
			// deduplication

			if rf.VotedFor == -1 {
				// validation
				if rf.validateCandidate(args.LastLogTerm, args.LastLogIndex) {
					rf.VotedFor = args.CandidiateId
					rf.persistNoLock(nil)

					reply.VoteGranted = true

					rf.resetElectionTimeout()
				} else {
					reply.VoteGranted = false
				}
			} else {
				if rf.VotedFor == args.CandidiateId {
					reply.VoteGranted = true
				} else {
					reply.VoteGranted = false
				}
			}
		} else {
			// not granted
			reply.VoteGranted = false
		}
	} else {
		if rf.roleState.role == Follower {
			// validation
			if rf.validateCandidate(args.LastLogTerm, args.LastLogIndex) {
				rf.VotedFor = args.CandidiateId
				reply.VoteGranted = true

				rf.resetElectionTimeout()
			} else {
				reply.VoteGranted = false
			}
		} else {
			rf.transitRole(Follower)

			// validation
			if rf.validateCandidate(args.LastLogTerm, args.LastLogIndex) {
				rf.VotedFor = args.CandidiateId
				reply.VoteGranted = true

				rf.resetElectionTimeout()
			} else {
				reply.VoteGranted = false
			}
		}

		rf.CurrentTerm = args.Term
		rf.persistNoLock(nil)

		reply.Term = rf.CurrentTerm
		reply.Voted = true
	}

}

func (rf *Raft) validateCandidate(candidateLastLogTerm int, candidateLastLogIndex int) bool {
	lastLogIndex := rf.getLastEntryIndex()

	if lastLogIndex < 0 {
		return true
	}

	lastLogTerm := rf.getLastEntryTerm()

	if lastLogTerm < candidateLastLogTerm ||
		(lastLogTerm == candidateLastLogTerm && lastLogIndex <= candidateLastLogIndex) {

		return true
	}

	return false
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	LeaderCommit      int
	SearchedIndex     int
	SearchedIndexTerm int
	WasTermSearched   bool
	WasMatched        bool
	ReplicatedIndex   int
	Entries           []Entry
}

type AppendEntriesReply struct {
	Term           int
	SearchSusscess bool
	XLen           int
	XIndex         int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		return
	}

	if rf.CurrentTerm == args.Term && rf.roleState.role == Leader {
		log.Fatalf("<ERROR> [More than one Leader observed in same term] me: %v \n", rf.me)
	}

	if (rf.CurrentTerm < args.Term && rf.roleState.role == Leader) ||
		(rf.CurrentTerm <= args.Term && rf.roleState.role == Candidate) {
		rf.transitRole(Follower)
	}

	rf.resetElectionTimeout()

	rf.CurrentTerm = args.Term
	rf.VotedFor = args.LeaderId
	reply.Term = rf.CurrentTerm

	if rf.wasMatchedTermFollower < args.Term {
		// first arrived AppendEntries RPC from current Leader
		rf.wasMatchedTermFollower = args.Term
		rf.wasMatchedFollower = false
	}

	if args.WasMatched {
		// args.WasMatched == true => rf.wasMatchedFollower == true
		// args.WasMatched == false => rf.wasMatchedFollower == true or false
		rf.wasMatchedFollower = true
	}

	if rf.wasMatchedFollower {
		reply.SearchSusscess = true

		if args.WasMatched {

			if len(args.Entries) > 0 && args.ReplicatedIndex >= rf.getLength() {
				appendedEntries := make([]Entry, args.ReplicatedIndex-rf.getLength()+1)
				i := 0
				for j := len(args.Entries) - (args.ReplicatedIndex - rf.getLength()) - 1; j < len(args.Entries); j++ {
					appendedEntries[i] = args.Entries[j]
					i++
				}
				rf.Log = append(rf.Log, appendedEntries...)

				log.Printf("<INFO> [Log was replicated] me: %v / role: %v / term: %v / leader: %v / index: %v / entries: %v  \n",
					rf.me, rf.roleToStr(rf.roleState.role), rf.CurrentTerm, args.LeaderId, args.ReplicatedIndex, appendedEntries)
			} else {
				// already replicated
				// response at this replicatedIndex might be dropped
				// stale
				// deduplication
			}
		} else {
			// stale
		}
	} else {
		if !args.WasMatched {

			reply.XLen = rf.getLength()

			if args.SearchedIndex < rf.getLength() {
				if args.WasTermSearched {
					if args.SearchedIndex <= rf.lastIncludedIndex {
						rf.Log = make([]Entry, 0) // no match after lastIncludedIndex

						log.Printf("<INFO> [Search was finished] me: %v / role: %v / term: %v / index: %v \n",
							rf.me, rf.roleToStr(rf.roleState.role), rf.CurrentTerm, rf.lastIncludedIndex)

						rf.wasMatchedFollower = true
						reply.SearchSusscess = true

					} else if rf.getTermByIndex(args.SearchedIndex) == args.SearchedIndexTerm {
						// first match with current Leader's AppendEntries RPC

						logOnlyMatchedCurrentLeader := make([]Entry, args.SearchedIndex-rf.lastIncludedIndex)
						for i := 0; i < args.SearchedIndex-rf.lastIncludedIndex; i++ {
							logOnlyMatchedCurrentLeader[i] = rf.Log[i]
						}
						rf.Log = logOnlyMatchedCurrentLeader

						log.Printf("<INFO> [Search was finished] me: %v / role: %v / term: %v / index: %v \n",
							rf.me, rf.roleToStr(rf.roleState.role), rf.CurrentTerm, args.SearchedIndex)

						rf.wasMatchedFollower = true
						reply.SearchSusscess = true
					} else {
						reply.SearchSusscess = false
					}
				} else {
					if args.SearchedIndex <= rf.lastIncludedIndex {
						reply.XIndex = rf.lastIncludedIndex
						reply.SearchSusscess = true

					} else if rf.getTermByIndex(args.SearchedIndex) == args.SearchedIndexTerm {
						// first term match
						toBeSearched := args.SearchedIndex

						for ; toBeSearched < rf.getLength(); toBeSearched++ {

							if rf.getEntryByIndex(toBeSearched).Term > args.SearchedIndexTerm {
								break
							}
						}

						toBeSearched--

						reply.XIndex = toBeSearched
						reply.SearchSusscess = true
					} else {
						reply.SearchSusscess = false
					}
				}
			} else {
				reply.SearchSusscess = false
			}
		} else {
			// never happen
		}
	}

	rf.persistNoLock(nil)

	if rf.wasMatchedFollower {
		// only after sync with current leader
		// if not, can apply an entry which has same index but different command
		index := rf.commitIndex

		rf.commitIndex = min(rf.getLength()-1, args.LeaderCommit)

		if index != rf.commitIndex {
			log.Printf("<INFO> [Commit index was updated] me: %v / role: %v / term: %v / commit_index: %v \n",
				rf.me, rf.roleToStr(rf.roleState.role), rf.CurrentTerm, rf.commitIndex)
		}

	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}

	if rf.CurrentTerm == args.Term && rf.roleState.role == Leader {
		log.Fatalf("<ERROR> [More than one Leader observed in same term] me: %v \n", rf.me)
	}

	if (rf.CurrentTerm < args.Term && rf.roleState.role == Leader) ||
		(rf.CurrentTerm <= args.Term && rf.roleState.role == Candidate) {
		rf.transitRole(Follower)
	}

	rf.CurrentTerm = args.Term
	reply.Term = rf.CurrentTerm

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		reply.Success = true
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock() // to avoid deadlock

	replyChan := make(chan bool, 1)

	msg := InstalledSnapshotApplyMessage{
		lastIncludedIndex: args.LastIncludedIndex,
		lastIncludedTerm:  args.LastIncludedTerm,
		snapshot:          args.Snapshot,
		replyChan:         replyChan,
	}

	rf.installedSnapshotApplier <- msg

	if <-replyChan {
		reply.Success = true

	}

	reply.Success = false
}

func (rf *Raft) runCandidate(epochByRoleSnapshot uint64) {
	rf.roleState.roleTransition.wg.Add(1)
	defer rf.roleState.roleTransition.wg.Done()

	rf.mu.Lock()
	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	rf.election = ElectionSession{}

	rf.election.voting = make(map[int]Voter)
	rf.election.voting[rf.me] = Voter{
		voted:   true,
		granted: true,
	}
	rf.election.voteCount++
	rf.mu.Unlock()

	log.Printf("<INFO> [Start an election] me: %v / term: %v \n", rf.me, rf.CurrentTerm)

	for i := range rf.peers {
		if rf.me != i {
			go rf.handleVotingPerPeer(epochByRoleSnapshot, i)
		}
	}

}

func (rf *Raft) handleVotingPerPeer(epochByRoleSnapshot uint64, peer int) {
	rf.roleState.roleTransition.wg.Add(1)
	defer rf.roleState.roleTransition.wg.Done()

	ticker := time.NewTicker(RequestVoteBackOff * time.Millisecond)
	defer ticker.Stop()

	voted := make(chan struct{})

	for {
		select {
		case <-ticker.C:
			go rf.sendRequestVote(epochByRoleSnapshot, peer, voted)
		case <-voted:
			return
		case <-rf.roleState.roleTransition.ctx.Done():
			return
		}
	}
}

func (rf *Raft) sendRequestVote(epochByRoleSnapshot uint64, peer int, voted chan struct{}) {
	rf.mu.Lock()

	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	if rf.election.voting[peer].voted {

		select {
		case voted <- struct{}{}:
		default:
		}

		rf.mu.Unlock()
		return
	}

	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidiateId: rf.me,
	}

	args.LastLogIndex = rf.getLastEntryIndex()
	args.LastLogTerm = rf.getLastEntryTerm()

	reply := &RequestVoteReply{}

	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)

	if !ok {
		// log.Printf("<ERROR> [RPC call was not responsed by network condition] me: %v / RPC method: %v \n",
		// 	rf.me, "RequestVote")
		return
	}

	rf.mu.Lock()

	// fenced by old epoch
	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	// election closed
	// transit to Follower
	if rf.CurrentTerm < reply.Term {

		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1

		rf.transitRole(Follower)

		rf.persistNoLock(nil)

		rf.mu.Unlock()

		return
	}

	if !reply.Voted {
		rf.mu.Unlock()
		return
	} else {
		// deduplicate already-voted voters
		if !rf.election.voting[peer].voted {
			if reply.VoteGranted {
				// granted
				rf.election.voting[peer] = Voter{
					voted:   true,
					granted: true,
				}
				rf.election.voteCount++

				log.Printf("<INFO> [Grant the vote] me: %v / term: %v / voter: %v \n", rf.me, rf.CurrentTerm, peer)

				// validation to be Leader
				if rf.election.voteCount >= (len(rf.peers)/2)+1 {

					log.Printf("<INFO> [Leader is elected] me: %v / term: %v \n", rf.me, rf.CurrentTerm)

					rf.transitRole(Leader)

					rf.persistNoLock(nil)
				}

			} else {
				rf.election.voting[peer] = Voter{
					voted:   true,
					granted: false,
				}

			}

			select {
			case voted <- struct{}{}:
			default:
			}
		}

	}

	rf.mu.Unlock()
}

func (rf *Raft) runLeader(epochByRoleSnapshot uint64) {
	rf.roleState.roleTransition.wg.Add(1)
	defer rf.roleState.roleTransition.wg.Done()

	heartbeatTicker := time.NewTicker(HeartbeatInterval * time.Millisecond)
	defer heartbeatTicker.Stop()

	rf.mu.Lock()
	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	rf.searchedIndex, rf.wasTermSearched, rf.matchedIndex, rf.wasMatched = make([]int, len(rf.peers)), make([]bool, len(rf.peers)), make([]int, len(rf.peers)), make([]bool, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.wasTermSearched[i] = false
			rf.wasMatched[i] = false

			toBeSearched := rf.getLength() - 1
			lastEntryTerm := rf.getLastEntryTerm()

			for ; toBeSearched >= rf.lastIncludedIndex; toBeSearched-- {

				if rf.getTermByIndex(toBeSearched) < lastEntryTerm {
					break
				}
			}

			toBeSearched++

			rf.searchedIndex[i] = toBeSearched

		} else {
			rf.matchedIndex[i] = rf.getLength() - 1
			rf.wasMatched[i] = true
		}
	}
	rf.mu.Unlock()

	tickCount := 1
	fanoutTick := make([]chan int, len(rf.peers))
	for i := range rf.peers {

		tickNotificationChan := make(chan int)
		fanoutTick[i] = tickNotificationChan

		go rf.runHeartbeatHandler(epochByRoleSnapshot, i, tickNotificationChan)

	}

	for {
		select {
		case <-heartbeatTicker.C:

			for _, tickNotificationChan := range fanoutTick {

				select {
				case tickNotificationChan <- tickCount:
				default:
					// cannot command sending a heartbeat to a  on current tick
				}

				tickCount++
			}

		case <-rf.roleState.roleTransition.ctx.Done():
			return
		}
	}

}

func (rf *Raft) runHeartbeatHandler(epochByRoleSnapshot uint64, peer int, tickNotificationChan chan int) {

	rf.roleState.roleTransition.wg.Add(1)
	defer rf.roleState.roleTransition.wg.Done()

	for {
		select {
		case tickCount := <-tickNotificationChan:

			go rf.sendAppendEntries(epochByRoleSnapshot, peer, tickCount)
		case <-rf.roleState.roleTransition.ctx.Done():
			// aborted by role update
			return
		}
	}

}

func (rf *Raft) sendAppendEntries(epochByRoleSnapshot uint64, peer int, tickCount int) {
	// no deadline for RPC requests
	// fencing by roleVersion is required
	rf.mu.Lock()

	// fence old role before sending RPC requests
	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	if peer == rf.me {
		rf.matchedIndex[peer] = rf.getLength() - 1
		rf.mu.Unlock()
		return
	}

	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	if rf.wasMatched[peer] {
		entriesToBeSent := make([]Entry, 0)
		if rf.matchedIndex[peer] < rf.lastIncludedIndex {
			go rf.sendInstallSnapshot(epochByRoleSnapshot, peer, tickCount)
		} else {
			for i := rf.matchedIndex[peer] + 1; i <= rf.matchedIndex[peer]+MaxNumEntriesSentPerEachHeartbeat; i++ {
				if i == rf.getLength() {
					break
				}

				entriesToBeSent = append(entriesToBeSent, rf.getEntryByIndex(i))
			}

		}

		args.ReplicatedIndex = rf.matchedIndex[peer] + len(entriesToBeSent)
		args.Entries = entriesToBeSent
		args.WasMatched = true
	} else {
		args.WasMatched = false
		if rf.searchedIndex[peer] < rf.lastIncludedIndex {
			go rf.sendInstallSnapshot(epochByRoleSnapshot, peer, tickCount)

			args.SearchedIndex = rf.lastIncludedIndex
			args.SearchedIndexTerm = rf.lastIncludedTerm
		} else {
			args.SearchedIndex = rf.searchedIndex[peer]
			args.SearchedIndexTerm = rf.getTermByIndex(rf.searchedIndex[peer])
		}

		args.WasTermSearched = rf.wasTermSearched[peer]
	}

	reply := &AppendEntriesReply{}

	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.AppendEntries", args,
		reply)

	if !ok {
		// drop
		return
	}

	rf.mu.Lock()

	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	// transit to Follower
	if rf.CurrentTerm < reply.Term {

		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1

		rf.transitRole(Follower)

		rf.persistNoLock(nil)

		rf.mu.Unlock()

		return
	}

	if rf.wasMatched[peer] {
		if args.WasMatched {

			if len(args.Entries) > 0 && args.ReplicatedIndex > rf.matchedIndex[peer] {
				rf.matchedIndex[peer] = args.ReplicatedIndex

				rf.commit()
			} else {
				// ignore => stale replication with lower index than latest view
			}

		} else {
			// if rf.wasMatched[peer] == true, args.WasMatched == true
			// stale
		}

		// reply.SearchSuccess => always-true

	} else {

		if rf.wasTermSearched[peer] {
			if args.WasTermSearched {
				if reply.SearchSusscess {
					rf.wasMatched[peer] = true
					rf.matchedIndex[peer] = args.SearchedIndex

					rf.commit()

				} else {
					if args.SearchedIndex <= rf.lastIncludedIndex {
						// must send InstallSnapshot RPC
						rf.searchedIndex[peer] = rf.lastIncludedIndex - 1
					} else {
						next := args.SearchedIndex - NumberOfStepsForFindingMatchedIndex

						if next < 0 {
							next = 0
						}

						if next < rf.lastIncludedIndex {
							rf.searchedIndex[peer] = rf.lastIncludedIndex
						} else {
							if next < rf.searchedIndex[peer] {
								rf.searchedIndex[peer] = next
							}
						}
					}
				}
			} else {
				// if rf.wasTermSearched[peer] == true, args.WasTermSearched == true
				// stale
			}

		} else {

			if reply.SearchSusscess {
				rf.wasTermSearched[peer] = true

				if args.SearchedIndexTerm < rf.lastIncludedTerm {
					// must send InstallSnapshot RPC
					rf.searchedIndex[peer] = rf.lastIncludedIndex - 1
				} else {
					toBeSearched := args.SearchedIndex

					if args.SearchedIndex < rf.lastIncludedIndex {
						toBeSearched = rf.lastIncludedIndex
					}

					for ; toBeSearched < rf.getLength(); toBeSearched++ {

						if rf.getTermByIndex(toBeSearched) > args.SearchedIndexTerm {
							break
						}
					}

					toBeSearched--

					if reply.XIndex >= rf.lastIncludedIndex && reply.XIndex < toBeSearched {
						rf.searchedIndex[peer] = reply.XIndex
					} else {
						rf.searchedIndex[peer] = toBeSearched
					}
				}

			} else {
				if args.SearchedIndex <= rf.lastIncludedIndex {
					// must send InstallSnapshot RPC
					rf.searchedIndex[peer] = rf.lastIncludedIndex - 1
				} else {
					toBeSearched := args.SearchedIndex - 1

					toBeSearchedTerm := rf.getTermByIndex(toBeSearched)

					for ; toBeSearched >= rf.lastIncludedIndex; toBeSearched-- {

						if rf.getTermByIndex(toBeSearched) < toBeSearchedTerm {
							break
						}
					}

					toBeSearched++

					if reply.XLen >= rf.lastIncludedIndex && reply.XLen < toBeSearched {
						if toBeSearched < rf.searchedIndex[peer] {
							rf.searchedIndex[peer] = reply.XLen
						}
					} else {
						if toBeSearched < rf.searchedIndex[peer] {
							rf.searchedIndex[peer] = toBeSearched
						}
					}
				}

			}
		}

	}

	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(epochByRoleSnapshot uint64, peer int, tickCount int) {
	rf.mu.Lock()

	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	if !rf.wasMatched[peer] && rf.searchedIndex[peer] >= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	if rf.wasMatched[peer] && rf.matchedIndex[peer] >= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	args := &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          rf.persister.ReadSnapshot(),
	}

	reply := &InstallSnapshotReply{}

	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args,
		reply)

	if !ok {
		// drop
		return
	}

	rf.mu.Lock()

	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	if rf.CurrentTerm < reply.Term {

		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1

		rf.transitRole(Follower)

		rf.persistNoLock(nil)

		rf.mu.Unlock()

		return
	}

	if !rf.wasMatched[peer] && rf.searchedIndex[peer] >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	if rf.wasMatched[peer] && rf.matchedIndex[peer] >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		if rf.wasMatched[peer] {
			rf.matchedIndex[peer] = args.LastIncludedIndex
		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) commit() bool {

	if rf.getLength() == 0 {
		// Committing entries from previous terms => at least one comitted entry at current term(to be the most up-to-date)
		return false
	}

	start := rf.commitIndex
	updated := start
	isCommittedAtCurrentTerm := false

	for indexToBeCommitted := start + 1; indexToBeCommitted < rf.getLength(); indexToBeCommitted++ {
		matchedCount := 0
		for j, _ := range rf.peers {
			if rf.matchedIndex[j] >= indexToBeCommitted {
				matchedCount++
			}
		}

		if matchedCount >= (len(rf.peers)/2)+1 {
			updated = indexToBeCommitted

			if rf.getTermByIndex(updated) == rf.CurrentTerm {
				isCommittedAtCurrentTerm = true
			}
		} else {
			break
		}
	}

	if start == updated {
		return false
	}

	if !isCommittedAtCurrentTerm {
		return false
	}

	rf.commitIndex = updated

	log.Printf("<INFO> [Commit index was updated] me: %v / role: %v / term: %v / commit_index_start: %v / commit_index_updated: %v / last_committed_term: %v \n",
		rf.me, rf.roleToStr(rf.roleState.role), rf.CurrentTerm, start, updated, rf.getTermByIndex(updated))

	return true
}

func (rf *Raft) runApplierLoop() {
	rf.exitSync.wg.Add(1)
	defer rf.exitSync.wg.Done()

	ticker := time.NewTicker(ApplierLoopInterval * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied {
				start := rf.lastApplied + 1
				rf.lastApplied = rf.commitIndex
				end := rf.commitIndex
				rf.mu.Unlock()

				for i := start; i <= end; i++ {
					if i == 0 {
						continue
					}

					e := rf.getEntryByIndex(i)

					msg := raftapi.ApplyMsg{
						CommandValid: true,
						Command:      e.Command,
						CommandIndex: i,
					}

					rf.applier <- msg
				}

				log.Printf("<INFO> [Log was applied] me: %v / last_applied_term: %v /last_applied_index: %v  \n",
					rf.me, rf.getTermByIndex(end), end)
			} else {
				rf.mu.Unlock()
			}
		case msg := <-rf.installedSnapshotApplier:
			rf.mu.Lock()
			if msg.lastIncludedIndex > rf.lastApplied {
				rf.SnapshotByInstallNoLock(msg.lastIncludedTerm, msg.lastIncludedIndex, msg.snapshot)
				rf.lastApplied = rf.lastIncludedIndex
				if rf.commitIndex < rf.lastApplied {
					rf.commitIndex = rf.lastApplied
				}
				rf.mu.Unlock()

				snapshotMsg := raftapi.ApplyMsg{
					// CommandValid: false,
					SnapshotValid: true,
					Snapshot:      msg.snapshot,
					SnapshotTerm:  msg.lastIncludedTerm,
					SnapshotIndex: msg.lastIncludedIndex,
				}

				rf.applier <- snapshotMsg

				log.Printf("<INFO> [Snapshot was installed] me: %v / last_included_term: %v /last_included_index: %v \n",
					rf.me, msg.lastIncludedTerm, msg.lastIncludedIndex)

			} else {
				rf.mu.Unlock()
			}

			msg.replyChan <- true

		case <-rf.exitSync.ctx.Done():
			ticker.Stop()
			log.Printf("<INFO [Applier loop was stopped] me: %v ", rf.me)
			return
		}
	}
}

func (rf *Raft) runRoleTransitioner() {
	rf.exitSync.wg.Add(1)
	defer rf.exitSync.wg.Done()

	for {
		select {
		case msg := <-rf.roleState.roleTransition.roleTransitionChan:

			if rf.roleState.roleTransition.knownEpochByRole > msg.epochByRoleSnapshot {
				break
			}

			log.Printf("<INFO> [Try to synchronize role transition] me %v / term: %v / old role: %v / new role: %v / Epoch: %v \n",
				rf.me, msg.term, rf.roleToStr(msg.oldRole), rf.roleToStr(msg.newRole), msg.epochByRoleSnapshot)

			rf.roleState.roleTransition.knownEpochByRole = msg.epochByRoleSnapshot

			if rf.roleState.roleTransition.ctx != nil && rf.roleState.roleTransition.cancel != nil {
				rf.roleState.roleTransition.cancel()
				rf.roleState.roleTransition.wg.Wait()
			}

			log.Printf("<INFO> [Role transition was synchronized] me %v / term: %v / old role: %v / new role: %v / Epoch: %v \n",
				rf.me, msg.term, rf.roleToStr(msg.oldRole), rf.roleToStr(msg.newRole), msg.epochByRoleSnapshot)

			if msg.newRole != Follower {
				rf.roleState.roleTransition.ctx, rf.roleState.roleTransition.cancel = context.WithCancel(context.Background())
			}

			switch msg.newRole {
			case Candidate:
				go rf.runCandidate(msg.epochByRoleSnapshot)
			case Leader:
				go rf.runLeader(msg.epochByRoleSnapshot)
			}

			rf.resetElectionTimeout()

		case <-rf.exitSync.ctx.Done():

			rf.exitRole()
			log.Printf("<INFO [Role transitioner loop was stopped] me: %v ", rf.me)
			return
		}
	}
}

func (rf *Raft) transitRole(newRole Role) {
	var term int
	var oldRole Role
	var nextEpoch uint64

	oldRole = rf.roleState.role
	rf.roleState.role = newRole
	rf.roleState.epochByRole++

	term = rf.CurrentTerm
	nextEpoch = rf.roleState.epochByRole

	log.Printf("<INFO> [Role was transitioned] me %v / term: %v / old role: %v / new role: %v / Epoch: %v \n",
		rf.me, term, rf.roleToStr(oldRole), rf.roleToStr(newRole), nextEpoch)

	go func() {

		msg := RoleTransitionMsg{
			epochByRoleSnapshot: nextEpoch,
			term:                term,
			oldRole:             oldRole,
			newRole:             newRole,
		}

		rf.roleState.roleTransition.roleTransitionChan <- msg

	}()

}

func (rf *Raft) exitRole() {
	// fence for active workers with current role
	rf.mu.Lock()
	rf.roleState.epochByRole++
	rf.mu.Unlock()

	// sync
	if rf.roleState.roleTransition.ctx != nil && rf.roleState.roleTransition.cancel != nil {
		rf.roleState.roleTransition.cancel()
		rf.roleState.roleTransition.wg.Wait()
	}

	// drain concurrent senders for role transition
	for {
		select {
		case <-rf.roleState.roleTransition.roleTransitionChan:

		default:
			return
		}
	}
}

func (rf *Raft) isFencedByRoleTransition(epochByRoleSnapshot uint64) bool {
	if epochByRoleSnapshot < rf.roleState.epochByRole {
		return true
	}

	return false
}

func (rf *Raft) runElectionTimeout() {
	rf.exitSync.wg.Add(1)
	defer rf.exitSync.wg.Done()

	newRandomTimer := func() *time.Timer {
		randomDuration := time.Duration(ElectionTimeoutBase+(rand.Int63()%ElectionTimeoutBase)) * time.Millisecond
		return time.NewTimer(randomDuration)
	}

	onTick := newRandomTimer()

	for {
		select {
		case <-onTick.C:
			onTick = newRandomTimer()

			if rf.shouldResetElectionTimeout() {
				rf.electionTimeoutState.resetFlag.Store(0)
			} else {

				rf.mu.Lock()
				if rf.roleState.role == Leader {
					rf.mu.Unlock()
					break
				}

				log.Printf("<INFO> [Election timeout was expired] me: %v / term: %v \n", rf.me, rf.CurrentTerm)

				rf.CurrentTerm++
				rf.VotedFor = rf.me

				rf.transitRole(Candidate)

				rf.persistNoLock(nil)

				rf.mu.Unlock()

			}

		case <-rf.exitSync.ctx.Done():
			onTick.Stop()
			log.Printf("<INFO [Election timeout loop was stopped] me: %v ", rf.me)
			return
		}
	}

}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeoutState.resetFlag.Store(1)
}

func (rf *Raft) shouldResetElectionTimeout() bool {
	if rf.electionTimeoutState.resetFlag.Load() == 1 {
		return true
	}
	return false
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize from state persisted before a crash

	if !rf.readPersist(persister.ReadRaftState()) {
		rf.VotedFor = -1
		rf.Log = make([]Entry, 0)
		dummyEntry := Entry{Term: 0, Command: nil}
		rf.Log = append(rf.Log, dummyEntry)

		rf.lastIncludedIndex = -1
		rf.lastIncludedTerm = -1
	}

	rf.applier = applyCh
	rf.roleState.roleTransition.roleTransitionChan = make(chan RoleTransitionMsg)
	rf.exitSync.ctx, rf.exitSync.cancel = context.WithCancel(context.Background())
	rf.installedSnapshotApplier = make(chan InstalledSnapshotApplyMessage)

	log.Printf("<INFO> [Raft node was started] me: %v \n", me)

	go rf.runRoleTransitioner()
	go rf.runElectionTimeout()
	go rf.runApplierLoop()

	return rf
}

func (rf *Raft) roleToStr(role Role) string {
	var str string

	switch role {
	case Follower:
		str = "Follower"
	case Candidate:
		str = "Candidate"
	case Leader:
		str = "Leader"
	}

	return str
}
