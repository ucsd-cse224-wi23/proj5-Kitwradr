package surfstore

import (
	context "context"
	"fmt"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	id            int64
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	commitIndex   int64
	lastApplied   int64
	log           []*UpdateOperation
	peers         []string
	metaStore     *MetaStore

	matchIndex []int64 //for each server, index of the next log entry
	//to send to that server (initialized to leader
	//last log index + 1)
	nextIndex []int64 //for each server, index of highest log entry
	//known to be replicated on server
	//(initialized to 0, increases monotonically)

	pendingCommits []*chan bool

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetIsLeader() bool {
	s.isLeaderMutex.RLock()
	defer s.isLeaderMutex.RUnlock()
	return s.isLeader
}

func (s *RaftSurfstore) GetIsCrashed() bool {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()
	return s.isCrashed
}

// If the node is the leader, and if a majority of the nodes are working, should return the correct answer;
// if a majority of the nodes are crashed, should block until a majority recover.  If not the leader, should indicate an error back to the client
func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Execute only if server is not crashed
	if !s.GetIsCrashed() {
		// If server is leader, return the file info map
		if s.GetIsLeader() {
			if s.checkMajority(ctx) {
				return s.metaStore.GetFileInfoMap(ctx, empty)
			} else {
				return nil, ERR_NO_MAJORITY
			}
		} else {
			return nil, ERR_NOT_LEADER
		}
	} else {
		return nil, ERR_SERVER_CRASHED
	}
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Execute only if server is not crashed
	if !s.GetIsCrashed() {
		// If server is leader, return the block store map
		if s.GetIsLeader() {
			if s.checkMajority(ctx) {
				return s.metaStore.GetBlockStoreMap(ctx, hashes)
			} else {
				return nil, ERR_NO_MAJORITY
			}
		} else {
			return nil, ERR_NOT_LEADER
		}
	} else {
		return nil, ERR_SERVER_CRASHED
	}
}

// If the node is the leader, and if a majority of the nodes are working, should return the correct answer; if a majority of the nodes are crashed, should block until a majority recover.
// If not the leader, should indicate an error back to the client
func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Execute only if server is not crashed
	if !s.GetIsCrashed() {
		// If server is leader, return the block store addresses
		if s.GetIsLeader() {
			if s.checkMajority(ctx) {
				return s.metaStore.GetBlockStoreAddrs(ctx, empty)
			} else {
				return nil, ERR_NO_MAJORITY
			}
		} else {
			return nil, ERR_NOT_LEADER
		}
	} else {
		return nil, ERR_SERVER_CRASHED
	}
}

func (s *RaftSurfstore) checkMajority(ctx context.Context) bool {
	fmt.Println("---Checking majority---")
	_, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		fmt.Println("Error in sending heartbeat", err)
		return false
	}
	fmt.Println("---Majority check successful---", true)
	return true
}

// If the node is the leader, and if a majority of the nodes are working, should return the correct answer;
// if a majority of the nodes are crashed, should block until a majority recover.  If not the leader, should indicate an error back to the client
func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {

	// append entry to our log
	entry := &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, entry)

	fmt.Println("Appending entry on leader", s.id+1, "entry", entry)
	fmt.Println("Current log on leader", s.id+1, "log", s.log)

	s.checkMajority(ctx)

	// Execute only if server is not crashed
	if !s.GetIsCrashed() {
		// If server is leader, update the file
		if s.GetIsLeader() {
			version, err := s.metaStore.UpdateFile(ctx, filemeta)
			if err != nil {
				fmt.Println("Error in updating file")
				return nil, err
			} else {
				fmt.Println("File updated successfully, new log is ", s.log)
			}
			s.commitIndex += 1
			//commit to followers - part 2 of two phase commit
			fmt.Println("Committing to followers")
			s.checkMajority(ctx)
			return version, err
		} else {
			return nil, ERR_NOT_LEADER
		}
	} else {
		return nil, ERR_SERVER_CRASHED
	}

	//}

	//return nil, nil

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// Execute only if server is not crashed
	var appendEntryOutput = &AppendEntryOutput{}
	if !s.isCrashed {

		// 1. Reply false if term < currentTerm
		if input.Term > s.term {
			fmt.Println("1 Returning false in AppendEntries", input.Term, s.term)
			// Resolving conflicting leader
			s.isLeaderMutex.Lock()
			defer s.isLeaderMutex.Unlock()
			s.isLeader = false
			//update the term of the follower
			s.term = input.Term
			appendEntryOutput.Success = false
			return appendEntryOutput, nil
		}

		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		if input.PrevLogIndex >= 0 && input.PrevLogIndex < int64(len(s.log)) {

			if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
				fmt.Println("2 Returning false in AppendEntries", s.log[input.PrevLogIndex].Term, input.PrevLogTerm)
				appendEntryOutput.Success = false
				return appendEntryOutput, nil
			}
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		if input.PrevLogIndex+1 < int64(len(s.log)) {
			s.log = s.log[:input.PrevLogIndex+1]
		}

		// 4. Append any new entries not already in the log

		if len(input.Entries) > 0 {
			fmt.Println("Appending entries on server", s.id+1, input.Entries)
			s.log = append(s.log, input.Entries...)
			fmt.Println("Current log --->", s.log)
		}

		appendEntryOutput.Success = true
		appendEntryOutput.MatchedIndex = int64(len(s.log) - 1)
		appendEntryOutput.Term = s.term

		// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if input.LeaderCommit > s.commitIndex {
			s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
		}

		// Execute the entries in the log
		for s.lastApplied < input.LeaderCommit {
			entry := s.log[s.lastApplied+1]
			fmt.Println("Committing entry in follower", s.id+1, entry)
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
			s.lastApplied++
		}

		return appendEntryOutput, nil
	} else {
		appendEntryOutput.Success = false
		return appendEntryOutput, ERR_SERVER_CRASHED
	}
}

// Emulates elections, sets the node to be the leader
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if !s.isCrashed {
		s.isLeaderMutex.Lock()
		s.isLeader = true
		s.term += 1
		s.matchIndex = make([]int64, len(s.peers))
		initializeArray(s.matchIndex, -1)
		s.nextIndex = make([]int64, len(s.peers))
		initializeArray(s.nextIndex, int64(len(s.log)))
		s.isLeaderMutex.Unlock()
		fmt.Println("Leader set successfully", s.id+1)
	} else {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	return &Success{Flag: true}, nil
}

func initializeArray(array []int64, value int64) {
	for i := range array {
		array[i] = value
	}
}

// Sends a round of AppendEntries to all other nodes. The leader will attempt to replicate logs to all other nodes when this is called.
// It can be called even when there are no entries to replicate. If a node is not in the leader state it should do nothing.
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("SendHeartbeat from server: ", s.id+1)
	if s.GetIsCrashed() {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	} else {
		//wg := sync.WaitGroup{}
		if s.GetIsLeader() {
			majority := false
			for {
				if s.GetIsCrashed() || !s.GetIsLeader() {
					break
				}
				totalAppends := 1
				for index, server_addr := range s.peers {

					if int64(index) != s.id {
						//wg.Add(1)
						fmt.Println("Sending heartbeat to follower", index+1, "from leader", s.id+1, "state of leader", s.GetIsCrashed())
						appendEntryInput := createAppendEntry(s, index)
						//go s.sendToFollower(server_addr, appendEntryInput)

						conn, _ := grpc.Dial(server_addr, grpc.WithInsecure())
						client := NewRaftSurfstoreClient(conn)

						if s.GetIsCrashed() || !s.GetIsLeader() {
							break
						}
						output, err := client.AppendEntries(ctx, appendEntryInput)

						if err != nil {
							fmt.Println("Error sending heartbeat to follower", index+1, err)
							continue
						}

						if output.Success {
							totalAppends++
							if len(s.log) > 0 {
								s.nextIndex[index] = output.MatchedIndex + 1
								s.matchIndex[index] = output.MatchedIndex
							}
						} else {
							s.handleFollowerUpdateToLatest(appendEntryInput, client, index)
						}
					}
				}
				//check majority
				if totalAppends > len(s.peers)/2 {
					fmt.Println("Majority of followers have received heartbeat")
					majority = true
					break
				}

				// Waiting before sending another heartbeat
				if !majority {
					fmt.Println("Waiting before sending another heartbeat")
					time.Sleep(50 * time.Millisecond)
				}
			}
		}
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) handleFollowerUpdateToLatest(appendEntryInput *AppendEntryInput, client RaftSurfstoreClient, followerIndex int) {
	for {
		if s.GetIsCrashed() || !s.GetIsLeader() {
			break
		}
		fmt.Println("Updating follower", followerIndex+1, " to latest log")
		output, err := client.AppendEntries(context.Background(), appendEntryInput)
		if err != nil {
			fmt.Println("Error sending heartbeat to follower : maybe it is crashed", err)
			break
		}
		if output.Success {
			if len(s.log) > 0 {
				s.nextIndex[followerIndex] = output.MatchedIndex + 1
				s.matchIndex[followerIndex] = output.MatchedIndex
			}
			break
		} else {
			if appendEntryInput.PrevLogIndex >= int64(1) {
				appendEntryInput.PrevLogIndex--
			}
		}
	}
}

func createAppendEntry(leaderStore *RaftSurfstore, followerIndex int) *AppendEntryInput {
	//fmt.Println("----Beginning of createAppendEntry---- for follower: ", followerIndex)
	var appendEntryInput = &AppendEntryInput{}

	appendEntryInput.Term = leaderStore.term
	appendEntryInput.LeaderCommit = leaderStore.commitIndex

	appendEntryInput.PrevLogIndex = leaderStore.matchIndex[followerIndex]

	//fmt.Println("PrevLogIndex: ", appendEntryInput.PrevLogIndex, "Leader log length: ", len(leaderStore.log))

	if len(leaderStore.log) > int(appendEntryInput.PrevLogIndex) {
		//fmt.Println("Leader log length: ", len(leaderStore.log))

		if appendEntryInput.PrevLogIndex >= 0 {
			appendEntryInput.PrevLogTerm = leaderStore.log[appendEntryInput.PrevLogIndex].Term
		}
		appendEntryInput.Entries = leaderStore.log[appendEntryInput.PrevLogIndex+1:]
		//fmt.Println("Entries to be sent for follower : ", followerIndex, appendEntryInput.Entries)
	}

	//fmt.Println("----End of createAppendEntry----")

	return appendEntryInput
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()
	time.Sleep(1 * time.Second)
	fmt.Println("Crash from server: ", s.id+1)
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()
	fmt.Println("Restore from server: ", s.id+1)
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()
	fmt.Println("---Internal State of server: ", s.id+1, "---")
	fmt.Println(s.log)
	fmt.Println(state)
	fmt.Println("------")
	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
