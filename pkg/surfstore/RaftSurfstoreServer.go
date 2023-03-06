package surfstore

import (
	context "context"
	"math"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	id            int64
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	commitIndex   int64
	log           []*UpdateOperation
	raftAddrs     []string
	metaStore     *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

// If the node is the leader, and if a majority of the nodes are working, should return the correct answer; if a majority of the nodes are crashed, should block until a majority recover.  If not the leader, should indicate an error back to the client
func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Execute only if server is not crashed
	if !s.isCrashed {
		// If server is leader, return the file info map
		if s.isLeader {
			return s.metaStore.GetFileInfoMap(ctx, empty)
		} else {
			return nil, ERR_NOT_LEADER
		}
	} else {
		return nil, ERR_SERVER_CRASHED
	}
	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	panic("todo")
	return nil, nil
}

// If the node is the leader, and if a majority of the nodes are working, should return the correct answer; if a majority of the nodes are crashed, should block until a majority recover.
// If not the leader, should indicate an error back to the client
func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Execute only if server is not crashed
	if !s.isCrashed {
		// If server is leader, return the block store addresses
		if s.isLeader {
			return s.metaStore.GetBlockStoreAddrs(ctx, empty)
		} else {
			return nil, ERR_NOT_LEADER
		}
	} else {
		return nil, ERR_SERVER_CRASHED
	}
}

// If the node is the leader, and if a majority of the nodes are working, should return the correct answer;
// if a majority of the nodes are crashed, should block until a majority recover.  If not the leader, should indicate an error back to the client
func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Execute only if server is not crashed
	if !s.isCrashed {
		// If server is leader, update the file
		if s.isLeader {
			return s.metaStore.UpdateFile(ctx, filemeta)
		} else {
			return nil, ERR_NOT_LEADER
		}
	} else {
		return nil, ERR_SERVER_CRASHED
	}
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
		if input.Term < s.term {
			appendEntryOutput.Success = false
			return appendEntryOutput, nil
		}

		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		if input.PrevLogIndex >= 0 && input.PrevLogIndex < int64(len(s.log)) {
			if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
				appendEntryOutput.Success = false
				return appendEntryOutput, nil
			}
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		if input.PrevLogIndex+1 < int64(len(s.log)) {
			s.log = s.log[:input.PrevLogIndex+1]
		}

		// 4. Append any new entries not already in the log
		s.log = append(s.log, input.Entries...)
		appendEntryOutput.Success = true

		// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if input.LeaderCommit > s.commitIndex {
			s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
		}

		return appendEntryOutput, nil
	} else {
		return appendEntryOutput, ERR_SERVER_CRASHED
	}
}

// Emulates elections, sets the node to be the leader
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if !s.isCrashed {
		s.isLeaderMutex.Lock()
		s.isLeader = true
		s.isLeaderMutex.Unlock()
	} else {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	return nil, nil
}

// Sends a round of AppendEntries to all other nodes. The leader will attempt to replicate logs to all other nodes when this is called.
// It can be called even when there are no entries to replicate. If a node is not in the leader state it should do nothing.
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	} else {
		wg := sync.WaitGroup{}
		if s.isLeader {
			for _, server := range s.raftAddrs {
				if server != s.raftAddrs[s.id] {
					wg.Add(1)
					go s.AppendEntries(ctx, &AppendEntryInput{})
				}
			}
		}
	}
	return nil, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

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

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
