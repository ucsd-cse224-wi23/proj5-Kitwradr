package surfstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		id:             id,
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		peers:          config.RaftAddrs,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
		lastApplied:    -1,
		commitIndex:    -1,
		nextIndex:      make([]int64, len(config.RaftAddrs)),
		matchIndex:     make([]int64, len(config.RaftAddrs)),
		pendingCommits: make([]*chan bool, 0),
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	grpcServer := grpc.NewServer()

	RegisterRaftSurfstoreServer(grpcServer, server)

	l, e := net.Listen("tcp", server.peers[server.id])
	if e != nil {
		fmt.Println("Error starting server", e.Error())
		return e
	}

	grpcServer.Serve(l)

	return nil

}
