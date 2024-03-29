package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {

	hashes := []string{}
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)

	responsibleServer := ""

	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[hashes[0]]
	}

	//fmt.Println("Responsible server for blockId", blockId, responsibleServer)

	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {

	var consistentHashRing ConsistentHashRing

	consistentHashRing.ServerMap = make(map[string]string)

	for _, addr := range serverAddrs {
		addr = "blockstore" + addr
		hash := consistentHashRing.Hash(addr)
		consistentHashRing.ServerMap[hash] = addr
	}
	return &consistentHashRing
}
