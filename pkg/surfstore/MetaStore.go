package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	var blockStoreMap BlockStoreMap
	blockStoreMap.BlockStoreMap = make(map[string]*BlockHashes)

	for _, hash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(hash)

		if _, ok := blockStoreMap.BlockStoreMap[server]; !ok {
			//fmt.Println("Initializing blockhashlist for server", server)
			blockStoreMap.BlockStoreMap[server] = &BlockHashes{}
		}

		if blockStoreMap.BlockStoreMap[server].Hashes == nil {
			//fmt.Println("Initializing block hashes")
			blockStoreMap.BlockStoreMap[server].Hashes = make([]string, 0)
		}
		blockStoreMap.BlockStoreMap[server].Hashes = append(blockStoreMap.BlockStoreMap[server].Hashes, hash) //Do we need to make if its nil
	}

	return &blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	fmt.Println("GetFileInfoMap executed")
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
	//panic("todo")
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {

	if _, ok := (m.FileMetaMap)[fileMetaData.Filename]; ok {
		//Server file version is higher
		if m.FileMetaMap[fileMetaData.Filename].Version >= fileMetaData.Version {
			return &Version{
				Version: m.FileMetaMap[fileMetaData.Filename].Version,
			}, fmt.Errorf("old version update")
		}
	}
	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	//fmt.Println("Latest version of ", fileMetaData.Filename, "on server is ", fileMetaData.Version)
	return &Version{
		Version: fileMetaData.Version,
	}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {

	consistentHashRing := NewConsistentHashRing(blockStoreAddrs)

	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: consistentHashRing,
	}
}
