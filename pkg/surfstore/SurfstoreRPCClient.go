package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.PutBlock(ctx, block)
	//fmt.Println("Result of putting block ", b.GetFlag())
	*succ = b.GetFlag()
	if err != nil {
		conn.Close()
		return err
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	return nil
	//panic("todo")
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {

	for idx, addr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(addr, grpc.WithInsecure())

		if err != nil {
			fmt.Println("Error in grpc dial")
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		defer cancel()

		fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		if err != nil {
			fmt.Println("Error in get file info map", err.Error(), "server is", addr, idx)
			continue
		}
		fmt.Println("Found leader no error : leader is", addr, idx)
		for key, value := range (*fileInfoMap).FileInfoMap {
			(*serverFileInfoMap)[key] = value
		}
		return nil
	}

	return fmt.Errorf("error in getting file info map")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {

	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			continue
		}
		*latestVersion = b.Version
		return nil
	}
	return fmt.Errorf("error in updating file")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddr *[]string) error {
	// connect to the server

	for _, addr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			return err
		}

		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		addr, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			continue
		}
		*blockStoreAddr = addr.BlockStoreAddrs
		return nil
	}
	return fmt.Errorf("error in getting block store addrs")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	//Todo: Find leader in meta store and connect to it
	for _, addr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		block_store_map, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			continue
		}

		if *blockStoreMap == nil {
			*blockStoreMap = make(map[string][]string)
		}

		for server, hashes := range block_store_map.BlockStoreMap {
			if _, ok := (*blockStoreMap)[server]; !ok {
				(*blockStoreMap)[server] = make([]string, 0)
			}
			(*blockStoreMap)[server] = hashes.Hashes
		}
		return nil
	}

	return fmt.Errorf("error in getting block store map")

}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	block_hashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	//fmt.Println("Result of putting block ", b.GetFlag())
	if err != nil {
		conn.Close()
		return err
	}

	*blockHashes = block_hashes.Hashes

	// close the connection
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
