package surfstore

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	var delHash []string
	delHash = append(delHash, "0")

	fmt.Println("Starting sync", client.BaseDir)
	serverFileInfoMap := make(map[string]*FileMetaData)
	err := client.GetFileInfoMap(&serverFileInfoMap)

	if err != nil {
		log.Fatal("Error retrieving file info map from server", err.Error())
	}

	fmt.Println("Retrieved the file info map from server", serverFileInfoMap)

	base_path, err := filepath.Abs(client.BaseDir)

	if err != nil {
		fmt.Println("Error getting absolute path", err.Error())
	}

	//fmt.Println("Abs path", base_path)

	//Construct localFileInfoMap
	err = createDirectoryIfNotExits(base_path)

	if err != nil {
		fmt.Println("Error creating the client directory", err.Error())
		return
	}

	client_files_info := getClientFilesInfoMap(base_path, client)

	//------------------------------------------------------

	//Read local index
	local_index, err := LoadMetaFromMetaFile(base_path)

	if err != nil {
		fmt.Println("Error reading local index from index.db")
	}
	//fmt.Println("Local index:", local_index)

	//--------------------------------------------------------

	//Update versions and other data from db into client info map
	updateClientInfoMap(&client_files_info, client, local_index)

	// Write file which does not exist in server and exists in local directory
	PrintMetaMap(serverFileInfoMap)
	for file, file_info := range client_files_info {
		if _, ok := serverFileInfoMap[file]; ok {
			fmt.Println("File already exists in metastore", file_info.fileName, client.BaseDir)
		} else { //File exists on client but not in server
			if !checkDeletedFile(file_info.blockHashList) { // Send only if it's not a deleted file
				err := sendFileToServer(*file_info, file, client, (*file_info).version) // Version for new file is 0
				if err != nil {
					// Means server has a version >= client before client could write the file
					var newRemoteIndex map[string]*FileMetaData
					client.GetFileInfoMap(&newRemoteIndex)
					handleUpdateServerCase(newRemoteIndex[file], &client_files_info, client)
				}
			}
		}
	}

	for filename, filedata := range serverFileInfoMap {
		if _, ok := client_files_info[filename]; ok {
			//File exists both in client and server
			err := handleUpdateServerCase(filedata, &client_files_info, client)
			if err != nil {
				fmt.Println("Error in update case ", err.Error())
			}
		} else { // //Write file into client which exists on server but not on client
			if !arraysEqual(filedata.BlockHashList, delHash) {
				err := fetchFileFromServer(client, &client_files_info, *filedata, filename)
				if err != nil {
					fmt.Println("Error writing to file", err.Error())
				}
			}

		}
	}

	// Update db file in the end
	err = WriteFileUpdateInfo(client_files_info, base_path)

	if err != nil {
		fmt.Println("Error updating db file")
	}

}

func createDirectoryIfNotExits(path string) error {

	_, err := os.Stat(path)
	if err != nil {
		err := os.Mkdir(path, 0755)
		if err != nil {
			return err
		}
	}

	return nil
}

func handleUpdateServerCase(server_file *FileMetaData, client_files_info *map[string]*FileUpdateInfo, client RPCClient) error {

	var delHash []string

	delHash = append(delHash, "0")

	file_name := server_file.Filename

	// Server file has a higher version, so write from server to client
	if server_file.Version > (*client_files_info)[file_name].version {

		fmt.Println("Server has higher version thus updating the file from server")
		//If server file is deleted

		if arraysEqual(server_file.BlockHashList, delHash) {
			//Delete file from client directory
			filepath := filepath.Join(client.BaseDir, file_name)
			err := os.Remove(filepath)
			if err != nil {
				fmt.Println("Error removing file from directory")
				return err
			}
			// update client file map which is used later
			(*client_files_info)[file_name] = &FileUpdateInfo{
				version:       server_file.Version,
				blockHashList: server_file.BlockHashList,
			}
		} else { // If it's not deleted just get the new file from server
			fetchFileFromServer(client, client_files_info, *server_file, file_name)
		}

	} else if (*client_files_info)[file_name].version > server_file.Version { //Client has higher version
		client_file := *(*client_files_info)[file_name]
		// TODO : Update error case
		fmt.Println("Client has higher version thus updating the file")
		sendFileToServer(client_file, file_name, client, int32((*client_files_info)[file_name].version))

	} else if !arraysEqual((*client_files_info)[file_name].blockHashList, server_file.BlockHashList) { // Same version and hashbytes are unequal
		fmt.Println("Same version but hashbytes are unequal on", client.BaseDir, "thus updating the file from server")
		fetchFileFromServer(client, client_files_info, *server_file, file_name)
	}
	return nil
}

func fetchFileFromServer(client RPCClient, client_files_info *map[string]*FileUpdateInfo, server_file FileMetaData, file_name string) error {

	var block_store_map map[string][]string
	var block_map = make(map[string]Block)
	var file_blocks []Block
	ordered_block_hashes := server_file.GetBlockHashList()
	client.GetBlockStoreMap(ordered_block_hashes, &block_store_map)

	if !checkDeletedFile(server_file.BlockHashList) {
		for server_address, block_hash_list := range block_store_map {
			for _, block_hash := range block_hash_list {
				var block Block
				err := client.GetBlock(block_hash, server_address[len("blockstore"):], &block)
				if err != nil {
					fmt.Println("Get block error", err.Error(), "for block hash", block_hash)
					return err
				}
				//fmt.Println("Get block success")
				block_map[block_hash] = block
			}
		}

		// Reconstructing ordered file blocks
		for _, block_hash := range ordered_block_hashes {
			file_blocks = append(file_blocks, block_map[block_hash])
		}
	}

	//Update blocks and block hashes of the file in clientInfoMap
	if _, ok := (*client_files_info)[file_name]; !ok {
		(*client_files_info)[file_name] = &FileUpdateInfo{} // In case of adding new file
	}

	(*client_files_info)[file_name].blocks = file_blocks
	(*client_files_info)[file_name].blockHashList = server_file.GetBlockHashList()
	(*client_files_info)[file_name].version = server_file.Version

	//Write the update file into base directory
	//If it's deleted file remove the file
	if checkDeletedFile(server_file.BlockHashList) {
		err := os.Remove(path.Join(client.BaseDir, file_name))
		if err != nil {
			return err
		}
		return nil
	}

	err := WriteBlocksToFile(file_blocks, client.BaseDir, file_name)
	if err != nil {
		fmt.Println("Error updating file", err.Error())
		return err
	}

	return nil

}

func updateClientInfoMap(client_info_map *map[string]*FileUpdateInfo, client RPCClient, local_index map[string]*FileMetaData) {

	var delHash []string
	delHash = append(delHash, "0")

	for filename, filedata := range local_index {

		//If file in db exists in directory update the version
		if _, ok := (*client_info_map)[filename]; ok {
			fmt.Println("For file", filename)
			//fmt.Println("Number of blocks in client info map", len((*client_info_map)[filename].blockHashList))
			//fmt.Println("NUmber of blocks in local index", len(local_index[filename].BlockHashList))
			//fmt.Println("Hashvalues/////////\n", (*client_info_map)[filename].blockHashList, "//////\n", local_index[filename].BlockHashList, "////////")
			if arraysEqual((*client_info_map)[filename].blockHashList, local_index[filename].BlockHashList) {
				(*client_info_map)[filename].version = filedata.Version
			} else {
				fmt.Println("Hashlists are not equal")
				(*client_info_map)[filename].version = filedata.Version + 1
			}
		} else {
			//File is deleted in client and db index already knows it

			if arraysEqual(local_index[filename].BlockHashList, delHash) {

				(*client_info_map)[filename] = &FileUpdateInfo{
					fileName:      filename,
					version:       filedata.Version,
					blockHashList: delHash,
				}

			} else { // index doesn't know it, so update db index

				(*client_info_map)[filename] = &FileUpdateInfo{
					fileName:      filename,
					version:       filedata.Version + 1,
					blockHashList: delHash,
				}
			}

		}

	}

	fmt.Println("Updated the client info map successfully from local index")
	//PrintFileUpdateInfo(*client_info_map)

}

func arraysEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func checkDeletedFile(hash_blocks []string) bool {
	if len(hash_blocks) != 1 {
		return false
	}
	if hash_blocks[0] == "0" {
		return true
	}
	return false
}

func sendFileToServer(file_info FileUpdateInfo, file_name string, client RPCClient, version int32) error {
	fmt.Println("Sending", file_name, "to server")
	var fileMetaData FileMetaData
	var latestVersion int32
	fileMetaData.BlockHashList = file_info.blockHashList
	fileMetaData.Filename = file_info.fileName
	fileMetaData.Version = int32(version)
	fmt.Println("Sending version on server", version)
	err := client.UpdateFile(&fileMetaData, &latestVersion)
	if err != nil {
		fmt.Println("Error writing to file to server for", file_name, "error is", err.Error())
		return err
	}
	fmt.Println("Latest version of the file", file_name, "is", latestVersion)
	putAllBlocksToServer(client, file_info.blocks)

	return nil

}

// func WriteFileToClient(filename string, filedata FileMetaData, base_path string, client RPCClient, block_store_address string, client_files_info *map[string]*FileUpdateInfo) error {

// 	if !checkDeletedFile(filedata.BlockHashList) {
// 		//

// 		if _,ok := client_files_info[filename]; ok {

// 		}

// 		//
// 		fmt.Println("Creating the file in client", filename)
// 		block_hashes := filedata.BlockHashList
// 		var all_blocks []Block
// 		for _, hash := range block_hashes {
// 			var block Block
// 			err := client.GetBlock(hash, block_store_address, &block)
// 			if err != nil {
// 				return err
// 			}
// 			all_blocks = append(all_blocks, block)
// 			fmt.Println("Success getting block size is", block.BlockSize)
// 		}

// 		err := WriteBlocksToFile(all_blocks, base_path, filename)

// 		if err != nil {
// 			return err
// 		}

// 	} else {
// 		fmt.Println("File is deleted on server ")
// 	}

// 	return nil

// }

func putAllBlocksToServer(client RPCClient, blocks []Block) error {

	var blockHashes []string
	var blockStoreMap map[string][]string
	var blockMap = make(map[string]Block)

	for _, block := range blocks {
		hash := getHash(block.BlockData, int(block.BlockSize))
		blockMap[hash] = block
		blockHashes = append(blockHashes, hash)
	}

	client.GetBlockStoreMap(blockHashes, &blockStoreMap)

	fmt.Println("Block store map", blockStoreMap)

	for server_address, block_hash_list := range blockStoreMap {
		var success bool
		for _, block_hash := range block_hash_list {
			block := blockMap[block_hash]
			err := client.PutBlock(&block, server_address[len("blockstore"):], &success)
			if err != nil {
				fmt.Println("Put block error")
				return err
			}
			fmt.Println("Put block success on server", server_address)
		}

	}
	return nil
}

func WriteBlocksToFile(blocks []Block, baseDir string, filename string) error {

	file, err := os.Create(filepath.Join(baseDir, filename))

	if err != nil {
		fmt.Println("Error creating file to write", filename)
		return err
	}

	defer file.Close()

	for _, block := range blocks {
		_, err := file.Write(block.BlockData)
		if err != nil {
			return err
		}
	}

	return nil

}
