package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"

	_ "github.com/mattn/go-sqlite3"
)

type FileUpdateInfo struct {
	blockHashList []string
	fileName      string
	version       int32
	blocks        []Block
}

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName,version,hashIndex,hashValue) VALUES (?,?,?,?)`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			fmt.Println("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		fmt.Println("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		fmt.Println("Error During Meta Write Back")
	}
	statement.Exec()

	statement, err = db.Prepare(insertTuple)

	if err != nil {
		fmt.Println("Error writing to db")
		return err
	}

	for filename, filedata := range fileMetas {

		for hash_index, hash_value := range filedata.BlockHashList {
			_, err := statement.Exec(filename, filedata.Version, hash_index, hash_value)
			if err != nil {
				fmt.Println("Error writing to db")
				return err
			}
		}

	}
	return nil
}

func WriteFileUpdateInfo(fileMetas map[string]*FileUpdateInfo, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			fmt.Println("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		fmt.Println("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		fmt.Println("Error During Meta Write Back")
	}
	statement.Exec()

	statement, err = db.Prepare(insertTuple)

	if err != nil {
		fmt.Println("Error writing to db")
		return err
	}

	for filename, filedata := range fileMetas {

		for hash_index, hash_value := range filedata.blockHashList {
			_, err := statement.Exec(filename, filedata.version, hash_index, hash_value)
			if err != nil {
				fmt.Println("Error writing to db")
				return err
			}
		}

	}
	return nil
}

/*
Reading Local Metadata File Related
*/

const getDistinctFileName string = `select distinct fileName from indexes`

const getTuplesByFileName string = `select version,hashIndex,hashValue from indexes where filename = (?)`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	database, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		//log.Fatal("Error When Opening Meta")
		return nil, err
	}

	// Creating table if not exists
	statement, err := database.Prepare(createTable)

	if err != nil {
		fmt.Println("Error creating table")
		return nil, err
	}

	statement.Exec()

	filename_rows, err := database.Query(getDistinctFileName)

	if err != nil {
		fmt.Println("Error getting distinct filenames")
		return nil, err
	}

	var file_names []string
	var filename string
	for filename_rows.Next() {
		filename_rows.Scan(&filename)
		file_names = append(file_names, filename)
		fmt.Println("Filename from db:", filename)
	}

	for _, filename := range file_names {
		var hash_value string
		var hash_index int
		var version int
		var hashlist = make(map[int]string)
		rows, err := database.Query(getTuplesByFileName, filename)
		if err != nil {
			fmt.Println("Error getting filename tuples")
			return nil, err
		}
		for rows.Next() {
			rows.Scan(&version, &hash_index, &hash_value)
			//fmt.Println("Tuple : ", version, hash_index, hash_value)
			hashlist[hash_index] = hash_value
		}
		fileMetaMap[filename] = &FileMetaData{}
		fileMetaMap[filename].Filename = filename
		fileMetaMap[filename].Version = int32(version)
		fileMetaMap[filename].BlockHashList = getSortedValues(hashlist)
	}
	//PrintMetaMap(fileMetaMap)
	return fileMetaMap, nil
	//panic("todo")
}

func getSortedValues(slice map[int]string) []string {
	var values []string
	var keys []int
	for key, _ := range slice {
		keys = append(keys, key)
	}

	sort.Ints(keys)

	for _, key := range keys {
		values = append(values, slice[key])
	}

	return values
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintFileUpdateInfo(metaMap map[string]*FileUpdateInfo) {

	fmt.Println("--------BEGIN FILE UPDATE INFO--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.fileName, filemeta.version)
		for _, blockHash := range filemeta.blockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END FILE UPDATE INFO--------")

}

func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}

func getHash(buf []byte, bytesRead int) string {
	hashBytes := sha256.Sum256(buf[:bytesRead])
	return hex.EncodeToString(hashBytes[:])
}

func getClientFilesInfoMap(dirpath string, client RPCClient) map[string]*FileUpdateInfo {

	fmt.Println("Starting creation of client info map")

	files_info := make(map[string]*FileUpdateInfo)

	files, err := ioutil.ReadDir(dirpath)

	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		fmt.Println(file.Name())

		if file.Name() == DEFAULT_META_FILENAME {
			//fmt.Println("Encountered index file")
			continue
		}

		file_update_obj := &FileUpdateInfo{}
		file_update_obj.fileName = file.Name()
		files_info[file.Name()] = file_update_obj

		var allBlocks []Block
		var hashes []string

		fileName := file.Name()
		file, err := os.Open(filepath.Join(dirpath, file.Name()))
		if err != nil {
			log.Fatal(err)
		}
		//index := 0
		for {
			buf := make([]byte, client.BlockSize)
			// Read 4096 bytes into buffer using binary.Read
			n, err := file.Read(buf)
			if err != nil && err != io.EOF {
				log.Println(err)
				break
			}
			if err == io.EOF {
				break
			}
			allBlocks = append(allBlocks, Block{BlockData: buf[:n], BlockSize: int32(n)})
			hashes = append(hashes, getHash(buf, n))
			// Do something with the buffer
			//fmt.Println(fileName, getHash(buf, n))
			//_, err = statement.Exec(fileName, 0, index, getHash(buf, n))
			if err != nil {
				log.Println(err)
				break
			}
			//index += 1

		}
		fmt.Println("Number of blocks", fileName, len(allBlocks), len(hashes))
		//fmt.Println(allBlocks)
		//fmt.Println("All hashes", hashes)
		file_update_obj.blocks = allBlocks
		file_update_obj.blockHashList = hashes
		file_update_obj.version = 1 // Default version, for existing files it will be updated later

	}

	//fmt.Println("Files info object", files_info)
	//fmt.Println("All hashes",hash)

	return files_info
}
