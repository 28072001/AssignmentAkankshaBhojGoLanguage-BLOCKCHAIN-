package main
import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
	"bufio"
	"strconv"
	"strings"
	"math/rand"
	"math"

	"github.com/syndtr/goleveldb/leveldb"
) 

// Transaction represents a single transaction.
type Transaction struct {
	ID       string  `json:"id"`
	Value    float64 `json:"val"`
	Version  float64 `json:"ver"`
	Valid    bool    `json:"valid"`
	Hash     string  `json:"hash"`
	HashDone bool    `json:"-"`
}

// BlockStatus represents the status of a block.
type BlockStatus string

const (
	Committed BlockStatus = "committed"
	Pending   BlockStatus = "pending"
)
//Interface of Blockchain
type BlockChainInterface interface {
	Start()
	AddTransaction(txn Transaction)
	GetBlockDetailsByNumber(blockNumber int) *Block
	PrintAllBlockDetails()
	ReadBlocksFromFile() error
}

// Block represents a block in the blockchain.
type Block struct {
	BlockNumber  int           `json:"blockNumber"`
	PreviousHash string        `json:"prevBlockHash"`
	Transactions []Transaction `json:"txns"`
	Timestamp    int64         `json:"timestamp"`
	Status       BlockStatus   `json:"blockStatus"`
	Hash         string        `json:"hash"` // New field for block hash
}

// BlockChain represents the blockchain.
type BlockChain struct {
	Blocks         []Block        `json:"blocks"`
	FileName       string         `json:"-"`
	Mutex          sync.RWMutex   `json:"-"`
	DB             *leveldb.DB    `json:"-"`
	HashChan       chan *Block    `json:"-"`
	WriteChan      chan *Block    `json:"-"`
	DoneChan       chan struct{}  `json:"-"`
	TxnsPerBlock   int            `json:"-"`
	CurrentBlock   *Block         `json:"-"`
	CurrentCounter int            `json:"-"`
}

// NewBlockChain creates a new blockchain instance.
func NewBlockChain(fileName string, db *leveldb.DB, hashChanSize, writeChanSize, txnsPerBlock int) *BlockChain {
	return &BlockChain{
		Blocks:         []Block{},
		FileName:       fileName,
		Mutex:          sync.RWMutex{},
		DB:             db,
		HashChan:       make(chan *Block, hashChanSize),
		WriteChan:      make(chan *Block, writeChanSize),
		DoneChan:       make(chan struct{}),
		TxnsPerBlock:   txnsPerBlock,
		CurrentBlock:   nil,
		CurrentCounter: 0,
	}
}



// Start starts the block processing and writing to the file.
func (bc *BlockChain) Start() {
	go bc.processBlocks()
	go bc.writeBlocksToFile()
}

func (bc *BlockChain) processBlocks() {
	for block := range bc.HashChan {
		startTime := time.Now()

		var wg sync.WaitGroup
		for i := range block.Transactions {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				txn := &block.Transactions[i]
				txn.Hash = calculateHash(txn.ID, txn.Value, txn.Version)
				txn.Hash = calculateSHA256Hash(txn.Hash)
				txn.HashDone = true
			

				// Validate version
				if txn.Version == getVersionFromLevelDB(bc.DB, txn.ID) {
					txn.Valid = true
				}
			
			}(i)
		}
		wg.Wait()

		block.Status = Committed
		bc.WriteChan <- block

		processingTime := time.Since(startTime)
		log.Printf("Block %d processing time: %v\n", block.BlockNumber, processingTime)
	}
	close(bc.WriteChan)
}


func (bc *BlockChain) writeBlocksToFile() {
	file, err := os.OpenFile(bc.FileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Error openin file:", err)
	}
	defer file.Close()

	for block := range bc.WriteChan {
		// Update previous block's hash for blocks after the first one
		if block.BlockNumber > 1 {
			previousBlock := bc.GetBlockDetailsByNumber(block.BlockNumber - 1)
			block.PreviousHash = previousBlock.Hash
		}

		block.Hash = calculateBlockHash(*block) // Calculate block hash
		blockJSON, err := json.Marshal(block)
		if err != nil {
			log.Println("Error marshaling block to JSON:", err)
			continue
		}

		if _, err := file.Write(blockJSON); err != nil {
			log.Println("Error writing block to file:", err)
		}

		bc.Mutex.Lock()
		bc.Blocks = append(bc.Blocks, *block)
		bc.Mutex.Unlock()
	}

	bc.DoneChan <- struct{}{}
}


func getVersionFromLevelDB(db *leveldb.DB, key string) float64 {
	value, err := db.Get([]byte(key), nil)
	if err != nil {
		log.Println("Error retrieving value from LevelDB:", err)
		return 0
	}

	var txn Transaction
	err = json.Unmarshal(value, &txn)
	if err != nil {
		log.Println("Error unmarshaling stored transaction:", err)
		return 0
	}

	return txn.Version
}

func calculateHash(id string, value float64, version float64) string {
	return fmt.Sprintf("%s-%f-%f", id, value, version)
}

// calculateBlockHash calculates the hash of a block based on its properties.
func calculateBlockHash(block Block) string {
	blockJSON, err := json.Marshal(block)
	if err != nil {
		log.Println("Error marshaling block to JSON:", err)
		return ""
	}

	hash := sha256.Sum256(blockJSON)
	return fmt.Sprintf("%x", hash)
}
func calculateSHA256Hash(data string) string {
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash)
}
func handleUserInput(bc *BlockChain) {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("Enter a block number to access details (or 'q' to quit): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "q" {
			break
		}

		blockNumber, err := strconv.Atoi(input)
		if err != nil {
			fmt.Println("Invalid block number. Please try again.")
			continue
		}

		blockDetails := bc.GetBlockDetailsByNumber(blockNumber)
		if blockDetails != nil {
			fmt.Printf("Block %d details:\n", blockNumber)
			fmt.Printf("Previous Hash: %s\n", blockDetails.PreviousHash)
			fmt.Printf("Hash: %s\n", blockDetails.Hash)
			fmt.Printf("Transactions: %+v\n", blockDetails.Transactions)
			fmt.Printf("Timestamp: %d\n", blockDetails.Timestamp)
			fmt.Printf("Status: %s\n", blockDetails.Status)
		} else {
			fmt.Printf("Block %d not found\n", blockNumber)
		}
	}
}
func (bc *BlockChain) PrintAllBlockDetails() {
	fmt.Println("All block details:")

	// Read the blocks from the file if the Blocks slice is empty
	if len(bc.Blocks) == 0 {
		err := bc.ReadBlocksFromFile()
		if err != nil {
			log.Println("Error reading blocks from file:", err)
			return
		}
	}

	for _, details := range bc.Blocks {
		fmt.Printf("Block %d details:\n", details.BlockNumber)
		fmt.Printf("Previous Hash: %s\n", details.PreviousHash)
		fmt.Printf("Hash: %s\n", details.Hash)
		fmt.Printf("Transactions: %+v\n", details.Transactions)
		fmt.Printf("Timestamp: %d\n", details.Timestamp)
		fmt.Printf("Status: %s\n", details.Status)
		fmt.Println()
	}
}
func (bc *BlockChain) ReadBlocksFromFile() error {
	file, err := os.Open(bc.FileName)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		blockJSON := scanner.Bytes()

		var block Block
		err := json.Unmarshal(blockJSON, &block)
		if err != nil {
			log.Println("Error unmarshaling block from JSON:", err)
			continue
		}

		bc.Mutex.Lock()
		bc.Blocks = append(bc.Blocks, block)
		bc.Mutex.Unlock()
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}


func main() {
	// Open LevelDB instance
	db, err := leveldb.OpenFile("data", nil)
	if err != nil {
		log.Fatal("Error opening LevelDB:", err)
	}
	defer db.Close()

	// Setup LevelDB entries
	for i := 1; i <= 10000; i++ {
		key := fmt.Sprintf("SIM%d", i)
		value := fmt.Sprintf(`{"val": %d, "ver": 1.0}`, i)
		err = db.Put([]byte(key), []byte(value), nil)
		if err != nil {
			log.Println("Error putting value into LevelDB:", err)
		}
	}




txnsPerBlock := getTransactionsPerBlockFromEnv()
blockChain := NewBlockChain("ledger.txt", db, 100, 100, txnsPerBlock)
	blockChain.Start()


rand.Seed(time.Now().UnixNano())



	// Process input transactions
	inputTxns := make([]Transaction, 0)

for j := 1; j <= 10; j++ {
	for i := 1; i <= 1000; i++ {
		version := roundToNearest(rand.Float64()*4.0 + 1.0) // Generate a random version between 1.0 and 5.0 and round off to the nearest whole number
		txn := Transaction{
			ID:      fmt.Sprintf("SIM%d", i),
			Value:   float64(i),
			Version: version,
		}
		inputTxns = append(inputTxns, txn)
	}
}

	for _, txn := range inputTxns {
		blockChain.AddTransaction(txn)
	}

     
		handleUserInput(blockChain)

	// Wait for block processing and writing to finish
	close(blockChain.HashChan)
	<-blockChain.DoneChan
   // Print details of all blocks
    blockChain.PrintAllBlockDetails()
	//blockChain.ReadBlocksFromFile()
}

func (bc *BlockChain) AddTransaction(txn Transaction) {
	if bc.CurrentBlock == nil {
		bc.CurrentBlock = &Block{
			BlockNumber:  1,
			PreviousHash: "000000",
			Transactions: []Transaction{},
			Timestamp:    time.Now().Unix(),
			Status:       Pending,
		}
	}

	bc.CurrentBlock.Transactions = append(bc.CurrentBlock.Transactions, txn)
	bc.CurrentCounter++

	if bc.CurrentCounter == bc.TxnsPerBlock {
		bc.HashChan <- bc.CurrentBlock

		previousBlock := bc.CurrentBlock
		bc.CurrentBlock = &Block{
			BlockNumber:  previousBlock.BlockNumber + 1,
			PreviousHash: previousBlock.Hash, // Update previous hash
			Transactions: []Transaction{},
			Timestamp:    time.Now().Unix(),
			Status:       Pending,
		}
		bc.CurrentCounter = 0
	}
}

func (bc *BlockChain) GetBlockDetailsByNumber(blockNumber int) *Block {
	bc.Mutex.RLock()
	defer bc.Mutex.RUnlock()

	// Check if the block number is within the range of existing blocks
	if blockNumber < 1 || blockNumber > len(bc.Blocks) {
		return nil
	}

	// Read the blocks from the file if the Blocks slice is empty
	if len(bc.Blocks) == 0 {
		err := bc.ReadBlocksFromFile()
		if err != nil {
			log.Println("Error reading blocks from file:", err)
			return nil
		}
	}

	// Get the block by block number
	blockIndex := blockNumber - 1
	block := &bc.Blocks[blockIndex]

	// Get the previous block's details
	if block.BlockNumber > 1 {
		previousBlock := bc.GetBlockDetailsByNumber(blockNumber - 1)
		if previousBlock != nil {
			block.PreviousHash = previousBlock.Hash
		}
	}

	return block}

// Function to round a float64 value to the nearest whole number9090
func roundToNearest(x float64) float64 {
	return math.Round(x)
}
func getTransactionsPerBlockFromEnv() int {
	txnsPerBlockStr := os.Getenv("TRANSACTIONS_PER_BLOCK")
	txnsPerBlock, err := strconv.Atoi(txnsPerBlockStr)
	if err != nil || txnsPerBlock <= 0 {
		log.Println("Invalid value for TRANSACTIONS_PER_BLOCK environment variable. Using default value: 1")
		return 1
	}
	return txnsPerBlock
}
