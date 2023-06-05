package main
import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

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
		log.Fatal("Error opening file:", err)
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

func main() {
	// Open LevelDB instance
	db, err := leveldb.OpenFile("data", nil)
	if err != nil {
		log.Fatal("Error opening LevelDB:", err)
	}
	defer db.Close()

	// Setup LevelDB entries
	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("SIM%d", i)
		value := fmt.Sprintf(`{"val": %d, "ver": 1.0}`, i)
		err = db.Put([]byte(key), []byte(value), nil)
		if err != nil {
			log.Println("Error putting value into LevelDB:", err)
		}
	}

	// Create a new blockchain
	blockChain := NewBlockChain("ledger.txt", db, 100, 100, 2)
	blockChain.Start()

	// Process input transactions
	inputTxns := []Transaction{
		{ID: "SIM1", Value: 2.0, Version: 1.0},
		{ID: "SIM2", Value: 3.0, Version: 1.0},
		{ID: "SIM3", Value: 4.0, Version: 2.0},
		{ID: "SIM4", Value: 4.0, Version: 2.0},
		{ID: "SIM5", Value: 4.0, Version: 2.0},
		{ID: "SIM6", Value: 4.0, Version: 2.0},
		
	}

	for _, txn := range inputTxns {
		blockChain.AddTransaction(txn)
	}

	// Wait for block processing and writing to finish
	close(blockChain.HashChan)
	<-blockChain.DoneChan

	
		// ...
	
		// Fetch block details by block number
		blockNumber := 2
		blockDetails := blockChain.GetBlockDetailsByNumber(blockNumber)
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
	
		

	// Fetch details of all blocks
	allBlockDetails := blockChain.GetAllBlockDetails()
	fmt.Println("All block details:")
	for _, details := range allBlockDetails {
		fmt.Println(details)
	}
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

	for _, block := range bc.Blocks {
		if block.BlockNumber == blockNumber {
			// Get the previous block's details
			previousBlock := bc.GetBlockDetailsByNumber(blockNumber - 1)
			if previousBlock != nil {
				block.PreviousHash = previousBlock.Hash
			}

			return &block
		}
	}
	return nil
}

func (bc *BlockChain) GetAllBlockDetails() []Block {
	bc.Mutex.RLock()
	defer bc.Mutex.RUnlock()

	return bc.Blocks
}