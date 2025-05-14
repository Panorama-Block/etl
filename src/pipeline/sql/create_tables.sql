CREATE TABLE IF NOT EXISTS chains (
    chainId                String,
    status                 String,
    chainName              String,
    description            String,
    platformChainId        String,
    subnetId               String,
    vmId                   String,
    vmName                 String,
    explorerUrl            String,
    rpcUrl                 String,
    wsUrl                  String,
    isTestnet              UInt8,
    private                UInt8,
    chainLogoUri           String,
    enabledFeatures        String,
    networkToken_name      String,
    networkToken_symbol    String,
    networkToken_decimals  Int32,
    networkToken_logoUri   String,
    networkToken_description String
) ENGINE = MergeTree()
ORDER BY chainId;

CREATE TABLE IF NOT EXISTS blocks(
    baseFee UInt64,                                -- The base fee of the block, stored as an unsigned integer
    blockHash String,                              -- The block hash, stored as a string
    blockNumber UInt64,                           -- The block number, stored as an unsigned integer
    blockTimestamp Int32,                         -- The timestamp of the block, stored as an integer (Unix timestamp)
    chainId UInt32,                               -- The chain ID, stored as an unsigned integer
    cumulativeTransactions UInt64,                -- The cumulative number of transactions, stored as an unsigned integer
    feesSpent UInt64,                             -- The fees spent on the block, stored as an unsigned integer
    gasCost UInt64,                               -- The gas cost for the block, stored as an unsigned integer
    gasLimit UInt64,                              -- The gas limit of the block, stored as an unsigned integer
    gasUsed UInt64,                               -- The amount of gas used by the block, stored as an unsigned integer
    parentHash String,                            -- The parent block hash, stored as a string
    txCount UInt32                                -- The number of transactions in the block, stored as an unsigned integer
)
ENGINE = MergeTree()
ORDER BY (blockNumber);

CREATE TABLE IF NOT EXISTS loaded_files
(
    file_name String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY file_name;