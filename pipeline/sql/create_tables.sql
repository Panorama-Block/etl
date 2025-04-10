CREATE TABLE IF NOT EXISTS chains
(
    chainId           String,
    status            String,
    chainName         String,
    description       String,
    platformChainId   String,
    subnetId          String,
    vmId              String,
    vmName            String,
    explorerUrl       String,
    rpcUrl            String,
    wsUrl             String,
    isTestnet         UInt8,
    private           UInt8,
    chainLogoUri      String,
    enabledFeatures   Array(String),
    networkToken      Tuple(
                        name        String,
                        symbol      String,
                        decimals    Int32,
                        logoUri     String,
                        description String
                    )
)
ENGINE = MergeTree()
ORDER BY chainId;

CREATE TABLE IF NOT EXISTS loaded_files
(
    file_name String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY file_name;