# Avalanche Data Extraction

This service extracts data from Avalanche chains and publishes it to Kafka topics. Below is the documentation of the data structures being sent to each topic.

## Topics and Data Structures

### Chain Events (`avalanche.chains.*`)
```json
{
  "chainId": "string",
  "status": "string",
  "chainName": "string",
  "description": "string",
  "platformChainId": "string",
  "subnetId": "string",
  "vmId": "string",
  "vmName": "string",
  "explorerUrl": "string",
  "rpcUrl": "string",
  "wsUrl": "string",
  "isTestnet": "boolean",
  "private": "boolean",
  "chainLogoUri": "string",
  "enabledFeatures": ["string"],
  "networkToken": {
    "name": "string",
    "symbol": "string",
    "decimals": "number",
    "logoUri": "string",
    "description": "string"
  }
}
```

### Block Events (`avalanche.blocks.*`)
```json
{
  "chainId": "string",
  "blockNumber": "string",
  "blockTimestamp": "number",
  "blockHash": "string",
  "txCount": "number",
  "baseFee": "string",
  "gasUsed": "string",
  "gasLimit": "string",
  "gasCost": "string",
  "parentHash": "string",
  "feesSpent": "string",
  "cumulativeTransactions": "string"
}
```

### Transaction Events (`avalanche.transactions.*`)
```json
{
  "blockHash": "string",
  "blockNumber": "string",
  "from": "string",
  "gas": "string",
  "gasPrice": "string",
  "maxFeePerGas": "string",
  "maxPriorityFeePerGas": "string",
  "txHash": "string",
  "txStatus": "string",
  "input": "string",
  "nonce": "string",
  "to": "string",
  "transactionIndex": "number",
  "value": "string",
  "type": "number",
  "chainId": "string",
  "receiptCumulativeGasUsed": "string",
  "receiptGasUsed": "string",
  "receiptEffectiveGasPrice": "string",
  "receiptRoot": "string",
  "blockTimestamp": "number",
  "erc20Transfers": [
    {
      // ERC20 transfer details
    }
  ],
  "erc721Transfers": [
    {
      // ERC721 transfer details
    }
  ],
  "erc1155Transfers": [
    {
      // ERC1155 transfer details
    }
  ],
  "internalTransactions": [
    {
      // Internal transaction details
    }
  ],
  "logs": [
    {
      // Log details
    }
  ]
}
```

### Metrics Events

#### Activity Metrics (`avalanche.metrics.activity.*`)
```json
{
  "chainId": "string",
  "timestamp": "string (ISO 8601)",
  "activeAddresses": "number",
  "activeSenders": "number",
  "txCount": "number",
  "newAddresses": "number",
  "uniqueContracts": "number"
}
```

#### Performance Metrics (`avalanche.metrics.performance.*`)
```json
{
  "chainId": "string",
  "timestamp": "string (ISO 8601)",
  "avgTps": "number",
  "maxTps": "number",
  "avgGps": "number",
  "maxGps": "number",
  "blockTime": "number",
  "avgLatency": "number"
}
```

#### Gas Metrics (`avalanche.metrics.gas.*`)
```json
{
  "chainId": "string",
  "timestamp": "string (ISO 8601)",
  "gasUsed": "number",
  "avgGasPrice": "number",
  "maxGasPrice": "number",
  "feesPaid": "number",
  "avgGasLimit": "number",
  "gasEfficiency": "number"
}
```

#### Cumulative Metrics (`avalanche.metrics.cumulative.*`)
```json
{
  "chainId": "string",
  "timestamp": "string (ISO 8601)",
  "totalTxs": "number",
  "totalAddresses": "number",
  "totalContracts": "number",
  "totalTokens": "number",
  "totalGasUsed": "number",
  "totalFeesCollected": "number"
}
```

### Staking Events

#### Validator Events (`avalanche.validators.*`)
```json
{
  // Validator details
}
```

#### Delegator Events (`avalanche.delegators.*`)
```json
{
  // Delegator details
}
```

### Bridge Events (`avalanche.bridges.*`)
```json
{
  // Teleporter transaction details
}
```

## Notes

1. All timestamps are in Unix timestamp format (seconds since epoch)
2. All numeric values that represent amounts (gas, fees, etc.) are sent as strings to preserve precision
3. The event type is used internally for routing but is not included in the Kafka message payload
4. Each topic follows the pattern `avalanche.{entity}.{action}` where:
   - `entity` is the type of data (chains, blocks, transactions, etc.)
   - `action` is the type of event (created, updated, etc.)

## Database Schema Considerations

When creating database tables from this data:

1. Use appropriate data types:
   - `string` fields for hashes and addresses
   - `numeric` fields for amounts (consider using `decimal` for precision)
   - `timestamp` fields for timestamps
   - `boolean` fields for boolean values
   - `jsonb` fields for nested objects and arrays

2. Consider indexing:
   - Primary keys (hashes, IDs)
   - Foreign keys (chainId, blockHash, etc.)
   - Frequently queried fields
   - Timestamp fields for time-based queries

3. Consider partitioning:
   - By chainId for multi-chain data
   - By timestamp for time-series data
   - By block number for block-related data 