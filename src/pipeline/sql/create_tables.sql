-- ═══════════════════════════════════════════════════
-- Avalanche Schema — versão de TESTE
-- ═══════════════════════════════════════════════════

-- 1.1 Chains ─ one row per network (C-, P-, X-, Fuji…)
CREATE TABLE IF NOT EXISTS chains (
    chain_id        String,
    name            String,
    description     String,
    rpc_url         String,
    ws_url          String,
    explorer_url    String,
    vm_info         String,                         -- arbitrary VM metadata
    native_token    String,                         -- e.g. "AVAX"
    features        String,                         -- e.g. ['subnets','amm']
    status          String   DEFAULT 'active',
    created_at      DateTime DEFAULT now(),
    updated_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY chain_id;

-- 1.2 Blocks
CREATE TABLE IF NOT EXISTS blocks (
    block_number    UInt64,
    chain_id        String,
    block_hash      String,
    parent_hash     String,
    timestamp       UInt64,                        -- unix/epoch
    tx_count        UInt32,
    gas_used        UInt64,
    gas_limit       UInt64,
    base_fee_per_gas UInt64,
    tps_avg         Float32,
    tps_peak        Float32,
    created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY block_number;

-- 1.3 Transactions
CREATE TABLE IF NOT EXISTS transactions (
    tx_hash         String,
    block_number    UInt64,
    chain_id        String,
    index_in_block  UInt32,
    from_address    String,
    to_address      String,
    value_raw       UInt256,                       -- wei
    gas             UInt64,
    gas_price       UInt64,
    status          UInt8,                         -- 1 success, 0 fail
    tx_type         UInt8,                         -- EIP-2718 / legacy
    created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY tx_hash;

-- 1.4 Internal Transactions
CREATE TABLE IF NOT EXISTS internal_transactions (
    id              UInt64,
    parent_tx_hash  String,
    trace_id        String,
    from_address    String,
    to_address      String,
    value_raw       UInt256,
    call_type       String,
    created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

-- 1.5 Logs (EVM event logs)
CREATE TABLE IF NOT EXISTS logs (
    id              UInt64,
    tx_hash         String,
    log_index       UInt32,
    address         String,
    topic0          String,
    topic1          String,
    topic2          String,
    topic3          String,
    data            String,
    removed         UInt8 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY id;

-- 1.6 Tokens (ERC-20/721/1155 metadata)
CREATE TABLE IF NOT EXISTS tokens (
    token_address   String,
    chain_id        String,
    name            String,
    symbol          String,
    decimals        UInt8,
    logo_uri        String,
    price_usd       Decimal(38,18)  -- NULL if unknown
) ENGINE = MergeTree()
ORDER BY token_address;

-- 1.7 ERC-20 transfers
CREATE TABLE IF NOT EXISTS erc20_transfers (
    id              UInt64,
    tx_hash         String,
    log_id          UInt64,
    token_address   String,
    from_address    String,
    to_address      String,
    amount_raw      UInt256
) ENGINE = MergeTree()
ORDER BY id;

-- 1.8 ERC-721 transfers
CREATE TABLE IF NOT EXISTS erc721_transfers (
    id              UInt64,
    tx_hash         String,
    log_id          UInt64,
    token_address   String,
    token_id        UInt256,
    from_address    String,
    to_address      String
) ENGINE = MergeTree()
ORDER BY id;

-- 1.9 ERC-1155 transfers
CREATE TABLE IF NOT EXISTS erc1155_transfers (
    id              UInt64,
    tx_hash         String,
    log_id          UInt64,
    token_address   String,
    token_id        UInt256,
    operator        String,
    from_address    String,
    to_address      String,
    amount_raw      UInt256
) ENGINE = MergeTree()
ORDER BY id;

-- 1.10 Validators
CREATE TABLE IF NOT EXISTS validators (
    node_id         String,
    chain_id        String,
    start_time      UInt64,
    end_time        UInt64,
    stake_amount_raw UInt256,
    uptime_percent  Float32
) ENGINE = MergeTree()
ORDER BY node_id;

-- 1.11 Delegators
CREATE TABLE IF NOT EXISTS delegators (
    id              UInt64,
    delegator_addr  String,
    node_id         String,
    stake_amount_raw UInt256,
    start_time      UInt64,
    end_time        UInt64,
    reward_address  String
) ENGINE = MergeTree()
ORDER BY id;

-- 1.12 Subnets
CREATE TABLE IF NOT EXISTS subnets (
    subnet_id       String,
    chain_id        String,
    created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY subnet_id;

-- 1.13 Blockchains (within subnets)
CREATE TABLE IF NOT EXISTS blockchains (
    blockchain_id   String,
    subnet_id       String,
    vm_id           String,
    name            String,
    created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY blockchain_id;

-- 1.14 Teleporter transactions (bridge AVAX ↔ other)
CREATE TABLE IF NOT EXISTS teleporter_tx (
    id              String,
    source_chain    String,
    dest_chain      String,
    asset_address   String,
    amount_raw      UInt256,
    sender          String,
    recipient       String,
    status          String,
    created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

-- Metrics Tables
CREATE TABLE IF NOT EXISTS staking_metrics (
    chain_id        String,
    day             Date,
    validator_count UInt32,
    delegator_count UInt32,
    total_staked_raw UInt256
) ENGINE = MergeTree()
ORDER BY (chain_id, day);

CREATE TABLE IF NOT EXISTS activity_metrics (
    chain_id        String,
    day             Date,
    active_addresses UInt32,
    active_senders   UInt32,
    tx_count         UInt64,
    new_addresses    UInt32,
    unique_contracts UInt32
) ENGINE = MergeTree()
ORDER BY (chain_id, day);

CREATE TABLE IF NOT EXISTS performance_metrics (
    chain_id        String,
    day             Date,
    tps_avg         Float32,
    tps_max         Float32,
    gps_avg         Float32,
    gps_max         Float32,
    block_time_ms   Float32,
    latency_ms      Float32
) ENGINE = MergeTree()
ORDER BY (chain_id, day);

CREATE TABLE IF NOT EXISTS gas_metrics (
    chain_id        String,
    day             Date,
    gas_used_raw    UInt256,
    gas_price_avg   UInt64,
    gas_price_max   UInt64,
    fees_paid_raw   UInt256,
    gas_limit_avg   UInt64,
    gas_efficiency  Float32
) ENGINE = MergeTree()
ORDER BY (chain_id, day);

CREATE TABLE IF NOT EXISTS cumulative_metrics (
    chain_id        String,
    total_tx        UInt64,
    total_addresses UInt64,
    total_contracts UInt64,
    total_tokens    UInt64,
    total_gas_used_raw UInt256,
    total_fees_raw  UInt256,
    updated_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY chain_id;

-- DEX Related Tables
CREATE TABLE IF NOT EXISTS dex_factories (
    id                  String,
    dex_name            String,                 -- e.g. 'TraderJoe', 'Dexalot'
    chain_id            String,
    pool_count          UInt64,
    tx_count            UInt64,
    total_volume_usd    Decimal(38,18),
    total_fees_usd      Decimal(38,18),
    tvl_usd             Decimal(38,18),
    tvl_usd_untracked   Decimal(38,18),
    owner               String,
    created_at          DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE IF NOT EXISTS pools (
    pool_id             String,
    dex_name            String,
    chain_id            String,
    token0              String,
    token1              String,
    fee_tier            UInt32,
    tvl_token0_raw      UInt256,
    tvl_token1_raw      UInt256,
    tvl_usd             Decimal(38,18),
    volume_usd          Decimal(38,18),
    created_at          DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY pool_id;

CREATE TABLE IF NOT EXISTS dex_swaps (
    id                  String,
    dex_name            String,
    tx_hash             String,
    block_timestamp     UInt64,
    sender              String,
    origin              String,
    recipient           String,
    amount_usd          Decimal(38,18),
    sqrt_price_x96      UInt256,
    tick                Int64,
    log_index           UInt64
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE IF NOT EXISTS price_bundles (
    bundle_id           String,
    chain_id            String,
    native_price_usd    Decimal(38,18),
    eth_price_usd       Decimal(38,18),
    block_timestamp     UInt64
) ENGINE = MergeTree()
ORDER BY bundle_id;

CREATE TABLE IF NOT EXISTS bridge_vaults (
    vault_id                   String,
    chain_id                   String,
    default_alien_deposit_fee  Decimal(38,18),
    default_alien_withdraw_fee Decimal(38,18),
    default_native_deposit_fee Decimal(38,18),
    default_native_withdraw_fee Decimal(38,18)
) ENGINE = MergeTree()
ORDER BY vault_id;

CREATE TABLE IF NOT EXISTS bridge_transactions (
    tx_id               String,
    chain_id            String,
    block_number        UInt64,
    event               String,
    from_address        String,
    to_address          String,
    value_raw           UInt256,
    gas_sent_raw        UInt256,
    gas_price_raw       UInt256,
    gas_limit_raw       UInt256,
    timestamp           UInt64,
    log_index           UInt64
) ENGINE = MergeTree()
ORDER BY tx_id;

CREATE TABLE IF NOT EXISTS dex_tokens (
    token_address               String,
    dex_name                    String,
    activation_block            UInt64,
    vault_id                    String,
    blacklisted                 UInt8,
    deposit_fee                 Decimal(38,18),
    is_native                   UInt8,
    total_supply_raw            UInt256,
    tvl_raw                     UInt256,
    tvl_usd                     Decimal(38,18),
    tx_count                    UInt64,
    volume_raw                  UInt256,
    volume_usd                  Decimal(38,18),
    untracked_volume_usd        Decimal(38,18),
    fees_usd                    Decimal(38,18),
    pool_count                  UInt64,
    derived_native              Decimal(38,18),
    derived_eth                 Decimal(38,18),
    tvl_usd_untracked           Decimal(38,18)
) ENGINE = MergeTree()
ORDER BY (token_address, dex_name);

CREATE TABLE IF NOT EXISTS raw_entities (
    id          String,
    entity_type String,
    deployment  String,
    block_ts    DateTime,
    cursor      String,
    data        String,
    metadata    String
) ENGINE = MergeTree()
ORDER BY id;

-- Keep the existing loaded_files table
CREATE TABLE IF NOT EXISTS loaded_files
(
    file_name String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY file_name;