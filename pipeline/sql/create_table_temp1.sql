CREATE TABLE
  IF NOT EXISTS tb_ckBTC_height (timestamp DateTime, height UInt64) ENGINE = MergeTree ()
ORDER BY
  timestamp;

CREATE TABLE
  IF NOT EXISTS tb_ckBTC_number_txo (timestamp DateTime, txos UInt64) ENGINE = MergeTree ()
ORDER BY
  timestamp;

CREATE TABLE
  IF NOT EXISTS tb_ckBTC_stable_memory (timestamp DateTime, memory UInt64) ENGINE = MergeTree ()
ORDER BY
  timestamp;

CREATE TABLE
  IF NOT EXISTS tb_icp_height (timestamp DateTime, height UInt64) ENGINE = MergeTree ()
ORDER BY
  timestamp;

CREATE TABLE
  IF NOT EXISTS tb_icp_height_overtime (timestamp DateTime, height UInt64) ENGINE = MergeTree ()
ORDER BY
  timestamp;

CREATE TABLE
  IF NOT EXISTS tb_icp_canister (
    canister_id String,
    controllers String,
    enabled Boolean,
    id UInt32,
    module_hash String,
    name String,
    subnet_id String,
    updated_at DateTime,
    upgrades String
  ) ENGINE = MergeTree ()
ORDER BY
  id;

CREATE TABLE
  IF NOT EXISTS tb_icp_daily_stats (
    average_query_transactions_per_second UInt64,
    average_transactions_per_second UInt64,
    average_update_transactions_per_second UInt64,
    blocks_per_second_average UInt64,
    canister_memory_usage_bytes UInt64,
    circulating_supply UInt64,
    ckbtc_total_supply UInt64,
    community_fund_total_maturity UInt64,
    cycle_burn_rate_average UInt64,
    daily_active_users UInt64,
    day String,
    estimated_rewards_percentage String,
    governance_latest_reward_round_total_available_e8s UInt64,
    governance_neuron_fund_total_maturity_e8s_equivalent UInt64,
    governance_neuron_fund_total_staked_e8s UInt64,
    governance_neurons_total UInt32,
    governance_total_locked_e8s UInt64,
    icp_burned_fees UInt64,
    icp_burned_total UInt64,
    internet_identity_user_count UInt32,
    max_query_transactions_per_second UInt32,
    max_total_transactions_per_second UInt32,
    max_update_transactions_per_second UInt32,
    proposals_count UInt32,
    registered_canisters_count UInt32,
    timestamp DateTime,
    total_transactions UInt64,
    unique_accounts_per_day UInt32
  ) ENGINE = MergeTree ()
ORDER BY
  timestamp;

-- Analisar se vale a pena usar
-- CREATE TABLE
--   IF NOT EXISTS tb_ckBTC (
--     timestamp DateTime,
--     height UInt64,
--     txos UInt64,
--     memory UInt64
--   ) ENGINE = MergeTree ()
-- ORDER BY
--   timestamp;