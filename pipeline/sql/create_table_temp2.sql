CREATE TABLE IF NOT EXISTS tb_message_execution_rate (
    timestamp DateTime,
    subnet_id String,
    message_type String,
    execution_rate Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_ic_nodes_count (
    timestamp DateTime,
    subnet_id String,
    total_nodes UInt64,
    up_nodes UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_ic_memory_total (
    timestamp DateTime,
    subnet_id String,
    total_memory UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_ic_cpu_cores (
    timestamp DateTime,
    subnet_id String,
    cpu_cores UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_ic_memory_usage (
    timestamp DateTime,
    subnet_id String,
    memory_usage UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_block_rate (
    timestamp DateTime,
    subnet_id String,
    block_rate Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_boundary_nodes_count (
    timestamp DateTime,
    boundary_nodes_count UInt32
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_governance_neurons_total (
    timestamp DateTime,
    total_neurons UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_internet_identity_user_count (
    timestamp DateTime,
    user_count UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_governance_voting_power_total (
    timestamp DateTime,
    voting_power_total UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_average_governance_voting_power_total (
    timestamp DateTime,
    avg_voting_power_total Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_cycle_burn_rate (
    timestamp DateTime,
    subnet_id String,
    cycle_burn_rate Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_ic_subnet_total (
    timestamp DateTime,
    subnet_total UInt32
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_last_reward_event (
    timestamp DateTime,
    last_reward_event UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_latest_reward_event_total_available (
    timestamp DateTime,
    total_available_reward_event UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_average_last_reward_event (
    timestamp DateTime,
    avg_last_reward_event Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_community_fund_total_staked (
    timestamp DateTime,
    total_staked UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_community_fund_total_maturity (
    timestamp DateTime,
    total_maturity UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_total_rewardable_nodes_count (
    timestamp DateTime,
    rewardable_nodes_count UInt32
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_registered_canisters_count (
    timestamp DateTime,
    subnet_id String,
    running_canisters Float64,
    stopped_canisters Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_total_ic_energy_consumption_rate (
    timestamp DateTime,
    energy_consumption_rate Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_node_energy_consumption_rate (
    timestamp DateTime,
    node_id String,
    energy_consumption_rate Float64,
    recorded_data Boolean
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_transaction_rate (
    timestamp DateTime,
    transaction_rate Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_max_transactions_per_sec_90_days (
    timestamp DateTime,
    max_transactions_per_sec Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_average_cycle_burn_rate (
    timestamp DateTime,
    avg_cycle_burn_rate Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_icp_txn_vs_eth_txn (
    timestamp DateTime,
    txn_comparison Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_eth_equivalent_txns (
    timestamp DateTime,
    eth_equivalent_txns Float64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_governance_not_dissolving_neurons_e8s_1year (
    timestamp DateTime,
    not_dissolving_neurons_e8s UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_governance_dissolving_neurons_e8s_1year (
    timestamp DateTime,
    dissolving_neurons_e8s UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_governance_not_dissolving_neurons_staked_maturity_e8s_1year (
    timestamp DateTime,
    not_dissolving_staked_maturity_e8s UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_governance_dissolving_neurons_staked_maturity_e8s_1year (
    timestamp DateTime,
    dissolving_staked_maturity_e8s UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_messages_counts (
    timestamp DateTime,
    subnet_id String,
    messages_count UInt64
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS tb_neurons (
    neuron_id String,
    age_seconds UInt64,
    created_timestamp_seconds UInt64,
    description Nullable(String),
    dissolve_delay_seconds UInt64,
    is_gtc Boolean,
    is_known Boolean,
    community_fund_joined_at Nullable(UInt64),
    neuron_name Nullable(String),
    stake_e8s UInt64,
    neuron_state Enum('Spawning', 'Dissolving', 'NotDissolving', 'Dissolved'),
    visibility Enum('PRIVATE', 'PUBLIC'),
    voting_power UInt64,
    retrieved_at_timestamp_seconds UInt64,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY neuron_id;

CREATE TABLE IF NOT EXISTS tb_neuron_recent_ballots (
    neuron_id String,
    proposal_id UInt64,
    vote Enum8('no_vote' = 0, 'yes' = 1, 'no' = 2)
) ENGINE = MergeTree()
ORDER BY neuron_id;

-- Tabela: reward_node_providers_timeseries
CREATE TABLE IF NOT EXISTS reward_node_providers_timeseries (
    reward_id String PRIMARY KEY,   -- ID único do registro de recompensa
    timestamp_seconds UInt64        -- Timestamp da recompensa (Unix Timestamp)
) ENGINE = MergeTree()
ORDER BY reward_id;

-- Tabela: reward_node_providers
CREATE TABLE IF NOT EXISTS reward_node_providers (
    id UInt32 PRIMARY KEY,                       -- ID único da recompensa
    node_provider String,                        -- Identificador do provedor de nós
    amount_e8s UInt64,                           -- Quantidade da recompensa em e8s
    to_account String,                           -- Conta de destino para o recebimento da recompensa
    maximum_node_provider_rewards_e8s UInt64,    -- Valor máximo permitido para a recompensa
    minimum_xdr_permyriad_per_icp UInt32,        -- Taxa mínima de conversão XDR/ICP em permyriad
    proposal_id Nullable(UInt32),                -- ID da proposta de atualização, caso existente
    registry_version UInt32,                     -- Versão do registro
    reward_mode Enum('RewardToAccount', 'Other'),-- Modo de recompensa (ex: para conta)
    timestamp_seconds UInt64,                    -- Data e hora da transação (Unix Timestamp)
    updated_at DateTime,                         -- Data de atualização do registro
    xdr_conversion_rate_timestamp UInt64,        -- Timestamp da taxa de conversão XDR/ICP
    xdr_permyriad_per_icp UInt32                 -- Taxa de conversão XDR/ICP em permyriad
) ENGINE = MergeTree()
ORDER BY timestamp_seconds;


-- Tabela principal: subnets
CREATE TABLE IF NOT EXISTS subnets (
    subnet_id String PRIMARY KEY,  -- Identificador único da subnet
    display_name Nullable(String), -- Nome de exibição da subnet, pode ser nulo
    subnet_type String,            -- Tipo da subnet (ex: system, application)
    subnet_specialization Nullable(String), -- Especialização da subnet (pode ser nulo)
    total_nodes UInt16,            -- Total de nós que compõem a subnet
    up_nodes UInt16,               -- Quantidade de nós em operação
    running_canisters UInt32,      -- Número de canisters em execução
    stopped_canisters UInt32       -- Número de canisters parados
) ENGINE = MergeTree()
ORDER BY subnet_id;

-- Tabela auxiliar: replica_versions
CREATE TABLE IF NOT EXISTS replica_versions (
    subnet_id String,              -- Identificador único da subnet, chave estrangeira
    executed_timestamp_seconds UInt64, -- Data de execução em Unix Timestamp
    proposal_id String,            -- ID da proposta de atualização de versão
    replica_version_id String      -- ID da versão do software em execução
) ENGINE = MergeTree()
ORDER BY (subnet_id, executed_timestamp_seconds);

