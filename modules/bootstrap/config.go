package bootstrap

import (
	"fmt"

	callistotypes "github.com/forbole/callisto/v4/types"
	"gopkg.in/yaml.v3"
)

const (
	// VirtualAddress is the unified virtual address used for all chain asset ID construction
	VirtualAddress = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type Config struct {
	ETHHttp       string `yaml:"eth_http"`
	ETHWebsocket  string `yaml:"eth_websocket"`
	BTCRPC        string `yaml:"btc_rpc"`
	XRPRPC        string `yaml:"xrp_rpc"`
	BootstrapAddr string `yaml:"bootstrap_addr"`
	// Update intervals specify the interval in minutes at which states are refreshed
	ETHUpdateInterval   int    `yaml:"eth_update_interval"`
	BTCUpdateInterval   int    `yaml:"btc_update_interval"`
	XRPUpdateInterval   int    `yaml:"xrp_update_interval"`
	PriceUpdateInterval int    `yaml:"price_update_interval"`
	ETHLZChainID        uint64 `yaml:"eth_lz_chain_id"`
	// BTC vault address for bootstrap deposits
	BTCVaultAddr string `yaml:"btc_vault_addr"`
	// XRP vault address for bootstrap deposits
	XRPVaultAddr string `yaml:"xrp_vault_addr"`
	// Minimum confirmations required for BTC transactions
	BTCMinConfirmations int `yaml:"btc_min_confirmations"`
	// Minimum confirmations required for XRP transactions
	XRPMinConfirmations int `yaml:"xrp_min_confirmations"`
	// Minimum BTC amount in satoshis (0.001 BTC = 100000 satoshis)
	BTCMinAmount int64 `yaml:"btc_min_amount"`
	// Minimum XRP amount in drops (50 XRP = 50000000 drops)
	XRPMinAmount int64 `yaml:"xrp_min_amount"`
	// XRP destination tag for bootstrap deposits
	XRPDestinationTag int64 `yaml:"xrp_destination_tag"`
	// Maximum iterations for transaction fetching to prevent infinite loops
	MaxFetchIterations int `yaml:"max_fetch_iterations"`
	// Incremental scanning configuration
	BTCStartHeight    int64                                `yaml:"btc_start_height"` // BTC scanning start height
	XRPStartLedger    int64                                `yaml:"xrp_start_ledger"` // XRP scanning start ledger
	ScanBatchSize     int                                  `yaml:"scan_batch_size"`  // Batch size for scanning
	ReorgDepth        int                                  `yaml:"reorg_depth"`      // Reorganization detection depth
	ClientChainInfos  []callistotypes.BootstrapClientChain `yaml:"client_chain_infos"`
	StakingTokenInfos []callistotypes.BootstrapToken       `yaml:"staking_token_infos"`
	TokenOracleFeeds  []callistotypes.OracleFeed           `yaml:"token_oracle_feeds"`
}

func ParseConfig(bz []byte) (*Config, error) {
	type T struct {
		Config *Config `yaml:"bootstrap"`
	}
	var cfg T
	err := yaml.Unmarshal(bz, &cfg)
	if err != nil {
		return nil, fmt.Errorf("parse bootstrap config: %w", err)
	}
	if cfg.Config == nil {
		return nil, fmt.Errorf("bootstrap config missing")
	}

	// Validate configuration
	if err := cfg.Config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid bootstrap config: %w", err)
	}

	return cfg.Config, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.ETHHttp == "" {
		return fmt.Errorf("eth_http is required")
	}
	if c.ETHWebsocket == "" {
		return fmt.Errorf("eth_websocket is required")
	}
	if c.BTCRPC == "" {
		return fmt.Errorf("btc_rpc is required")
	}
	if c.XRPRPC == "" {
		return fmt.Errorf("xrp_rpc is required")
	}
	if c.BootstrapAddr == "" {
		return fmt.Errorf("bootstrap_addr is required")
	}
	if c.ETHUpdateInterval <= 0 {
		return fmt.Errorf("eth_update_interval must be positive")
	}
	if c.BTCUpdateInterval <= 0 {
		return fmt.Errorf("btc_update_interval must be positive")
	}
	if c.XRPUpdateInterval <= 0 {
		return fmt.Errorf("xrp_update_interval must be positive")
	}
	if c.PriceUpdateInterval <= 0 {
		return fmt.Errorf("price_update_interval must be positive")
	}
	if c.BTCMinConfirmations <= 0 {
		return fmt.Errorf("btc_min_confirmations must be positive")
	}
	if c.XRPMinConfirmations <= 0 {
		return fmt.Errorf("xrp_min_confirmations must be positive")
	}
	return nil
}
