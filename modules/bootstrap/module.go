package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/xrpscan/xrpl-go"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	callistodb "github.com/forbole/callisto/v4/database"
	"github.com/forbole/callisto/v4/modules/bootstrap/bootstrap_binding"
	"github.com/forbole/callisto/v4/types"
	"github.com/forbole/juno/v5/modules"
	"github.com/forbole/juno/v5/types/config"
)

var (
	_ modules.Module                   = &Module{}
	_ modules.PeriodicOperationsModule = &Module{}
)

type Module struct {
	database      *callistodb.Db
	EthHTTPClient *ethclient.Client
	EthWSClient   *ethclient.Client
	XrpClient     *xrpl.Client
	Config        Config
	BootstrapAddr common.Address
	// No dedicated clients for BTC and XRP, since we may call their RPCs
	// directly using the http package.
	ctx               context.Context
	bootstrapSession  *bootstrap_binding.BootstrapCallerSession
	bootstrapFilterer *bootstrap_binding.BootstrapFilterer
	// Address mappings for 1-1 binding validation
	btcAddressMappings map[string]string // bitcoin -> imuachain
	xrpAddressMappings map[string]string // xrp -> imuachain
}

// NewModule builds a new Module instance
func NewModule(
	cfg config.Config,
	database *callistodb.Db,
) *Module {
	bz, err := cfg.GetBytes()
	if err != nil {
		panic(err)
	}

	bootstrapCfg, err := ParseConfig(bz)
	if err != nil {
		panic(err)
	}
	if !common.IsHexAddress(bootstrapCfg.BootstrapAddr) {
		panic(fmt.Sprintf("invalid bootstrap address:%s", bootstrapCfg.BootstrapAddr))
	}
	bootstrapAddr := common.HexToAddress(bootstrapCfg.BootstrapAddr)

	// Initialize ETH clients
	ethHTTPClient, ethWSClient, err := initETHClients(bootstrapCfg)
	if err != nil {
		panic(err)
	}

	// Initialize XRP client
	xrpClient, err := initXRPClient(bootstrapCfg.XRPRPC)
	if err != nil {
		panic(err)
	}

	// Initialize bootstrap contract sessions
	ctx := context.Background()
	bootstrapSession, bootstrapFilterer, err := initBootstrapContracts(ctx, bootstrapAddr, ethHTTPClient, ethWSClient)
	if err != nil {
		panic(err)
	}

	module := &Module{
		database:           database,
		EthHTTPClient:      ethHTTPClient,
		EthWSClient:        ethWSClient,
		XrpClient:          xrpClient,
		Config:             *bootstrapCfg,
		BootstrapAddr:      bootstrapAddr,
		ctx:                ctx,
		bootstrapSession:   bootstrapSession,
		bootstrapFilterer:  bootstrapFilterer,
		btcAddressMappings: make(map[string]string),
		xrpAddressMappings: make(map[string]string),
	}

	// Initialize address bindings from database
	if err := module.loadExistingBindings(); err != nil {
		panic(fmt.Errorf("failed to load existing address bindings: %w", err))
	}

	// Initialize bootstrap tokens for BTC and XRP if not exists
	if err := module.initializeBootstrapTokens(); err != nil {
		panic(fmt.Errorf("failed to initialize bootstrap tokens: %w", err))
	}

	return module
}

// initETHClients initializes Ethereum HTTP and WebSocket clients
func initETHClients(cfg *Config) (*ethclient.Client, *ethclient.Client, error) {
	httpRC, err := rpc.DialContext(context.Background(), cfg.ETHHttp)
	if err != nil {
		return nil, nil, err
	}
	ethHTTPClient := ethclient.NewClient(httpRC)

	websocketRC, err := rpc.DialContext(context.Background(), cfg.ETHWebsocket)
	if err != nil {
		return nil, nil, err
	}
	ethWSClient := ethclient.NewClient(websocketRC)

	return ethHTTPClient, ethWSClient, nil
}

// initXRPClient initializes XRP client
func initXRPClient(xrpRPC string) (*xrpl.Client, error) {
	xrpClient := xrpl.NewClient(xrpl.ClientConfig{URL: xrpRPC})
	err := xrpClient.Ping([]byte("PING"))
	if err != nil {
		return nil, fmt.Errorf("failed to ping XRP client at %s: %w", xrpRPC, err)
	}
	return xrpClient, nil
}

// initBootstrapContracts initializes bootstrap contract sessions
func initBootstrapContracts(ctx context.Context, bootstrapAddr common.Address, ethHTTPClient, ethWSClient *ethclient.Client) (*bootstrap_binding.BootstrapCallerSession, *bootstrap_binding.BootstrapFilterer, error) {
	bootstrapCaller, err := bootstrap_binding.NewBootstrapCaller(bootstrapAddr, ethHTTPClient)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to new bootstrap caller,err:%s", err)
	}
	bootstrapSession := &bootstrap_binding.BootstrapCallerSession{
		Contract: bootstrapCaller,
		CallOpts: bind.CallOpts{Context: ctx},
	}

	bootstrapFilterer, err := bootstrap_binding.NewBootstrapFilterer(bootstrapAddr, ethWSClient)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to new bootstrap filterer,err:%s", err)
	}

	return bootstrapSession, bootstrapFilterer, nil
}

// Name implements modules.Module
func (m *Module) Name() string {
	return "bootstrap"
}

// loadExistingBindings loads existing address bindings from database into memory
func (m *Module) loadExistingBindings() error {
	// Load BTC bindings
	btcBindings, err := m.database.GetAddressBindings("BTC")
	if err != nil {
		return fmt.Errorf("failed to load BTC address bindings: %w", err)
	}

	for _, binding := range btcBindings {
		m.btcAddressMappings[binding.SourceAddr] = binding.TargetAddr
	}

	// Load XRP bindings
	xrpBindings, err := m.database.GetAddressBindings("XRP")
	if err != nil {
		return fmt.Errorf("failed to load XRP address bindings: %w", err)
	}

	for _, binding := range xrpBindings {
		m.xrpAddressMappings[binding.SourceAddr] = binding.TargetAddr
	}

	return nil
}

// initializeBootstrapTokens initializes bootstrap client chains and tokens if they don't exist
func (m *Module) initializeBootstrapTokens() error {
	// Define bootstrap client chains for BTC and XRP
	clientChains := []types.BootstrapClientChain{
		{
			Name:      "Bitcoin",
			MetaInfo:  `{"native_currency":"BTC","decimals":8}`,
			LZChainID: 1, // BTC LayerZero chain ID
		},
		{
			Name:      "XRP",
			MetaInfo:  `{"native_currency":"XRP","decimals":6}`,
			LZChainID: 2, // XRP LayerZero chain ID
		},
	}

	// Save client chains first
	for _, chain := range clientChains {
		err := m.database.SaveBootstrapClientChain(&chain)
		if err != nil {
			return fmt.Errorf("failed to save bootstrap client chain %s: %w", chain.Name, err)
		}
	}

	// Define bootstrap token states for BTC and XRP
	tokenStates := []types.BootstrapTokenState{
		{
			BootstrapToken: types.BootstrapToken{
				AssetID:   VirtualAddress + "_0x1", // BTC
				Name:      "Bitcoin",
				Symbol:    "BTC",
				Address:   "0x0000000000000000000000000000000000000000", // Placeholder address for BTC
				Decimals:  8,                                            // BTC has 8 decimal places (satoshis)
				LZChainID: 1,                                            // BTC chain ID = 1
			},
			StakingTotalAmount: "0",
			UpdatedAt:          time.Now(),
		},
		{
			BootstrapToken: types.BootstrapToken{
				AssetID:   VirtualAddress + "_0x2", // XRP
				Name:      "XRP",
				Symbol:    "XRP",
				Address:   "0x0000000000000000000000000000000000000000", // Placeholder address for XRP
				Decimals:  6,                                            // XRP has 6 decimal places (drops)
				LZChainID: 2,                                            // XRP chain ID = 2
			},
			StakingTotalAmount: "0",
			UpdatedAt:          time.Now(),
		},
	}

	// Save each token state if it doesn't exist
	for _, tokenState := range tokenStates {
		err := m.database.SaveBootstrapToken(&tokenState)
		if err != nil {
			return fmt.Errorf("failed to save bootstrap token %s: %w", tokenState.Symbol, err)
		}
	}

	return nil
}
