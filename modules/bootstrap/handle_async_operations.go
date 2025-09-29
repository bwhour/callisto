package bootstrap

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/forbole/callisto/v4/modules/bootstrap/bootstrap_binding"
	"github.com/forbole/callisto/v4/types"
	assetstypes "github.com/imua-xyz/imuachain/x/assets/types"
	"github.com/rs/zerolog/log"
)

func (m *Module) updateStatesAfterStakerAssetChange(stakerAddr, assetAddr common.Address) (string, string, error) {
	stakerID, assetID, err := m.updateStakerAsset(stakerAddr, assetAddr)
	if err != nil {
		return "", "", err
	}

	// update the total deposit amount in asset states
	assetDepositAmount, err := m.bootstrapSession.DepositsByToken(assetAddr)
	if err != nil {
		return "", "", err
	}

	err = m.database.UpdateBootstrapTokenDepositAmount(assetID, assetDepositAmount.String())
	if err != nil {
		return "", "", err
	}
	return stakerID, assetID, nil
}

func (m *Module) updateStatesAfterDelegationChange(stakerAddr, assetAddr common.Address, validatorAddr string) error {
	// update the states of staker assets
	stakerID, assetID, err := m.updateStatesAfterStakerAssetChange(stakerAddr, assetAddr)
	if err != nil {
		return err
	}

	delegationAmount, err := m.bootstrapSession.Delegations(stakerAddr, validatorAddr, assetAddr)
	if err != nil {
		return err
	}

	// update the delegation states
	err = m.database.SaveBootstrapDelegationState(&types.BootstrapDelegationState{
		StakerID:     stakerID,
		AssetID:      assetID,
		OperatorAddr: validatorAddr,
		Delegated:    delegationAmount.String(),
		UpdatedAt:    time.Now(),
	})
	if err != nil {
		return err
	}

	// update the states of operator assets
	operatorAmount, err := m.bootstrapSession.DelegationsByValidator(validatorAddr, assetAddr)
	if err != nil {
		return err
	}
	validatorCount, err := m.bootstrapSession.GetValidatorsCount()
	if err != nil {
		return err
	}

	var validatorETHAddr common.Address
	for i := int64(0); i < validatorCount.Int64(); i++ {
		tmpValidatorETHAddr, err := m.bootstrapSession.RegisteredValidators(big.NewInt(i))
		if err != nil {
			return err
		}
		tmpValidatorAddr, err := m.bootstrapSession.EthToImAddress(tmpValidatorETHAddr)
		if err != nil {
			return err
		}
		if tmpValidatorAddr == validatorAddr {
			validatorETHAddr = tmpValidatorETHAddr
			break
		}
	}
	if validatorETHAddr == (common.Address{}) {
		return fmt.Errorf("can't find the validator in the registered list")
	}

	// get the self delegation amount
	selfDelegation, err := m.bootstrapSession.Delegations(validatorETHAddr, validatorAddr, assetAddr)
	if err != nil {
		return err
	}

	err = m.database.SaveBootstrapOperatorAsset(&types.BootstrapOperatorAsset{
		OperatorAddr: validatorAddr,
		AssetID:      assetID,
		TotalAmount:  operatorAmount.String(),
		SelfAmount:   selfDelegation.String(),
		OtherAmount:  big.NewInt(0).Sub(operatorAmount, selfDelegation).String(),
		UpdatedAt:    time.Now(),
	})
	if err != nil {
		return err
	}
	return nil
}

// RunAsyncOperations implements modules.AsyncOperationsModule
func (m *Module) RunAsyncOperations() {
	commonWatchCtx := &bind.WatchOpts{
		Context: m.ctx,
	}
	// create event channels for validator
	newValidatorCh := make(chan *bootstrap_binding.BootstrapValidatorRegistered)
	commissionUpdatedCh := make(chan *bootstrap_binding.BootstrapValidatorCommissionUpdated)
	keyReplaceCh := make(chan *bootstrap_binding.BootstrapValidatorKeyReplaced)

	newValidatorSub, err := m.bootstrapFilterer.WatchValidatorRegistered(commonWatchCtx, newValidatorCh)
	if err != nil {
		panic(fmt.Errorf("failed to watch validator registeration,err:%s", err))
	}
	defer newValidatorSub.Unsubscribe()

	commissionSub, err := m.bootstrapFilterer.WatchValidatorCommissionUpdated(commonWatchCtx, commissionUpdatedCh)
	if err != nil {
		panic(fmt.Errorf("failed to watch commission update,err:%s", err))
	}
	defer commissionSub.Unsubscribe()

	keyReplaceSub, err := m.bootstrapFilterer.WatchValidatorKeyReplaced(commonWatchCtx, keyReplaceCh)
	if err != nil {
		panic(fmt.Errorf("failed to watch key replace,err:%s", err))
	}
	defer keyReplaceSub.Unsubscribe()

	// create event channels for whitelist assets
	newAssetCh := make(chan *bootstrap_binding.BootstrapWhitelistTokenAdded)
	newAssetSub, err := m.bootstrapFilterer.WatchWhitelistTokenAdded(commonWatchCtx, newAssetCh)
	if err != nil {
		panic(fmt.Errorf("failed to watch whitelist token addition,err:%s", err))
	}
	defer newAssetSub.Unsubscribe()

	// create event channels for deposit, claim, delegation and undelegation
	depositCh := make(chan *bootstrap_binding.BootstrapDepositResult)
	depositSub, err := m.bootstrapFilterer.WatchDepositResult(commonWatchCtx, depositCh, nil, nil, nil)
	if err != nil {
		panic(fmt.Errorf("failed to watch token deposit,err:%s", err))
	}
	defer depositSub.Unsubscribe()

	claimCh := make(chan *bootstrap_binding.BootstrapClaimPrincipalResult)
	claimSub, err := m.bootstrapFilterer.WatchClaimPrincipalResult(commonWatchCtx, claimCh, nil, nil, nil)
	if err != nil {
		panic(fmt.Errorf("failed to watch token claim,err:%s", err))
	}
	defer claimSub.Unsubscribe()

	delegationCh := make(chan *bootstrap_binding.BootstrapDelegateResult)
	delegationSub, err := m.bootstrapFilterer.WatchDelegateResult(commonWatchCtx, delegationCh, nil, nil)
	if err != nil {
		panic(fmt.Errorf("failed to watch token delegation,err:%s", err))
	}
	defer delegationSub.Unsubscribe()

	undelegationCh := make(chan *bootstrap_binding.BootstrapUndelegateResult)
	undelegationSub, err := m.bootstrapFilterer.WatchUndelegateResult(commonWatchCtx, undelegationCh, nil, nil)
	if err != nil {
		panic(fmt.Errorf("failed to watch token undelegation,err:%s", err))
	}
	defer undelegationSub.Unsubscribe()

	for {
		select {
		// Handle subscription errors with graceful logging
		case err := <-newValidatorSub.Err():
			log.Err(err).Msg("new validator subscription error")
			// Continue running instead of terminating
		case err := <-commissionSub.Err():
			log.Err(err).Msg("commission update subscription error")
			// Continue running instead of terminating
		case err := <-keyReplaceSub.Err():
			log.Err(err).Msg("key replace subscription error")
			// Continue running instead of terminating
		case err := <-newAssetSub.Err():
			log.Err(err).Msg("whitelist token addition subscription error")
			// Continue running instead of terminating
		case err := <-depositSub.Err():
			log.Err(err).Msg("token deposit subscription error")
			// Continue running instead of terminating
		case err := <-claimSub.Err():
			log.Err(err).Msg("token claim subscription error")
			// Continue running instead of terminating
		case err := <-delegationSub.Err():
			log.Err(err).Msg("token delegation subscription error")
			// Continue running instead of terminating
		case err := <-undelegationSub.Err():
			log.Err(err).Msg("token undelegation subscription error")
			// Continue running instead of terminating
		case e := <-newValidatorCh:
			// save the new validator
			err := m.database.SaveBootstrapValidator(&types.BootstrapValidator{
				ValidatorEthAddress: e.EthAddress.String(),
				ValidatorIMAddress:  e.ValidatorAddress,
				ValidatorName:       e.Name,
				ConsensusPubKey:     hexutil.Encode(e.ConsensusPublicKey[:]),
				Rate:                e.Commission.Rate.String(),
				MaxRate:             e.Commission.MaxRate.String(),
				MaxChangeRate:       e.Commission.MaxChangeRate.String(),
				UpdatedAt:           time.Now(),
			})
			if err != nil {
				log.Err(err).Msg("failed to saving the new validator")
			}
		case e := <-commissionUpdatedCh:
			err = m.database.UpdateCommissionRate(e.ValidatorAddress, e.NewRate.String())
			if err != nil {
				log.Err(err).Str("validatorAddr", e.ValidatorAddress).Msg("failed to update the commission rate")
			}
		case e := <-keyReplaceCh:
			err = m.database.UpdateConsensusPubKey(e.ValidatorAddress, hexutil.Encode(e.NewConsensusPublicKey[:]))
			if err != nil {
				log.Err(err).Str("validatorAddr", e.ValidatorAddress).Msg("failed to replace the consensus key")
			}
		case e := <-newAssetCh:
			tokenCount, err := m.bootstrapSession.GetWhitelistedTokensCount()
			if err != nil {
				log.Err(err).Msg("failed to get the count of whitelisted tokens")
				continue
			}
			// get the token info by iterating all indexes
			for i := int64(0); i < tokenCount.Int64(); i++ {
				tokenInfo, err := m.bootstrapSession.GetWhitelistedTokenAtIndex(big.NewInt(i))
				if err != nil {
					log.Err(err).Int64("index", i).Msg("failed to get the count of whitelisted tokens")
					continue
				}
				if tokenInfo.TokenAddress == e.Token {
					_, assetID := assetstypes.GetStakerIDAndAssetID(m.Config.ETHLZChainID, nil, e.Token[:])
					err = m.database.SaveBootstrapToken(&types.BootstrapTokenState{
						BootstrapToken: types.BootstrapToken{
							AssetID:   assetID,
							Address:   e.Token.String(),
							Name:      tokenInfo.Name,
							Symbol:    tokenInfo.Symbol,
							Decimals:  tokenInfo.Decimals,
							LZChainID: m.Config.ETHLZChainID,
						},
						StakingTotalAmount: big.NewInt(0).String(),
						UpdatedAt:          time.Now(),
					})
					if err != nil {
						log.Err(err).Str("token", e.Token.String()).Msg("failed to save the whitelist asset")
					}
					break
				}
			}
		case e := <-depositCh:
			if e.Success {
				_, _, err := m.updateStatesAfterStakerAssetChange(e.Depositor, e.Token)
				if err != nil {
					log.Err(err).Str("depositor", e.Depositor.String()).Str("token", e.Token.String()).Str("amount", e.Amount.String()).Msg("failed to handle the deposit event")
				}
			}
		case e := <-claimCh:
			if e.Success {
				_, _, err := m.updateStatesAfterStakerAssetChange(e.Withdrawer, e.Token)
				if err != nil {
					log.Err(err).Str("withdrawer", e.Withdrawer.String()).Str("token", e.Token.String()).Str("amount", e.Amount.String()).Msg("failed to handle the claim event")
				}
			}
		case e := <-delegationCh:
			if e.Success {
				err := m.updateStatesAfterDelegationChange(e.Delegator, e.Token, e.Delegatee)
				if err != nil {
					log.Err(err).Str("delegator", e.Delegator.String()).Str("token", e.Token.String()).Str("validator", e.Delegatee).Str("amount", e.Amount.String()).Msg("failed to handle the delegation event")
				}
			}
		case e := <-undelegationCh:
			if e.Success {
				err := m.updateStatesAfterDelegationChange(e.Undelegator, e.Token, e.Undelegatee)
				if err != nil {
					log.Err(err).Str("undelegator", e.Undelegator.String()).Str("token", e.Token.String()).Str("validator", e.Undelegatee).Str("amount", e.Amount.String()).Msg("failed to handle the undelegation event")
				}
			}
		}
	}

}
