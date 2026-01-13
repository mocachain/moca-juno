package bucket

import (
	"context"
	"errors"
	"strconv"

	abci "github.com/cometbft/cometbft/abci/types"
	tmctypes "github.com/cometbft/cometbft/rpc/core/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/gogoproto/proto"
	storagetypes "github.com/evmos/evmos/v12/x/storage/types"

	"github.com/forbole/juno/v4/common"
	"github.com/forbole/juno/v4/log"
	"github.com/forbole/juno/v4/models"
)

var (
	EventCreateBucket            = proto.MessageName(&storagetypes.EventCreateBucket{})
	EventDeleteBucket            = proto.MessageName(&storagetypes.EventDeleteBucket{})
	EventUpdateBucketInfo        = proto.MessageName(&storagetypes.EventUpdateBucketInfo{})
	EventDiscontinueBucket       = proto.MessageName(&storagetypes.EventDiscontinueBucket{})
	EventMigrationBucket         = proto.MessageName(&storagetypes.EventMigrationBucket{})
	EventCompleteMigrationBucket = proto.MessageName(&storagetypes.EventCompleteMigrationBucket{})
	EventCancelMigrationBucket   = proto.MessageName(&storagetypes.EventCancelMigrationBucket{})
	EventRejectMigrateBucket     = proto.MessageName(&storagetypes.EventRejectMigrateBucket{})
)

var BucketEvents = map[string]bool{
	EventCreateBucket:            true,
	EventDeleteBucket:            true,
	EventUpdateBucketInfo:        true,
	EventDiscontinueBucket:       true,
	EventMigrationBucket:         true,
	EventCompleteMigrationBucket: true,
	EventCancelMigrationBucket:   true,
	EventRejectMigrateBucket:     true,
}

func (m *Module) ExtractEventStatements(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, event sdk.Event) (map[string][]interface{}, error) {
	return nil, nil
}

func (m *Module) HandleEvent(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, event sdk.Event) error {
	if !BucketEvents[event.Type] {
		return nil
	}

	typedEvent, err := sdk.ParseTypedEvent(abci.Event(event))
	if err != nil {
		log.Errorw("parse typed events error", "module", m.Name(), "event", event, "err", err)
		return err
	}

	switch event.Type {
	case EventCreateBucket:
		createBucket, ok := typedEvent.(*storagetypes.EventCreateBucket)
		if !ok {
			log.Errorw("type assert error", "type", "EventCreateBucket", "event", typedEvent)
			return errors.New("create bucket event assert error")
		}
		return m.handleCreateBucket(ctx, block, txHash, createBucket)
	case EventDeleteBucket:
		deleteBucket, ok := typedEvent.(*storagetypes.EventDeleteBucket)
		if !ok {
			log.Errorw("type assert error", "type", "EventDeleteBucket", "event", typedEvent)
			return errors.New("delete bucket event assert error")
		}
		return m.handleDeleteBucket(ctx, block, txHash, deleteBucket)
	case EventUpdateBucketInfo:
		updateBucketInfo, ok := typedEvent.(*storagetypes.EventUpdateBucketInfo)
		if !ok {
			log.Errorw("type assert error", "type", "EventUpdateBucketInfo", "event", typedEvent)
			return errors.New("update bucket event assert error")
		}
		return m.handleUpdateBucketInfo(ctx, block, txHash, updateBucketInfo)
	case EventDiscontinueBucket:
		discontinueBucket, ok := typedEvent.(*storagetypes.EventDiscontinueBucket)
		if !ok {
			log.Errorw("type assert error", "type", "EventDiscontinueBucket", "event", typedEvent)
			return errors.New("discontinue bucket event assert error")
		}
		return m.handleDiscontinueBucket(ctx, block, txHash, discontinueBucket)
	case EventMigrationBucket:
		migrationBucket, ok := typedEvent.(*storagetypes.EventMigrationBucket)
		if !ok {
			log.Errorw("type assert error", "type", "EventMigrationBucket", "event", typedEvent)
			return errors.New("migration bucket event assert error")
		}
		return m.handleMigrationBucket(ctx, block, txHash, migrationBucket)
	case EventCompleteMigrationBucket:
		completeMigrationBucket, ok := typedEvent.(*storagetypes.EventCompleteMigrationBucket)
		if !ok {
			log.Errorw("type assert error", "type", "EventCompleteMigrationBucket", "event", typedEvent)
			return errors.New("complete migrate bucket event assert error")
		}
		return m.handleCompleteMigrationBucket(ctx, block, txHash, completeMigrationBucket)
	case EventCancelMigrationBucket:
		cancelMigrationBucket, ok := typedEvent.(*storagetypes.EventCancelMigrationBucket)
		if !ok {
			log.Errorw("type assert error", "type", "EventCancelMigrationBucket", "event", typedEvent)
			return errors.New("cancel migration bucket event assert error")
		}
		return m.handleCancelMigrationBucket(ctx, block, txHash, cancelMigrationBucket)
	case EventRejectMigrateBucket:
		rejectMigrateBucket, ok := typedEvent.(*storagetypes.EventRejectMigrateBucket)
		if !ok {
			log.Errorw("type assert error", "type", "EventRejectMigrateBucket", "event", typedEvent)
			return errors.New("reject migrate bucket event assert error")
		}
		return m.handleRejectMigrateBucket(ctx, block, txHash, rejectMigrateBucket)
	}

	return nil
}

func (m *Module) handleCreateBucket(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, createBucket *storagetypes.EventCreateBucket) error {
	bucket := &models.Bucket{
		BucketID:                   common.BigToHash(createBucket.BucketId.BigInt()),
		BucketName:                 createBucket.BucketName,
		Owner:                      common.HexToAddress(createBucket.Owner),
		PaymentAddress:             common.HexToAddress(createBucket.PaymentAddress),
		GlobalVirtualGroupFamilyId: createBucket.GlobalVirtualGroupFamilyId,
		Operator:                   common.HexToAddress(createBucket.Owner),
		SourceType:                 createBucket.SourceType.String(),
		ChargedReadQuota:           createBucket.ChargedReadQuota,
		Visibility:                 createBucket.Visibility.String(),
		Status:                     createBucket.Status.String(),

		Removed:      false,
		CreateAt:     block.Block.Height,
		CreateTxHash: txHash,
		CreateTime:   createBucket.CreateAt,
		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.SaveBucket(ctx, bucket)
}

func (m *Module) handleDeleteBucket(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, deleteBucket *storagetypes.EventDeleteBucket) error {
	bucket := &models.Bucket{
		BucketID:                   common.BigToHash(deleteBucket.BucketId.BigInt()),
		BucketName:                 deleteBucket.BucketName,
		Owner:                      common.HexToAddress(deleteBucket.Owner),
		GlobalVirtualGroupFamilyId: deleteBucket.GlobalVirtualGroupFamilyId,

		Removed:      true,
		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateBucket(ctx, bucket)
}

func (m *Module) handleDiscontinueBucket(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, discontinueBucket *storagetypes.EventDiscontinueBucket) error {
	bucket := &models.Bucket{
		BucketID:     common.BigToHash(discontinueBucket.BucketId.BigInt()),
		BucketName:   discontinueBucket.BucketName,
		DeleteReason: discontinueBucket.Reason,
		DeleteAt:     discontinueBucket.DeleteAt,
		Status:       storagetypes.BUCKET_STATUS_DISCONTINUED.String(),

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateBucket(ctx, bucket)
}

func (m *Module) handleUpdateBucketInfo(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, updateBucket *storagetypes.EventUpdateBucketInfo) error {
	bucket := &models.Bucket{
		BucketName:                 updateBucket.BucketName,
		BucketID:                   common.BigToHash(updateBucket.BucketId.BigInt()),
		ChargedReadQuota:           updateBucket.ChargedReadQuota,
		PaymentAddress:             common.HexToAddress(updateBucket.PaymentAddress),
		Visibility:                 updateBucket.Visibility.String(),
		GlobalVirtualGroupFamilyId: updateBucket.GlobalVirtualGroupFamilyId,

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateBucket(ctx, bucket)
}

func (m *Module) handleMigrationBucket(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, migrationBucket *storagetypes.EventMigrationBucket) error {
	// Set MIGRATING status and record migration start
	migrationStartTime := block.Block.Time.UTC().Unix()
	bucket := &models.Bucket{
		BucketID:           common.BigToHash(migrationBucket.BucketId.BigInt()),
		BucketName:         migrationBucket.BucketName,
		Status:             storagetypes.BUCKET_STATUS_MIGRATING.String(),
		MigrationStartTime: &migrationStartTime,
		DestPrimarySPID:    strconv.FormatUint(uint64(migrationBucket.DstPrimarySpId), 10),
		
		// Clear other migration fields
		MigrationCompleteTime: nil,
		MigrationRejectReason: "",

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateBucket(ctx, bucket)
}

func (m *Module) handleCompleteMigrationBucket(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, completeMigrationBucket *storagetypes.EventCompleteMigrationBucket) error {
	// Critical fix: reset status to CREATED and update primary SP
	migrationCompleteTime := block.Block.Time.UTC().Unix()
	bucket := &models.Bucket{
		BucketID:                   common.BigToHash(completeMigrationBucket.BucketId.BigInt()),
		BucketName:                 completeMigrationBucket.BucketName,
		GlobalVirtualGroupFamilyId: completeMigrationBucket.GlobalVirtualGroupFamilyId,
		Status:                     storagetypes.BUCKET_STATUS_CREATED.String(), // Reset to CREATED
		MigrationCompleteTime:      &migrationCompleteTime,

		// Clear migration-related fields
		MigrationStartTime:    nil,
		DestPrimarySPID:       "",
		MigrationRejectReason: "",

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateBucket(ctx, bucket)
}

func (m *Module) handleCancelMigrationBucket(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, cancelMigrationBucket *storagetypes.EventCancelMigrationBucket) error {
	// Critical fix: restore status to CREATED
	bucket := &models.Bucket{
		BucketID:   common.BigToHash(cancelMigrationBucket.BucketId.BigInt()),
		BucketName: cancelMigrationBucket.BucketName,
		Status:     storagetypes.BUCKET_STATUS_CREATED.String(), // Restore to CREATED

		// Clear migration-related fields
		MigrationStartTime:    nil,
		DestPrimarySPID:       "",
		MigrationCompleteTime: nil,
		MigrationRejectReason: "",

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateBucket(ctx, bucket)
}

func (m *Module) handleRejectMigrateBucket(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, rejectMigrateBucket *storagetypes.EventRejectMigrateBucket) error {
	// Critical fix: restore status to CREATED
	bucket := &models.Bucket{
		BucketID:              common.BigToHash(rejectMigrateBucket.BucketId.BigInt()),
		BucketName:            rejectMigrateBucket.BucketName,
		Status:                storagetypes.BUCKET_STATUS_CREATED.String(), // Restore to CREATED
		MigrationRejectReason: "Migration rejected", // Record rejection

		// Clear migration-related fields
		MigrationStartTime:    nil,
		DestPrimarySPID:       "",
		MigrationCompleteTime: nil,

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateBucket(ctx, bucket)
}
