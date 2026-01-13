package object

import (
	"context"
	"errors"

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
	EventCreateObject               = proto.MessageName(&storagetypes.EventCreateObject{})
	EventCancelCreateObject         = proto.MessageName(&storagetypes.EventCancelCreateObject{})
	EventSealObject                 = proto.MessageName(&storagetypes.EventSealObject{})
	EventCopyObject                 = proto.MessageName(&storagetypes.EventCopyObject{})
	EventDeleteObject               = proto.MessageName(&storagetypes.EventDeleteObject{})
	EventRejectSealObject           = proto.MessageName(&storagetypes.EventRejectSealObject{})
	EventDiscontinueObject          = proto.MessageName(&storagetypes.EventDiscontinueObject{})
	EventUpdateObjectInfo           = proto.MessageName(&storagetypes.EventUpdateObjectInfo{})
	EventUpdateObjectContent        = proto.MessageName(&storagetypes.EventUpdateObjectContent{})
	EventUpdateObjectContentSuccess = proto.MessageName(&storagetypes.EventUpdateObjectContentSuccess{})
	EventCancelUpdateObjectContent  = proto.MessageName(&storagetypes.EventCancelUpdateObjectContent{})
	EventMirrorObject               = proto.MessageName(&storagetypes.EventMirrorObject{})
	EventMirrorObjectResult         = proto.MessageName(&storagetypes.EventMirrorObjectResult{})
)

var ObjectEvents = map[string]bool{
	EventCreateObject:               true,
	EventCancelCreateObject:         true,
	EventSealObject:                 true,
	EventCopyObject:                 true,
	EventDeleteObject:               true,
	EventRejectSealObject:           true,
	EventDiscontinueObject:          true,
	EventUpdateObjectInfo:           true,
	EventUpdateObjectContent:        true,
	EventUpdateObjectContentSuccess: true,
	EventCancelUpdateObjectContent:  true,
	EventMirrorObject:               true,
	EventMirrorObjectResult:         true,
}

func (m *Module) ExtractEventStatements(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, event sdk.Event) (map[string][]interface{}, error) {
	return nil, nil
}

func (m *Module) HandleEvent(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, event sdk.Event) error {
	if !ObjectEvents[event.Type] {
		return nil
	}

	typedEvent, err := sdk.ParseTypedEvent(abci.Event(event))
	if err != nil {
		log.Errorw("parse typed events error", "module", m.Name(), "event", event, "err", err)
		return err
	}

	switch event.Type {
	case EventCreateObject:
		createObject, ok := typedEvent.(*storagetypes.EventCreateObject)
		if !ok {
			log.Errorw("type assert error", "type", "EventCreateObject", "event", typedEvent)
			return errors.New("create object event assert error")
		}
		return m.handleCreateObject(ctx, block, txHash, createObject)
	case EventCancelCreateObject:
		cancelCreateObject, ok := typedEvent.(*storagetypes.EventCancelCreateObject)
		if !ok {
			log.Errorw("type assert error", "type", "EventCancelCreateObject", "event", typedEvent)
			return errors.New("cancel create object event assert error")
		}
		return m.handleCancelCreateObject(ctx, block, txHash, cancelCreateObject)
	case EventSealObject:
		sealObject, ok := typedEvent.(*storagetypes.EventSealObject)
		if !ok {
			log.Errorw("type assert error", "type", "EventSealObject", "event", typedEvent)
			return errors.New("seal object event assert error")
		}
		return m.handleSealObject(ctx, block, txHash, sealObject)
	case EventCopyObject:
		copyObject, ok := typedEvent.(*storagetypes.EventCopyObject)
		if !ok {
			log.Errorw("type assert error", "type", "EventCopyObject", "event", typedEvent)
			return errors.New("copy object event assert error")
		}
		return m.handleCopyObject(ctx, block, txHash, copyObject)
	case EventDeleteObject:
		deleteObject, ok := typedEvent.(*storagetypes.EventDeleteObject)
		if !ok {
			log.Errorw("type assert error", "type", "EventDeleteObject", "event", typedEvent)
			return errors.New("delete object event assert error")
		}
		return m.handleDeleteObject(ctx, block, txHash, deleteObject)
	case EventRejectSealObject:
		rejectSealObject, ok := typedEvent.(*storagetypes.EventRejectSealObject)
		if !ok {
			log.Errorw("type assert error", "type", "EventRejectSealObject", "event", typedEvent)
			return errors.New("reject seal object event assert error")
		}
		return m.handleRejectSealObject(ctx, block, txHash, rejectSealObject)
	case EventDiscontinueObject:
		discontinueObject, ok := typedEvent.(*storagetypes.EventDiscontinueObject)
		if !ok {
			log.Errorw("type assert error", "type", "EventDiscontinueObject", "event", typedEvent)
			return errors.New("discontinue object event assert error")
		}
		return m.handleEventDiscontinueObject(ctx, block, txHash, discontinueObject)
	case EventUpdateObjectInfo:
		updateObjectInfo, ok := typedEvent.(*storagetypes.EventUpdateObjectInfo)
		if !ok {
			log.Errorw("type assert error", "type", "EventUpdateObjectInfo", "event", typedEvent)
			return errors.New("update object event assert error")
		}
		return m.handleUpdateObjectInfo(ctx, block, txHash, updateObjectInfo)
	case EventUpdateObjectContent:
		updateObjectContent, ok := typedEvent.(*storagetypes.EventUpdateObjectContent)
		if !ok {
			log.Errorw("type assert error", "type", "EventUpdateObjectContent", "event", typedEvent)
			return errors.New("update object content event assert error")
		}
		return m.handleUpdateObjectContent(ctx, block, txHash, updateObjectContent)
	case EventUpdateObjectContentSuccess:
		updateObjectContentSuccess, ok := typedEvent.(*storagetypes.EventUpdateObjectContentSuccess)
		if !ok {
			log.Errorw("type assert error", "type", "EventUpdateObjectContentSuccess", "event", typedEvent)
			return errors.New("update object content success event assert error")
		}
		return m.handleUpdateObjectContentSuccess(ctx, block, txHash, updateObjectContentSuccess)
	case EventCancelUpdateObjectContent:
		cancelUpdateObjectContent, ok := typedEvent.(*storagetypes.EventCancelUpdateObjectContent)
		if !ok {
			log.Errorw("type assert error", "type", "EventCancelUpdateObjectContent", "event", typedEvent)
			return errors.New("cancel update object content event assert error")
		}
		return m.handleCancelUpdateObjectContent(ctx, block, txHash, cancelUpdateObjectContent)
	case EventMirrorObject:
		mirrorObject, ok := typedEvent.(*storagetypes.EventMirrorObject)
		if !ok {
			log.Errorw("type assert error", "type", "EventMirrorObject", "event", typedEvent)
			return errors.New("mirror object event assert error")
		}
		return m.handleMirrorObject(ctx, block, txHash, mirrorObject)
	case EventMirrorObjectResult:
		mirrorObjectResult, ok := typedEvent.(*storagetypes.EventMirrorObjectResult)
		if !ok {
			log.Errorw("type assert error", "type", "EventMirrorObjectResult", "event", typedEvent)
			return errors.New("mirror object result event assert error")
		}
		return m.handleMirrorObjectResult(ctx, block, txHash, mirrorObjectResult)
	}

	return nil
}

func (m *Module) handleCreateObject(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, createObject *storagetypes.EventCreateObject) error {
	object := &models.Object{
		BucketID:       common.BigToHash(createObject.BucketId.BigInt()),
		BucketName:     createObject.BucketName,
		ObjectID:       common.BigToHash(createObject.ObjectId.BigInt()),
		ObjectName:     createObject.ObjectName,
		Creator:        common.HexToAddress(createObject.Creator),
		Owner:          common.HexToAddress(createObject.Owner),
		PayloadSize:    createObject.PayloadSize,
		Visibility:     createObject.Visibility.String(),
		ContentType:    createObject.ContentType,
		Status:         createObject.Status.String(),
		RedundancyType: createObject.RedundancyType.String(),
		SourceType:     createObject.SourceType.String(),
		CheckSums:      createObject.Checksums,

		CreateTxHash: txHash,
		CreateAt:     block.Block.Height,
		CreateTime:   createObject.CreateAt,
		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   createObject.CreateAt,
		Removed:      false,
	}

	return m.db.SaveObject(ctx, object)
}

func (m *Module) handleSealObject(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, sealObject *storagetypes.EventSealObject) error {
	object := &models.Object{
		BucketName:          sealObject.BucketName,
		ObjectName:          sealObject.ObjectName,
		ObjectID:            common.BigToHash(sealObject.ObjectId.BigInt()),
		Operator:            common.HexToAddress(sealObject.Operator),
		LocalVirtualGroupId: sealObject.LocalVirtualGroupId,
		Status:              sealObject.Status.String(),
		SealedTxHash:        txHash,

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
		Removed:      false,
	}

	return m.db.UpdateObject(ctx, object)
}

func (m *Module) handleCancelCreateObject(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, cancelCreateObject *storagetypes.EventCancelCreateObject) error {
	object := &models.Object{
		BucketName:   cancelCreateObject.BucketName,
		ObjectName:   cancelCreateObject.ObjectName,
		ObjectID:     common.BigToHash(cancelCreateObject.ObjectId.BigInt()),
		Operator:     common.HexToAddress(cancelCreateObject.Operator),
		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
		Removed:      true,
	}

	return m.db.UpdateObject(ctx, object)
}

func (m *Module) handleCopyObject(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, copyObject *storagetypes.EventCopyObject) error {
	destObject, err := m.db.GetObject(ctx, common.BigToHash(copyObject.SrcObjectId.BigInt()))
	if err != nil {
		return err
	}

	destObject.ObjectID = common.BigToHash(copyObject.DstObjectId.BigInt())
	destObject.ObjectName = copyObject.DstObjectName
	destObject.BucketName = copyObject.DstBucketName
	destObject.Operator = common.HexToAddress(copyObject.Operator)
	destObject.CreateAt = block.Block.Height
	destObject.CreateTxHash = txHash
	destObject.CreateTime = block.Block.Time.UTC().Unix()
	destObject.UpdateAt = block.Block.Height
	destObject.UpdateTxHash = txHash
	destObject.UpdateTime = block.Block.Time.UTC().Unix()
	destObject.Removed = false

	return m.db.UpdateObject(ctx, destObject)
}

func (m *Module) handleDeleteObject(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, deleteObject *storagetypes.EventDeleteObject) error {
	object := &models.Object{
		BucketName:          deleteObject.BucketName,
		ObjectName:          deleteObject.ObjectName,
		ObjectID:            common.BigToHash(deleteObject.ObjectId.BigInt()),
		LocalVirtualGroupId: deleteObject.LocalVirtualGroupId,

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
		Removed:      true,
	}

	return m.db.UpdateObject(ctx, object)
}

// RejectSeal event won't emit a delete event, need to be deleted manually here in metadata service
// handle logic is set as removed, no need to set status
func (m *Module) handleRejectSealObject(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, rejectSealObject *storagetypes.EventRejectSealObject) error {
	object := &models.Object{
		BucketName: rejectSealObject.BucketName,
		ObjectName: rejectSealObject.ObjectName,
		ObjectID:   common.BigToHash(rejectSealObject.ObjectId.BigInt()),
		Operator:   common.HexToAddress(rejectSealObject.Operator),

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
		Removed:      true,
	}

	return m.db.UpdateObject(ctx, object)
}

func (m *Module) handleEventDiscontinueObject(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, discontinueObject *storagetypes.EventDiscontinueObject) error {
	object := &models.Object{
		BucketName:   discontinueObject.BucketName,
		ObjectID:     common.BigToHash(discontinueObject.ObjectId.BigInt()),
		DeleteReason: discontinueObject.Reason,
		DeleteAt:     discontinueObject.DeleteAt,
		Status:       storagetypes.OBJECT_STATUS_DISCONTINUED.String(),

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
		Removed:      false,
	}

	return m.db.UpdateObject(ctx, object)
}

func (m *Module) handleUpdateObjectInfo(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, updateObject *storagetypes.EventUpdateObjectInfo) error {
	object := &models.Object{
		BucketName: updateObject.BucketName,
		ObjectID:   common.BigToHash(updateObject.ObjectId.BigInt()),
		ObjectName: updateObject.ObjectName,
		Operator:   common.HexToAddress(updateObject.Operator),
		Visibility: updateObject.Visibility.String(),

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateObject(ctx, object)
}

func (m *Module) handleUpdateObjectContent(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, updateObjectContent *storagetypes.EventUpdateObjectContent) error {
	if updateObjectContent.PayloadSize == 0 {
		// For 0-payload size (e.g. folder or empty file), chain updates metadata immediately.
		// We should reflect that: set Status=SEALED (keep as is), IsUpdating=false,
		// and update all metadata.
		// Note: ContentType is not available in EventUpdateObjectContent proto definition
		// The event only contains: operator, object_id, bucket_name, object_name, payload_size, checksums, version
		// So we preserve the existing ContentType value in the database
		object := &models.Object{
			BucketName: updateObjectContent.BucketName,
			ObjectName: updateObjectContent.ObjectName,
			ObjectID:   common.BigToHash(updateObjectContent.ObjectId.BigInt()),

			PayloadSize:        0, // Explicitly 0
			CheckSums:          updateObjectContent.Checksums,
			Version:            updateObjectContent.Version,
			Updater:            common.HexToAddress(updateObjectContent.Operator),
			ContentUpdatedTime: block.Block.Time.UTC().Unix(),

			Status:     storagetypes.OBJECT_STATUS_SEALED.String(),
			IsUpdating: false,

			UpdateAt:     block.Block.Height,
			UpdateTxHash: txHash,
			UpdateTime:   block.Block.Time.UTC().Unix(),
		}
		return m.db.UpdateObject(ctx, object)
	} else {
		// For normal update, only set IsUpdating=true and Operator.
		// Don't change Status to "UPDATING", keep it as is (likely SEALED).
		// Don't update metadata yet.
		object := &models.Object{
			BucketName: updateObjectContent.BucketName,
			ObjectName: updateObjectContent.ObjectName,
			ObjectID:   common.BigToHash(updateObjectContent.ObjectId.BigInt()),

			Updater:    common.HexToAddress(updateObjectContent.Operator),
			IsUpdating: true,
			// We pass Status=SEALED so that UpdateObject knows to update IsUpdating/Updater fields
			// (via the conditional logic we added in database.go)
			Status: storagetypes.OBJECT_STATUS_SEALED.String(),

			UpdateAt:     block.Block.Height,
			UpdateTxHash: txHash,
			UpdateTime:   block.Block.Time.UTC().Unix(),
		}
		return m.db.UpdateObject(ctx, object)
	}
}

func (m *Module) handleUpdateObjectContentSuccess(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, updateObjectContentSuccess *storagetypes.EventUpdateObjectContentSuccess) error {
	object := &models.Object{
		BucketName: updateObjectContentSuccess.BucketName,
		ObjectName: updateObjectContentSuccess.ObjectName,
		ObjectID:   common.BigToHash(updateObjectContentSuccess.ObjectId.BigInt()),

		Status:     storagetypes.OBJECT_STATUS_SEALED.String(),
		IsUpdating: false,

		// Reflect new metadata
		PayloadSize:        updateObjectContentSuccess.NewPayloadSize,
		CheckSums:          updateObjectContentSuccess.NewChecksums,
		ContentType:        updateObjectContentSuccess.ContentType,
		Version:            updateObjectContentSuccess.Version,
		Updater:            common.HexToAddress(updateObjectContentSuccess.Operator),
		ContentUpdatedTime: updateObjectContentSuccess.UpdatedAt,

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateObject(ctx, object)
}

func (m *Module) handleCancelUpdateObjectContent(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, cancelUpdateObjectContent *storagetypes.EventCancelUpdateObjectContent) error {
	object := &models.Object{
		BucketName: cancelUpdateObjectContent.BucketName,
		ObjectName: cancelUpdateObjectContent.ObjectName,
		ObjectID:   common.BigToHash(cancelUpdateObjectContent.ObjectId.BigInt()),

		Status:     storagetypes.OBJECT_STATUS_SEALED.String(),
		IsUpdating: false,
		Updater:    common.Address{}, // Clear updater (will be handled by conditional update)

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateObject(ctx, object)
}

func (m *Module) handleMirrorObject(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, mirrorObject *storagetypes.EventMirrorObject) error {
	// Don't use SaveObject, use UpdateObject to preserve existing data.
	// Only update mirror specific fields.
	// Note: EventMirrorObject does not contain SourceChainID in the proto definition.
	// SourceChainID could be set to the current chain's ID or left as zero.
	// The model includes this field for future extensibility.
	object := &models.Object{
		BucketName: mirrorObject.BucketName,
		ObjectName: mirrorObject.ObjectName,
		ObjectID:   common.BigToHash(mirrorObject.ObjectId.BigInt()),

		DestChainID:      mirrorObject.DestChainId,
		SourceChainID:    0, // Not available in EventMirrorObject, set to 0 for now
		MirrorStatus:     "pending",
		MirrorFailReason: "", // Clear any previous failure reason when starting new mirror

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateObject(ctx, object)
}

func (m *Module) handleMirrorObjectResult(ctx context.Context, block *tmctypes.ResultBlock, txHash common.Hash, mirrorObjectResult *storagetypes.EventMirrorObjectResult) error {
	// Map status code to string. Assuming 0 = success, non-zero = failed
	// The cross-chain status codes are defined in the storage types
	// Note: EventMirrorObjectResult does not contain detailed failure reason in the proto definition.
	// We provide a generic message based on the status code.
	mirrorStatus := "success"
	failReason := ""

	if mirrorObjectResult.Status != 0 {
		mirrorStatus = "failed"
		// Set a fixed message as requested by audit since event doesn't contain detailed reason
		failReason = "Mirror operation failed on destination chain"
	}

	object := &models.Object{
		BucketName: mirrorObjectResult.BucketName,
		ObjectName: mirrorObjectResult.ObjectName,
		ObjectID:   common.BigToHash(mirrorObjectResult.ObjectId.BigInt()),

		MirrorStatus:     mirrorStatus,
		MirrorFailReason: failReason,
		DestChainID:      mirrorObjectResult.DestChainId,

		UpdateAt:     block.Block.Height,
		UpdateTxHash: txHash,
		UpdateTime:   block.Block.Time.UTC().Unix(),
	}

	return m.db.UpdateObject(ctx, object)
}
