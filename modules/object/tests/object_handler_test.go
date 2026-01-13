package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/forbole/juno/v4/common"
	"github.com/forbole/juno/v4/database"
	"github.com/forbole/juno/v4/models"
)

// MockDBImpl is a wrapper around database.Impl to expose the DB for testing
type MockDBImpl struct {
	*database.Impl
}

func NewMockDB() (*MockDBImpl, error) {
	// Use in-memory SQLite for testing
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Auto-migrate the Object model
	err = db.AutoMigrate(&models.Object{})
	if err != nil {
		return nil, err
	}

	return &MockDBImpl{
		Impl: &database.Impl{
			Db: db,
		},
	}, nil
}

type ObjectHandlerTestSuite struct {
	suite.Suite
	db  *MockDBImpl
	ctx context.Context
}

func TestObjectHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(ObjectHandlerTestSuite))
}

func (s *ObjectHandlerTestSuite) SetupTest() {
	mockDB, err := NewMockDB()
	s.Require().NoError(err)
	s.db = mockDB
	s.ctx = context.Background()
}

// TestUpdateObjectContentSuccess_ClearsIsUpdating verifies that IsUpdating=false is correctly written
// which was the core GORM zero-value issue.
func (s *ObjectHandlerTestSuite) TestUpdateObjectContentSuccess_ClearsIsUpdating() {
	objectID := common.HexToHash("0x1001")
	
	// 1. Setup: Object in UPDATING state
	initialObj := &models.Object{
		ObjectID:   objectID,
		BucketName: "test-bucket",
		ObjectName: "test-object",
		Status:     "OBJECT_STATUS_SEALED",
		IsUpdating: true, // Updating
		Updater:    common.HexToAddress("0xABC"),
		PayloadSize: 100,
	}
	s.Require().NoError(s.db.SaveObject(s.ctx, initialObj))

	// 2. Action: Simulate Success Event (UpdateObject with Status set and IsUpdating=false)
	updateObj := &models.Object{
		ObjectID:           objectID,
		Status:             "OBJECT_STATUS_SEALED",
		IsUpdating:         false, // Should be written
		Updater:            common.HexToAddress("0xDEF"), // New updater
		PayloadSize:        200, // New size
		ContentUpdatedTime: 123456,
	}
	s.Require().NoError(s.db.UpdateObject(s.ctx, updateObj))

	// 3. Verify
	var storedObj models.Object
	s.Require().NoError(s.db.Impl.Db.Where("object_id = ?", objectID).First(&storedObj).Error)

	s.False(storedObj.IsUpdating, "IsUpdating should be false")
	s.Equal("OBJECT_STATUS_SEALED", storedObj.Status)
	s.Equal(uint64(200), storedObj.PayloadSize)
	s.Equal(common.HexToAddress("0xDEF"), storedObj.Updater)
}

// TestUpdateObjectContent_ZeroPayload verifies special handling for 0-payload updates
func (s *ObjectHandlerTestSuite) TestUpdateObjectContent_ZeroPayload() {
	objectID := common.HexToHash("0x1002")

	// 1. Setup
	initialObj := &models.Object{
		ObjectID:   objectID,
		Status:     "OBJECT_STATUS_SEALED",
		IsUpdating: false,
		PayloadSize: 50,
	}
	s.Require().NoError(s.db.SaveObject(s.ctx, initialObj))

	// 2. Action: Simulate 0-payload update (Immediate update)
	// In handler logic, this sets Status=SEALED, IsUpdating=false, PayloadSize=0
	// And it sets ContentUpdatedTime != 0 to trigger update
	updateObj := &models.Object{
		ObjectID:           objectID,
		Status:             "OBJECT_STATUS_SEALED",
		IsUpdating:         false,
		PayloadSize:        0, // Zero value
		Updater:            common.HexToAddress("0x111"),
		ContentUpdatedTime: 9999,
	}
	s.Require().NoError(s.db.UpdateObject(s.ctx, updateObj))

	// 3. Verify
	var storedObj models.Object
	s.Require().NoError(s.db.Impl.Db.Where("object_id = ?", objectID).First(&storedObj).Error)

	s.Equal(uint64(0), storedObj.PayloadSize, "PayloadSize should be 0")
	s.False(storedObj.IsUpdating)
	s.Equal(common.HexToAddress("0x111"), storedObj.Updater)
}

// TestCancelUpdateObject_ClearsUpdater verifies Cancel event clears updater
func (s *ObjectHandlerTestSuite) TestCancelUpdateObject_ClearsUpdater() {
	objectID := common.HexToHash("0x1003")

	// 1. Setup: Updating state
	initialObj := &models.Object{
		ObjectID:   objectID,
		Status:     "OBJECT_STATUS_SEALED",
		IsUpdating: true,
		Updater:    common.HexToAddress("0xBAD"),
	}
	s.Require().NoError(s.db.SaveObject(s.ctx, initialObj))

	// 2. Action: Cancel update (Status=SEALED, IsUpdating=false, Updater=Zero)
	updateObj := &models.Object{
		ObjectID:   objectID,
		Status:     "OBJECT_STATUS_SEALED",
		IsUpdating: false,
		Updater:    common.Address{}, // Zero value
	}
	s.Require().NoError(s.db.UpdateObject(s.ctx, updateObj))

	// 3. Verify
	var storedObj models.Object
	s.Require().NoError(s.db.Impl.Db.Where("object_id = ?", objectID).First(&storedObj).Error)

	s.False(storedObj.IsUpdating)
	s.Equal(common.Address{}, storedObj.Updater, "Updater should be cleared")
}

// TestMirrorObject_UpdateFields verifies Mirror update logic
func (s *ObjectHandlerTestSuite) TestMirrorObject_UpdateFields() {
	objectID := common.HexToHash("0x1004")

	// 1. Setup: Existing object with metadata
	initialObj := &models.Object{
		ObjectID:     objectID,
		BucketName:   "mirror-bucket",
		Creator:      common.HexToAddress("0xCREATOR"),
		Status:       "OBJECT_STATUS_SEALED",
		IsUpdating:   true, // Should not be cleared by mirror update unless specified
		MirrorStatus: "",
	}
	s.Require().NoError(s.db.SaveObject(s.ctx, initialObj))

	// 2. Action: Mirror Object Event (Status pending)
	// Only mirror fields should update
	mirrorObj := &models.Object{
		ObjectID:     objectID,
		MirrorStatus: "pending",
		DestChainID:  100,
	}
	s.Require().NoError(s.db.UpdateObject(s.ctx, mirrorObj))

	// 3. Verify
	var storedObj models.Object
	s.Require().NoError(s.db.Impl.Db.Where("object_id = ?", objectID).First(&storedObj).Error)

	s.Equal("pending", storedObj.MirrorStatus)
	s.Equal(uint32(100), storedObj.DestChainID)
	s.Equal(common.HexToAddress("0xCREATOR"), storedObj.Creator, "Creator should persist")
	s.True(storedObj.IsUpdating, "IsUpdating should persist as it was not touched by mirror update")

	// 4. Action: Mirror Result (Success)
	resultObj := &models.Object{
		ObjectID:     objectID,
		MirrorStatus: "success",
		// MirrorFailReason empty
	}
	s.Require().NoError(s.db.UpdateObject(s.ctx, resultObj))

	// 5. Verify
	s.Require().NoError(s.db.Impl.Db.Where("object_id = ?", objectID).First(&storedObj).Error)
	s.Equal("success", storedObj.MirrorStatus)
}

// TestNormalUpdate_PreservesIsUpdating verifies that non-content updates don't touch IsUpdating
func (s *ObjectHandlerTestSuite) TestNormalUpdate_PreservesIsUpdating() {
	objectID := common.HexToHash("0x1005")

	// 1. Setup: Updating state
	initialObj := &models.Object{
		ObjectID:   objectID,
		Status:     "OBJECT_STATUS_SEALED",
		IsUpdating: true,
		Visibility: "VISIBILITY_PRIVATE",
	}
	s.Require().NoError(s.db.SaveObject(s.ctx, initialObj))

	// 2. Action: Update Object Info (e.g. Visibility)
	// Status is empty string, IsUpdating is false (default in struct)
	updateObj := &models.Object{
		ObjectID:   objectID,
		Visibility: "VISIBILITY_PUBLIC",
	}
	s.Require().NoError(s.db.UpdateObject(s.ctx, updateObj))

	// 3. Verify
	var storedObj models.Object
	s.Require().NoError(s.db.Impl.Db.Where("object_id = ?", objectID).First(&storedObj).Error)

	s.Equal("VISIBILITY_PUBLIC", storedObj.Visibility)
	s.True(storedObj.IsUpdating, "IsUpdating should persist because Status was empty in update")
}
