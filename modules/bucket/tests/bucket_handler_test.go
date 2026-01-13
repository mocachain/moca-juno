package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/forbole/juno/v4/database"
	"github.com/forbole/juno/v4/models"

	// Mock dependencies
	"github.com/forbole/juno/v4/common"
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

	// Auto-migrate the Bucket model
	err = db.AutoMigrate(&models.Bucket{})
	if err != nil {
		return nil, err
	}

	return &MockDBImpl{
		Impl: &database.Impl{
			Db: db,
		},
	}, nil
}

type BucketHandlerTestSuite struct {
	suite.Suite
	db  *MockDBImpl
	ctx context.Context
}

func TestBucketHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(BucketHandlerTestSuite))
}

func (s *BucketHandlerTestSuite) SetupTest() {
	mockDB, err := NewMockDB()
	s.Require().NoError(err)
	s.db = mockDB
	s.ctx = context.Background()
}

// TestMigrationComplete_ClearsFields verifies that completing migration clears start time and dest SP
func (s *BucketHandlerTestSuite) TestMigrationComplete_ClearsFields() {
	// 1. Setup: Create a bucket in MIGRATING state with migration fields set
	startTime := time.Now().Unix()
	initialBucket := &models.Bucket{
		BucketID:           common.HexToHash("0x1234"),
		BucketName:         "test-bucket",
		Status:             "BUCKET_STATUS_MIGRATING",
		MigrationStartTime: &startTime,
		DestPrimarySPID:    "100",
	}
	err := s.db.SaveBucket(s.ctx, initialBucket)
	s.Require().NoError(err)

	// 2. Action: Simulate handleCompleteMigrationBucket logic
	// Since we can't easily invoke the full module handler with complex event types without mocking everything,
	// we'll test the UpdateBucket logic directly which is the core fix.
	// The handler constructs a bucket with:
	// - Status: CREATED
	// - MigrationCompleteTime: set
	// - MigrationStartTime: nil
	// - DestPrimarySPID: ""
	// - MigrationRejectReason: ""

	completeTime := time.Now().Unix()
	updateBucket := &models.Bucket{
		BucketID:              common.HexToHash("0x1234"),
		BucketName:            "test-bucket",
		Status:                "BUCKET_STATUS_CREATED",
		MigrationCompleteTime: &completeTime,
		MigrationStartTime:    nil, // Should be cleared
		DestPrimarySPID:       "",  // Should be cleared
		MigrationRejectReason: "",  // Should be cleared
		UpdateAt:              100,
	}

	err = s.db.UpdateBucket(s.ctx, updateBucket)
	s.Require().NoError(err)

	// 3. Verify: Check DB state
	var storedBucket models.Bucket
	err = s.db.Impl.Db.Where("bucket_id = ?", common.HexToHash("0x1234")).First(&storedBucket).Error
	s.Require().NoError(err)

	s.Equal("BUCKET_STATUS_CREATED", storedBucket.Status)
	s.NotNil(storedBucket.MigrationCompleteTime)
	s.Equal(completeTime, *storedBucket.MigrationCompleteTime)

	// Key verification: These should be cleared (nil/empty)
	s.Nil(storedBucket.MigrationStartTime, "MigrationStartTime should be nil")
	s.Equal("", storedBucket.DestPrimarySPID, "DestPrimarySPID should be empty")
	s.Equal("", storedBucket.MigrationRejectReason, "MigrationRejectReason should be empty")
}

// TestUpdateBucketInfo_PreservesMigrationFields verifies that updating info doesn't clear migration fields
func (s *BucketHandlerTestSuite) TestUpdateBucketInfo_PreservesMigrationFields() {
	// 1. Setup: Create a bucket in MIGRATING state
	startTime := time.Now().Unix()
	initialBucket := &models.Bucket{
		BucketID:           common.HexToHash("0x5678"),
		BucketName:         "migrating-bucket",
		Status:             "BUCKET_STATUS_MIGRATING",
		MigrationStartTime: &startTime,
		DestPrimarySPID:    "200",
		ChargedReadQuota:   1000,
	}
	err := s.db.SaveBucket(s.ctx, initialBucket)
	s.Require().NoError(err)

	// 2. Action: Simulate handleUpdateBucketInfo
	// Handler update only non-migration fields, Status is NOT set (empty string)
	updateBucket := &models.Bucket{
		BucketID:         common.HexToHash("0x5678"),
		BucketName:       "migrating-bucket",
		ChargedReadQuota: 2000, // Changed
		// Status is intentionally empty ("")
		// Migration fields are nil/empty in the struct
	}

	err = s.db.UpdateBucket(s.ctx, updateBucket)
	s.Require().NoError(err)

	// 3. Verify: Migration fields should remain UNCHANGED
	var storedBucket models.Bucket
	err = s.db.Impl.Db.Where("bucket_id = ?", common.HexToHash("0x5678")).First(&storedBucket).Error
	s.Require().NoError(err)

	s.Equal("BUCKET_STATUS_MIGRATING", storedBucket.Status)
	s.Equal(uint64(2000), storedBucket.ChargedReadQuota)

	// Key verification: Migration data preserved
	s.NotNil(storedBucket.MigrationStartTime)
	if storedBucket.MigrationStartTime != nil {
		s.Equal(startTime, *storedBucket.MigrationStartTime)
	}
	s.Equal("200", storedBucket.DestPrimarySPID)
}

// TestCompleteMigrationFlow verifies the full state transition sequence
func (s *BucketHandlerTestSuite) TestCompleteMigrationFlow() {
	bucketID := common.HexToHash("0x9999")

	// 1. Create Bucket (CREATED)
	createBucket := &models.Bucket{
		BucketID:   bucketID,
		BucketName: "flow-bucket",
		Status:     "BUCKET_STATUS_CREATED",
	}
	s.Require().NoError(s.db.SaveBucket(s.ctx, createBucket))

	// 2. Start Migration (MIGRATING)
	startTime := time.Now().Unix()
	startMigration := &models.Bucket{
		BucketID:           bucketID,
		Status:             "BUCKET_STATUS_MIGRATING",
		MigrationStartTime: &startTime,
		DestPrimarySPID:    "300",
	}
	s.Require().NoError(s.db.UpdateBucket(s.ctx, startMigration))

	// Verify MIGRATING state
	var step1Bucket models.Bucket
	s.db.Impl.Db.Where("bucket_id = ?", bucketID).First(&step1Bucket)
	s.Equal("BUCKET_STATUS_MIGRATING", step1Bucket.Status)
	s.NotNil(step1Bucket.MigrationStartTime)
	s.Equal("300", step1Bucket.DestPrimarySPID)

	// 3. Complete Migration (CREATED)
	completeTime := time.Now().Unix() + 100
	completeMigration := &models.Bucket{
		BucketID:              bucketID,
		Status:                "BUCKET_STATUS_CREATED",
		MigrationCompleteTime: &completeTime,
		MigrationStartTime:    nil,
		DestPrimarySPID:       "",
	}
	s.Require().NoError(s.db.UpdateBucket(s.ctx, completeMigration))

	// Verify Final State
	var finalBucket models.Bucket
	s.db.Impl.Db.Where("bucket_id = ?", bucketID).First(&finalBucket)
	s.Equal("BUCKET_STATUS_CREATED", finalBucket.Status)
	s.NotNil(finalBucket.MigrationCompleteTime)
	s.Nil(finalBucket.MigrationStartTime, "Start time cleared")
	s.Equal("", finalBucket.DestPrimarySPID, "Dest SP cleared")
}
