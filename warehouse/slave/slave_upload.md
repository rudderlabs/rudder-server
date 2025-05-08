# Moving Upload Job Execution from Router to Slave

## Proposed Change
Move the upload job state machine execution from the router (master) to the slave, where the router will only be responsible for job allocation and the slave will handle the actual job execution.

## Current Architecture
- Router is part of the warehouse manager
- Upload jobs are executed in router's background routines
- Router handles:
  - Backend config subscription
  - Upload job allocation
  - Worker management
  - Job creation and scheduling
  - Direct upload job execution via `uploadJob.run()`

## Implementation Steps

### Summary
1. **Slave Repository Setup**
   - Add database and repository dependencies to Slave struct
   - Initialize repositories in Slave's New function
   - Add repository fields to worker struct
   - Pass repositories to workers in SetupSlave

2. **Upload Job Processing**
   - Add processClaimedUploadSMJob method to worker
   - Handle upload ID from notification
   - Fetch upload details and staging files
   - Create and run upload job

3. **Router Changes**
   - Modify router to publish only upload ID
   - Remove direct upload job execution
   - Update job tracking logic

### 1. Update Slave Structure with Repository Access
```go
// warehouse/slave/slave.go
type Slave struct {
    // ... existing fields ...
    db              *sqlquerywrapper.DB
    uploadRepo      *repo.Uploads
    stagingRepo     *repo.StagingFiles
    tableUploadsRepo *repo.TableUploads
    loadFilesRepo   *repo.LoadFiles
    whSchemaRepo    *repo.WHSchema
}

func New(
    conf *config.Config,
    logger logger.Logger,
    stats stats.Stats,
    notifier slaveNotifier,
    bcManager *bcm.BackendConfigManager,
    constraintsManager *constraints.Manager,
    encodingFactory *encoding.Factory,
    db *sqlquerywrapper.DB,  // Add DB dependency
) *Slave {
    s := &Slave{}
    // ... existing initialization ...
    
    s.db = db
    s.uploadRepo = repo.NewUploads(db)
    s.stagingRepo = repo.NewStagingFiles(db)
    s.tableUploadsRepo = repo.NewTableUploads(db, conf)
    s.loadFilesRepo = repo.NewLoadFiles(db, conf)
    s.whSchemaRepo = repo.NewWHSchema(db)
    
    return s
}
```

#### Step 2: Update Worker Structure
```go
type worker struct {
    // ... existing fields ...
    uploadRepo      *repo.Uploads
    stagingRepo     *repo.StagingFiles
    tableUploadsRepo *repo.TableUploads
    loadFilesRepo   *repo.LoadFiles
    whSchemaRepo    *repo.WHSchema
}

func newWorker(
    conf *config.Config,
    logger logger.Logger,
    statsFactory stats.Stats,
    notifier slaveNotifier,
    bcManager *bcm.BackendConfigManager,
    constraintsManager *constraints.Manager,
    encodingFactory *encoding.Factory,
    uploadRepo *repo.Uploads,
    stagingRepo *repo.StagingFiles,
    tableUploadsRepo *repo.TableUploads,
    loadFilesRepo *repo.LoadFiles,
    whSchemaRepo *repo.WHSchema,
    workerIdx int,
) *worker {
    s := &worker{}
    // ... existing initialization ...
    
    s.uploadRepo = uploadRepo
    s.stagingRepo = stagingRepo
    s.tableUploadsRepo = tableUploadsRepo
    s.loadFilesRepo = loadFilesRepo
    s.whSchemaRepo = whSchemaRepo
    
    return s
}
```

#### Step 3: Pass Repositories to Workers
```go
func (s *Slave) SetupSlave(ctx context.Context) error {
    // ... existing code ...
    
    // Initialize repositories
    uploadRepo := repo.NewUploads(s.db)
    stagingRepo := repo.NewStagingFiles(s.db)
    tableUploadsRepo := repo.NewTableUploads(s.db, s.conf)
    loadFilesRepo := repo.NewLoadFiles(s.db, s.conf)
    whSchemaRepo := repo.NewWHSchema(s.db)
    
    for workerIdx := 0; workerIdx <= s.config.noOfSlaveWorkerRoutines.Load()-1; workerIdx++ {
        idx := workerIdx
        
        g.Go(crash.NotifyWarehouse(func() error {
            slaveWorker := newWorker(
                s.conf, 
                s.log, 
                s.stats, 
                s.notifier, 
                s.bcManager, 
                s.constraintsManager, 
                s.encodingFactory,
                uploadRepo,  // Pass initialized repos
                stagingRepo,
                tableUploadsRepo,
                loadFilesRepo,
                whSchemaRepo,
                idx,
            )
            slaveWorker.start(gCtx, jobNotificationChannel, slaveID)
            return nil
        }))
    }
    // ... rest of the code ...
}
```

#### Step 4: Add Upload Job Processing to Worker
```go
// warehouse/slave/worker.go

// New method to handle JobTypeUploadSM jobs
func (w *worker) processClaimedUploadSMJob(ctx context.Context, claimedJob *notifier.ClaimJob) {
    w.stats.workerClaimProcessingTime.RecordDuration()()

    handleErr := func(err error, claimedJob *notifier.ClaimJob) {
        w.stats.workerClaimProcessingFailed.Increment()
        w.notifier.UpdateClaim(ctx, claimedJob, &notifier.ClaimJobResponse{
            Err: err,
        })
    }

    var message struct {
        UploadID int64 `json:"upload_id"`
    }
    if err := jsonrs.Unmarshal(claimedJob.Job.Payload, &message); err != nil {
        handleErr(err, claimedJob)
        return
    }

    // Get upload details from database
    upload, err := w.uploadRepo.Get(ctx, message.UploadID)
    if err != nil {
        handleErr(err, claimedJob)
        return
    }

    // Get staging files for the upload
    stagingFiles, err := w.stagingRepo.GetForUploadID(ctx, message.UploadID)
    if err != nil {
        handleErr(err, claimedJob)
        return
    }

    // Create and run the upload job
    job := w.uploadJobFactory.NewUploadJob(ctx, &model.UploadJob{
        Upload:       upload,
        StagingFiles: stagingFiles,
    }, w.whManager)
    if err := job.run(); err != nil {
        handleErr(err, claimedJob)
        return
    }

    w.stats.workerClaimProcessingSucceeded.Increment()
    w.notifier.UpdateClaim(ctx, claimedJob, &notifier.ClaimJobResponse{
        Payload: claimedJob.Job.Payload,
    })
}
```

#### Step 5: Modify Router
```go
// warehouse/router/router.go
type Router struct {
    // ... existing fields ...
    notifier *notifier.Notifier
}

func (r *Router) runUploadJobAllocator(ctx context.Context) error {
    // ... existing code ...
    for _, uploadJob := range uploadJobsToProcess {
        // Only publish the upload ID
        message := struct {
            UploadID int64 `json:"upload_id"`
        }{
            UploadID: uploadJob.upload.ID,
        }
        
        payload, err := jsonrs.Marshal(message)
        if err != nil {
            r.logger.Errorf("[WH] Failed to marshal message for worker: %+v", err)
            continue
        }

        _, err = r.notifier.Publish(ctx, &notifier.PublishRequest{
            Payloads: []stdjson.RawMessage{
                payload,
            },
            JobType: notifier.JobTypeUploadSM,
        })
        if err != nil {
            r.logger.Errorf("[WH] Failed to publish message for worker: %+v", err)
            continue
        }
        r.setDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
    }
    // ... rest of the code ...
}
```

This change ensures that:
1. Only the upload ID is published to the notification system
2. The slave will be responsible for:
   - Fetching upload details using the upload ID
   - Getting associated staging files
   - Creating and managing the upload job
   - Handling all upload job execution logic
3. Reduces message payload size
4. Follows the principle of minimal data transfer
5. Makes the system more maintainable as all upload job logic is centralized in the slave

### 5. Repository Access Changes

To enable the slave to handle upload jobs, we need to add repository access. This involves:

#### Step 1: Add Repository Dependencies
```go
// warehouse/slave/slave.go
type Slave struct {
    // ... existing fields ...
    db              *sqlquerywrapper.DB
    uploadRepo      *repo.Uploads
    stagingRepo     *repo.StagingFiles
    tableUploadsRepo *repo.TableUploads
    loadFilesRepo   *repo.LoadFiles
    whSchemaRepo    *repo.WHSchema
}
```

#### Step 2: Initialize Repositories
```go
func New(
    conf *config.Config,
    logger logger.Logger,
    stats stats.Stats,
    notifier slaveNotifier,
    bcManager *bcm.BackendConfigManager,
    constraintsManager *constraints.Manager,
    encodingFactory *encoding.Factory,
    db *sqlquerywrapper.DB,  // Add DB dependency
) *Slave {
    s := &Slave{}
    // ... existing initialization ...
    
    s.db = db
    s.uploadRepo = repo.NewUploads(db)
    s.stagingRepo = repo.NewStagingFiles(db)
    s.tableUploadsRepo = repo.NewTableUploads(db, conf)
    s.loadFilesRepo = repo.NewLoadFiles(db, conf)
    s.whSchemaRepo = repo.NewWHSchema(db)
    
    return s
}
```

#### Step 3: Pass Repositories to Workers
```go
func (s *Slave) SetupSlave(ctx context.Context) error {
    // ... existing code ...
    
    // Initialize repositories
    uploadRepo := repo.NewUploads(s.db)
    stagingRepo := repo.NewStagingFiles(s.db)
    tableUploadsRepo := repo.NewTableUploads(s.db, s.conf)
    loadFilesRepo := repo.NewLoadFiles(s.db, s.conf)
    whSchemaRepo := repo.NewWHSchema(s.db)
    
    for workerIdx := 0; workerIdx <= s.config.noOfSlaveWorkerRoutines.Load()-1; workerIdx++ {
        idx := workerIdx
        
        g.Go(crash.NotifyWarehouse(func() error {
            slaveWorker := newWorker(
                s.conf, 
                s.log, 
                s.stats, 
                s.notifier, 
                s.bcManager, 
                s.constraintsManager, 
                s.encodingFactory,
                uploadRepo,  // Pass initialized repos
                stagingRepo,
                tableUploadsRepo,
                loadFilesRepo,
                whSchemaRepo,
                idx,
            )
            slaveWorker.start(gCtx, jobNotificationChannel, slaveID)
            return nil
        }))
    }
    // ... rest of the code ...
}
```

#### Step 4: Update Worker Structure
```go
type worker struct {
    // ... existing fields ...
    uploadRepo      *repo.Uploads
    stagingRepo     *repo.StagingFiles
    tableUploadsRepo *repo.TableUploads
    loadFilesRepo   *repo.LoadFiles
    whSchemaRepo    *repo.WHSchema
}

func newWorker(
    conf *config.Config,
    logger logger.Logger,
    statsFactory stats.Stats,
    notifier slaveNotifier,
    bcManager *bcm.BackendConfigManager,
    constraintsManager *constraints.Manager,
    encodingFactory *encoding.Factory,
    uploadRepo *repo.Uploads,
    stagingRepo *repo.StagingFiles,
    tableUploadsRepo *repo.TableUploads,
    loadFilesRepo *repo.LoadFiles,
    whSchemaRepo *repo.WHSchema,
    workerIdx int,
) *worker {
    s := &worker{}
    // ... existing initialization ...
    
    s.uploadRepo = uploadRepo
    s.stagingRepo = stagingRepo
    s.tableUploadsRepo = tableUploadsRepo
    s.loadFilesRepo = loadFilesRepo
    s.whSchemaRepo = whSchemaRepo
    
    return s
}
```

These changes will enable the slave to:
1. Access upload details using the upload ID
2. Get staging files associated with the upload
3. Create and manage table uploads
4. Handle load files
5. Manage warehouse schemas

### 6. Configuration Changes
- Move upload job related configs from router to slave
- Add new configs for slave upload job handling
- Update config loading mechanism
- Configure notification topics and channels
- Add notification retry and backoff settings
- Add job type configuration
- Add new job type constant to notifier package
- Add upload job factory configuration

### 7. Testing Strategy
1. Unit Tests
   - Test upload job handler in isolation
   - Test notification publishing
   - Test notification subscription
   - Test job lifecycle events
   - Test job type-based routing
   - Test new JobTypeUploadSM handling
   - Test upload job creation and execution

2. Integration Tests
   - Test end-to-end upload flow
   - Test error handling and recovery
   - Test concurrent job execution
   - Test notification delivery and processing
   - Test different job types
   - Test JobTypeUploadSM specific scenarios
   - Test upload job creation and execution

3. Performance Tests
   - Test job execution performance
   - Test resource utilization
   - Test scalability
   - Test notification system performance
   - Test job type handling performance
   - Test JobTypeUploadSM performance
   - Test upload job creation performance

### 8. Migration Plan
1. Phase 1: Implementation
   - Add new JobTypeUploadSM constant
   - Implement new upload job handler
   - Add slave upload job handling
   - Update router to publish jobs with new type
   - Implement notification subscription
   - Implement job type handling
   - Implement upload job factory

2. Phase 2: Testing
   - Run unit tests
   - Run integration tests
   - Run performance tests
   - Test notification flow
   - Test job type handling
   - Test JobTypeUploadSM specific cases
   - Test upload job creation and execution

3. Phase 3: Deployment
   - Deploy changes in staging
   - Monitor performance
   - Monitor notification flow
   - Monitor job type handling
   - Monitor JobTypeUploadSM metrics
   - Monitor upload job execution
   - Roll out to production

### 9. Rollback Plan
- Keep old router implementation
- Add feature flag for new implementation
- Monitor metrics for issues
- Monitor notification flow
- Monitor job type handling
- Monitor JobTypeUploadSM metrics
- Monitor upload job execution
- Prepare rollback procedure

## Benefits
1. Better separation of concerns
2. Improved scalability
3. More efficient resource utilization
4. Better error isolation
5. Easier maintenance
6. Decoupled job execution from job allocation
7. Flexible job type handling
8. Clear distinction between slave upload jobs and other job types
9. Centralized upload job creation and execution

## Risks and Mitigations
1. Risk: Job execution delays
   - Mitigation: Monitor job execution times
   - Add performance metrics
   - Implement notification retry mechanism

2. Risk: Resource contention
   - Mitigation: Implement proper resource limits
   - Monitor resource usage
   - Implement backoff strategies

3. Risk: Data consistency
   - Mitigation: Implement proper error handling
   - Add data validation checks
   - Implement idempotency

4. Risk: Notification failures
   - Mitigation: Implement retry mechanism
   - Add notification delivery monitoring
   - Implement fallback notification channels
   - Add dead letter queues

5. Risk: Job type handling errors
   - Mitigation: Add job type validation
   - Implement fallback handlers
   - Add job type monitoring
   - Add specific monitoring for JobTypeUploadSM

6. Risk: Upload job creation failures
   - Mitigation: Add upload job factory error handling
   - Monitor upload job creation metrics
   - Implement retry mechanism for job creation

## Success Metrics
1. Job execution time
2. Resource utilization
3. Error rates
4. System stability
5. Scalability metrics
6. Notification delivery success rate
7. Notification latency
8. Job processing latency
9. Job type handling success rate
10. JobTypeUploadSM specific metrics
11. Upload job creation success rate
12. Upload job execution success rate

## Alternative Implementation: Without DB in Slave

### Overview
Instead of giving the slave access to repositories, we can serialize the complete upload job data and pass it through the notification system. This approach:
1. Keeps the slave stateless
2. Avoids DB access in slave
3. Simplifies the architecture
4. Makes the system more distributed

### Implementation Steps

#### Step 1: Define Upload Job Payload
```go
// warehouse/slave/upload_job.go
type UploadJobPayload struct {
    Upload       model.Upload       `json:"upload"`
    Warehouse    model.Warehouse    `json:"warehouse"`
    StagingFiles []model.StagingFile `json:"staging_files"`
}

func (p *UploadJobPayload) UnmarshalJSON(data []byte) error {
    type Alias UploadJobPayload
    aux := &struct {
        *Alias
        Upload       json.RawMessage `json:"upload"`
        Warehouse    json.RawMessage `json:"warehouse"`
        StagingFiles json.RawMessage `json:"staging_files"`
    }{
        Alias: (*Alias)(p),
    }
    if err := json.Unmarshal(data, &aux); err != nil {
        return err
    }
    // Custom unmarshaling if needed
    return nil
}
```

#### Step 2: Update Router to Publish Complete Job Data
```go
// warehouse/router/router.go
func (r *Router) runUploadJobAllocator(ctx context.Context) error {
    // ... existing code ...
    for _, uploadJob := range uploadJobsToProcess {
        payload := UploadJobPayload{
            Upload:       uploadJob.upload,
            Warehouse:    uploadJob.warehouse,
            StagingFiles: uploadJob.stagingFiles,
        }
        
        payloadBytes, err := json.Marshal(payload)
        if err != nil {
            r.logger.Errorf("[WH] Failed to marshal upload job payload: %+v", err)
            continue
        }

        _, err = r.notifier.Publish(ctx, &notifier.PublishRequest{
            Payloads: []stdjson.RawMessage{
                payloadBytes,
            },
            JobType: notifier.JobTypeUploadSM,
        })
        if err != nil {
            r.logger.Errorf("[WH] Failed to publish upload job: %+v", err)
            continue
        }
        r.setDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
    }
    // ... rest of the code ...
}
```

#### Step 3: Update Worker to Handle Upload Job Payload
```go
// warehouse/slave/worker.go
func (w *worker) processClaimedUploadSMJob(ctx context.Context, claimedJob *notifier.ClaimJob) {
    w.stats.workerClaimProcessingTime.RecordDuration()()

    handleErr := func(err error, claimedJob *notifier.ClaimJob) {
        w.stats.workerClaimProcessingFailed.Increment()
        w.notifier.UpdateClaim(ctx, claimedJob, &notifier.ClaimJobResponse{
            Err: err,
        })
    }

    var payload UploadJobPayload
    if err := json.Unmarshal(claimedJob.Job.Payload, &payload); err != nil {
        handleErr(err, claimedJob)
        return
    }

    // Create and run the upload job
    job := w.uploadJobFactory.NewUploadJob(ctx, &model.UploadJob{
        Upload:       payload.Upload,
        Warehouse:    payload.Warehouse,
        StagingFiles: payload.StagingFiles,
    }, w.whManager)
    
    if err := job.run(); err != nil {
        handleErr(err, claimedJob)
        return
    }

    w.stats.workerClaimProcessingSucceeded.Increment()
    w.notifier.UpdateClaim(ctx, claimedJob, &notifier.ClaimJobResponse{
        Payload: claimedJob.Job.Payload,
    })
}
```

### Benefits of This Approach
1. **Stateless Slave**: The slave doesn't need to maintain any state or DB connections
2. **Simpler Architecture**: No need to manage repository access and DB connections
3. **Better Distribution**: Each job is self-contained with all necessary data
4. **Easier Scaling**: Can add more slaves without worrying about DB connection limits
5. **Reduced Complexity**: No need to handle DB connection pooling or repository initialization
6. **Better Isolation**: Each job runs with its own complete data set
7. **Easier Testing**: Can test job processing without DB dependencies

### Considerations
1. **Payload Size**: The notification payload will be larger since it includes all job data
2. **Data Freshness**: The data is as fresh as when it was published
3. **Memory Usage**: Workers need to hold complete job data in memory
4. **Error Handling**: Need to handle serialization/deserialization errors
5. **Job Updates**: Any updates to the job need to be handled through the notification system

### Migration Strategy
1. **Phase 1: Implementation**
   - Add UploadJobPayload struct
   - Update router to publish complete job data
   - Update worker to handle job payload
   - Add payload validation
   - Add error handling for serialization

2. **Phase 2: Testing**
   - Test payload serialization/deserialization
   - Test job processing with complete data
   - Test error handling
   - Test performance with larger payloads
   - Test notification system capacity

3. **Phase 3: Deployment**
   - Deploy with feature flag
   - Monitor payload sizes
   - Monitor memory usage
   - Monitor job processing times
   - Monitor notification system performance

### Success Metrics
1. Job processing time
2. Memory usage per job
3. Notification payload size
4. Serialization/deserialization time
5. Error rates
6. System stability
7. Resource utilization 