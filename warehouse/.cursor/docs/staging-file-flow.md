# Staging File Flow and Field Propagation

This document explains how staging files flow through the RudderStack warehouse service and how to add new fields to staging file payloads.

## Flow Architecture

The staging file flow follows a clear data pipeline across warehouse system components:

```
[Batch Router] → [Warehouse Client] → [Warehouse API] → [Model/Repo] → [Database]
               ↳ POST /v1/process ↗
```

Each component has distinct responsibilities and transformation points that must be considered when adding new fields.

## Component Responsibilities

### Batch Router: Origin and Data Generation
**Responsibility:** Creates staging file payload after uploading data to object storage

**Key Points:**
- All new fields must be populated here from available data sources
- Values originate from job parameters, environment state, or system context
- This is the single source of truth for field values
- Calls warehouse API endpoint `POST /v1/process` to notify about new staging files

### Warehouse Client: Transport and Conversion  
**Responsibility:** Converts staging files to HTTP payload format for inter-service communication

**Key Points:**
- Handles structural transformation between internal and transport formats
- Passes fields through without modification or value generation
- Requires mapping between input and payload structures

### Warehouse API: Request Processing
**Responsibility:** Receives HTTP requests and maps them to internal data models

**Key Points:**
- Unmarshals transport payloads into internal representations
- Validates incoming data structure and format
- Bridges external interface with internal architecture

### Model/Repo: Data Structure and Persistence
**Responsibility:** Defines canonical staging file structure and manages database operations

**Key Points:**
- Model establishes the authoritative data schema and handles type safety
- Repository persists field metadata through JSON serialization in metadata column
- Handles schema evolution, backward compatibility, and data retrieval operations

## Implementation Checklist

When adding new fields to staging file payloads, ensure coverage across all warehouse components:

### Warehouse Client Updates
- [ ] Add field to staging file input structure
- [ ] Add field to HTTP payload structure
- [ ] Implement field mapping from input to payload

### Warehouse API Updates
- [ ] Add field to incoming request schema
- [ ] Update mapping functions to internal model
- [ ] Validate field format and constraints

### Model/Repository Updates
- [ ] Add field to model structure
- [ ] Define field in metadata schema with JSON tags
- [ ] Update metadata conversion functions for persistence

### Batch Router Integration
- [ ] Set field value in batch router's warehouse notification
- [ ] Verify field population across all staging file creation points
- [ ] Use appropriate data sources for field value generation

### Testing Coverage
- [ ] Update test fixtures and staging file creation helpers
- [ ] Verify end-to-end field propagation in integration tests
- [ ] Test metadata serialization and deserialization cycles

## Key Architectural Concepts

### Metadata Persistence Strategy
Most staging file fields are stored as JSON within a metadata column, requiring:
- **Serialization:** Converting model structures to JSON format
- **Deserialization:** Reconstructing models from stored JSON
- **Schema Evolution:** Handling field additions without breaking existing data

### Data Flow Separation
The architecture maintains clear separation of concerns:
- **Value Generation:** Occurs only in batch router
- **Value Transport:** Warehouse client moves data without modification
- **Value Validation:** Warehouse API ensures data integrity
- **Value Persistence:** Repository handles storage mechanics

### Backward Compatibility Requirements
New field implementations must:
- Provide sensible defaults for missing values
- Handle graceful degradation when fields are absent
- Consider future evolution paths for field requirements

## Common Implementation Pitfalls

1. **Incomplete Batch Router Population:** Field structure exists but values are not set in batch router
2. **Misplaced Value Generation:** Attempting to fetch or compute values in warehouse client instead of batch router
3. **Broken Metadata Flow:** Field reaches model but persistence conversion is incomplete
4. **Test-Only Implementation:** Field works in test environment but fails in production data flow

## Reference Implementation Patterns

For consistent implementation approaches:
- Follow established patterns from existing staging file fields
- Maintain architectural layer separation used in current implementations
- Apply the same metadata persistence strategies for similar data types

## File Organization Reference

| Component | Primary Focus | Implementation Scope |
|-----------|---------------|---------------------|
| Batch Router | Data generation and context capture | Batch processing and initial payload creation |
| Warehouse Client | Inter-service communication | HTTP transport and format conversion |
| Warehouse API | Request handling and validation | Incoming data processing and routing |
| Model/Repo | Data representation and persistence | Core business logic, data structures, and database operations |
| Test Infrastructure | Development and validation support | Test utilities and verification helpers |

This architectural approach ensures staging file metadata accurately captures processing context for comprehensive traceability and effective debugging across the warehouse service.
