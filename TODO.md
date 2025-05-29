# Simpipe TODO

## Implement Functional Options Pattern

### Overview
Implement the functional options pattern for flexible and convenient configuration of blocks, processors, and pipelines.

### Routing Package
- Create option types for pipeline components:
  - `PipelineOption[T]` and improved constructor
  - `ProcessorOption[T]` for all processor types
- Implement processor options:
  - `WithFilter[T](filter func(message *T) bool)`
  - `WithParallelismOption[T](n int)`
  - `WithBufferSize[T](size int)`

### Migration Strategy
- Keep backward compatibility initially
- Update tests to use new constructors
- Update documentation with examples
- Mark direct field access as deprecated (with comments)