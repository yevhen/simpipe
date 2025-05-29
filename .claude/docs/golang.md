## Development Setup
- Go version: 1.24.1
- Dependencies: Uses `github.com/stretchr/testify` for testing

## Build & Test Commands
- Run all tests: `go test ./...`
- Run specific test: `go test -run TestName ./package/...`
- Run with verbose output: `go test -v ./...`
- No Makefile found; use standard Go commands

## Code Style Guidelines
- **Imports**: Standard Go import formatting (stdlib first, then external)
- **Types**: Uses Go generics with `[T any]` for type parameters
- **Naming**: 
  - Packages: lowercase (`blocks`, `routing`)
  - Types: PascalCase (`ActionBlock`, `BatchBlock`)
  - Methods: PascalCase for exported, camelCase for internal
  - Test functions: Begin with `Test` followed by behavior description
- **Error Handling**: Return errors explicitly, no panics in core code
- **Testing**: Uses testify for assertions and standard table-driven tests

## Project Structure
- `/blocks`: Low-level concurrent processing primitives
- `/routing`: Higher-level pipeline components

