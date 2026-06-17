# What is LuxLibertas?

Light pierces the darkness, freedom flows through the wall. A lightweight, high-performance VLESS over Websocket transit core built with Go.

# Performance Benchmark

## 📊 Benchmark Environment Info

- **OS**: `darwin` (macOS)
- **Architecture**: `amd64`
- **CPU**: `Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz`
- **Date**: June 9, 2026
- **Go Version**: `go version` (executed by system go environment)

---

## 📈 Benchmark Results

| Benchmark Name | Runs (Iterations) | Time (ns/op) | Memory (B/op) | Allocations (allocs/op) | Throughput (MB/s) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **VLESS Header Parsing** | | | | | |
| `BenchmarkParseVLESSHeader` | 4,803,958 | 247.0 ns/op | 576 B/op | 3 allocs/op | N/A |
| `BenchmarkParseVLESSHeader_Pure` | 12,149,959 | 110.5 ns/op | 16 B/op | 1 allocs/op | N/A |
| `BenchmarkParseVLESSHeader_IPv6` | 7,297,346 | 165.9 ns/op | 24 B/op | 1 allocs/op | N/A |
| `BenchmarkParseVLESSHeader_Domain` | 9,441,165 | 125.8 ns/op | 19 B/op | 2 allocs/op | N/A |
| `BenchmarkParseVLESSHeader_Mux` | 36,218,018 | 33.46 ns/op | 0 B/op | 0 allocs/op | N/A |
| `BenchmarkParseUUID` | 8,094,264 | 142.7 ns/op | 48 B/op | 2 allocs/op | N/A |
| **Connection & Relay Hot Paths** | | | | | |
| `BenchmarkWSConn_ReadHotPath` | 10,765,162 | 118.9 ns/op | 512 B/op | 1 allocs/op | N/A |
| `BenchmarkMuxServer_Relay` | 154,849 | 7,419.0 ns/op | 262 B/op | 3 allocs/op | 552.12 MB/s |
| `BenchmarkSlogLogging` | 6,248,131 | 186.6 ns/op | 0 B/op | 0 allocs/op | N/A |
| **Optimization Comparison (Old vs New)** | | | | | |
| `BenchmarkWSConnAllocation_Direct` | 49,302,873 | 24.73 ns/op | 32 B/op | 1 allocs/op | N/A |
| `BenchmarkWSConnAllocation_Pool` | 95,557,233 | 11.10 ns/op | 0 B/op | 0 allocs/op | N/A |
| `BenchmarkConnSetup_Old` | 6,813,486 | 161.6 ns/op | 640 B/op | 3 allocs/op | N/A |
| `BenchmarkConnSetup_New` | 38,127,796 | 28.64 ns/op | 0 B/op | 0 allocs/op | N/A |
| `BenchmarkMuxPool_PerServer` | 381,087 | 2,917.0 ns/op | 18,277 B/op | 4 allocs/op | N/A |
| `BenchmarkMuxPool_Global` | 100,000,000 | 12.01 ns/op | 0 B/op | 0 allocs/op | N/A |
| `BenchmarkSessionChan_Cap128` | 1,884,926 | 654.9 ns/op | 4,976 B/op | 2 allocs/op | N/A |
| `BenchmarkSessionChan_Cap32` | 5,471,156 | 225.9 ns/op | 1,264 B/op | 2 allocs/op | N/A |

---
