# Client-Side Streaming IoT Telemetry in Go

A Go gRPC application that demonstrates client-side streaming. The client sends a batch of temperature readings and the server returns one statistical summary after the stream closes.

## Features

- Client-side streaming over one gRPC connection.
- Simulated sensor readings between 18 and 32 degrees Celsius.
- Server-side average, minimum, and maximum temperature calculation.
- Half-close flow using `CloseAndRecv`.

## Project Structure

```text
proto/       Client-streaming contract and generated Go code
server/      gRPC server listening on 127.0.0.1:50053
client/      Client sending six simulated sensor readings
go.mod       Go module and dependency definitions
```

## Requirements

- Go 1.25 or later
- `protoc` and the Go Protocol Buffers plugins when regeneration is needed

## Run

Start the server:

```bash
go run server/main.go
```

In another terminal, start the client:

```bash
go run client/main.go
```

Regenerate the Protocol Buffers code with:

```bash
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/metrica.proto
```

## License

MIT License.
