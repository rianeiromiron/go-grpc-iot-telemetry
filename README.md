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

## Descripción línea por línea del código

### `proto/metrica.proto`

```proto
1  syntax = "proto3";
```
Declara que este archivo usa la versión 3 del lenguaje de Protocol Buffers.

```proto
3  package metrica;
```
Define el paquete `metrica` dentro del propio `.proto`, usado para evitar colisiones de nombres entre mensajes de distintos archivos `.proto`.

```proto
5  option go_package = "proyecto3/proto/metrica";
```
Indica al generador de Go en qué ruta de import debe ubicarse el código generado (`metrica.pb.go` y `metrica_grpc.pb.go`).

```proto
8  service TelemetriaService {
9      rpc RegistrarLecturas (stream LecturaSensor) returns (ResumenEstadistico);
11 }
```
Declara el servicio `TelemetriaService` con el método `RegistrarLecturas`. Aquí la palabra clave `stream` aparece en la **petición**, no en la respuesta: es **client-side streaming**, lo opuesto a proyecto2. El cliente envía múltiples mensajes por el mismo canal y el servidor responde una sola vez, cuando el cliente termina de enviar.

```proto
15 message LecturaSensor {
16     string sensor_id = 1;
17     double temperatura = 2;
18     string timestamp = 3;
19 }
```
Mensaje que el cliente envía repetidamente: identificador del sensor, temperatura en grados Celsius y la hora de la lectura. Los números `1`, `2` y `3` son las *tags* de campo usadas en la codificación binaria, no valores ni índices de array.

```proto
22 message ResumenEstadistico {
23     int32 total_muestras = 1;
24     double temperatura_promedio = 2;
25     double temperatura_maxima = 3;
26     double temperatura_minima = 4;
27     string mensaje = 5;
28 }
```
Mensaje de respuesta única que el servidor calcula y envía cuando el cliente cierra el stream: cantidad de lecturas recibidas, promedio, máxima, mínima y un mensaje descriptivo.

### `server/main.go`

```go
3  import (
4      "fmt"
5      "io"
6      "log"
7      "math"
8      "net"
10     pb "proyecto3/proto"
12     "google.golang.org/grpc"
13 )
```
Importa las librerías estándar necesarias (formateo, manejo de `io.EOF`, logging, funciones matemáticas y red), el paquete generado a partir del `.proto` (con alias `pb`) y el paquete núcleo de gRPC.

```go
15 type servidorTelemetria struct {
16     pb.UnimplementedTelemetriaServiceServer
17 }
```
Define el tipo que implementará el servicio. Al embeber `UnimplementedTelemetriaServiceServer`, el struct satisface la interfaz `TelemetriaServiceServer` aunque no implemente todos sus métodos, lo que da compatibilidad hacia adelante si el `.proto` agrega nuevos métodos en el futuro.

```go
20 func (s *servidorTelemetria) RegistrarLecturas(stream pb.TelemetriaService_RegistrarLecturasServer) error {
```
Implementación del método de client-streaming. La firma es casi inversa a la de proyecto2: no recibe un mensaje de petición como parámetro, sino directamente el `stream` desde el que irá leyendo cada lectura que el cliente envíe.

```go
23     var totalMuestras int32 = 0
24     var sumaTemperatura float64 = 0.0
25     tempMax := -math.MaxFloat64
26     tempMin := math.MaxFloat64
```
Variables acumuladoras que viven durante toda la conexión: cuentan las muestras, suman las temperaturas y llevan el máximo/mínimo visto hasta el momento. Se inicializan en los extremos de `float64` para que la primera lectura real siempre los reemplace.

```go
29     for {
30         lectura, err := stream.Recv()
```
Bucle que recibe lecturas una por una. `stream.Recv()` bloquea la ejecución hasta que llega el siguiente mensaje del cliente o hasta que el cliente cierra su extremo del stream.

```go
33         if err == io.EOF {
```
`io.EOF` es la señal que indica que el cliente terminó de enviar datos (llamó a `CloseAndRecv` del lado cliente). Es el punto donde el servidor deja de acumular y pasa a calcular el resumen final.

```go
34             var promedio float64 = 0.0
35             if totalMuestras > 0 {
36                 promedio = sumaTemperatura / float64(totalMuestras)
37             } else {
38                 tempMin = 0
39                 tempMax = 0
40             }
```
Calcula el promedio evitando una división por cero. Si no llegó ninguna muestra, además resetea `tempMin`/`tempMax` a `0` para no devolver los valores centinela `±math.MaxFloat64`.

```go
43             resumen := &pb.ResumenEstadistico{
44                 TotalMuestras:       totalMuestras,
45                 TemperaturaPromedio: promedio,
46                 TemperaturaMaxima:   tempMax,
47                 TemperaturaMinima:   tempMin,
48                 Mensaje:             fmt.Sprintf("Procesadas exitosamente %d lecturas.", totalMuestras),
49             }
```
Arma el mensaje `ResumenEstadistico` con las estadísticas acumuladas durante todo el stream.

```go
55             return stream.SendAndClose(resumen)
```
`SendAndClose` es el equivalente, en client-streaming, a retornar `(respuesta, nil)` en una RPC unaria: envía la única respuesta y cierra el ciclo RPC en un solo paso. Es la contraparte exacta de `CloseAndRecv()` del lado cliente.

```go
58         if err != nil {
59             log.Printf("❌ Error al recibir del cliente: %v\n", err)
60             return err
61         }
```
Cualquier error de `Recv()` distinto de `io.EOF` es una falla real (por ejemplo, pérdida de conexión), así que corta el bucle propagando el error.

```go
64         totalMuestras++
65         temp := lectura.GetTemperatura()
66         sumaTemperatura += temp
68         if temp > tempMax {
69             tempMax = temp
70         }
71         if temp < tempMin {
72             tempMin = temp
73         }
```
Si la lectura llegó sin errores, actualiza los acumuladores: cuenta la muestra, suma la temperatura para el promedio final y actualiza el máximo/mínimo si corresponde.

```go
81     puerto := "127.0.0.1:50053"
```
Punto de entrada del servidor. Usa el puerto `50053`, distinto de los de proyecto1 (`50051`) y proyecto2 (`50052`), para poder correr los tres servidores a la vez.

```go
83     listener, err := net.Listen("tcp", puerto)
84     if err != nil {
85         log.Fatalf("❌ Error al abrir puerto %s: %v", puerto, err)
86     }
```
Abre un socket TCP en el puerto indicado. Si el puerto ya está en uso o no se puede enlazar, el programa termina con `log.Fatalf`.

```go
88     grpcServer := grpc.NewServer()
89     pb.RegisterTelemetriaServiceServer(grpcServer, &servidorTelemetria{})
```
Crea el servidor gRPC y registra la implementación `servidorTelemetria`, asociando el servicio `TelemetriaService` a esa instancia.

```go
92     if err := grpcServer.Serve(listener); err != nil {
93         log.Fatalf("❌ Error al servir gRPC: %v", err)
94     }
```
Pone al servidor a aceptar y atender conexiones entrantes. Es una llamada bloqueante: el proceso queda corriendo aquí, manejando streams entrantes, hasta que el servidor se detenga o falle.

### `client/main.go`

```go
16     direccionServidor := "127.0.0.1:50053"
```
Punto de entrada del cliente. La dirección debe coincidir con la del servidor (puerto `50053`).

```go
18     conn, err := grpc.NewClient(direccionServidor, grpc.WithTransportCredentials(insecure.NewCredentials()))
19     if err != nil {
20         log.Fatalf("❌ No se pudo conectar: %v", err)
21     }
22     defer conn.Close()
```
Crea la conexión gRPC hacia el servidor. `insecure.NewCredentials()` desactiva TLS, válido aquí porque la comunicación es local; en producción se usarían credenciales TLS. `defer conn.Close()` garantiza que la conexión se cierre al terminar `main`.

```go
24     cliente := pb.NewTelemetriaServiceClient(conn)
```
Crea el *stub* del cliente: expone `RegistrarLecturas` como una función de Go, ocultando los detalles de serialización y transporte.

```go
27     stream, err := cliente.RegistrarLecturas(context.Background())
28     if err != nil {
29         log.Fatalf("❌ Error al abrir stream: %v", err)
30     }
```
Abre el canal de streaming hacia el servidor. A diferencia de proyecto2, aquí no se manda ningún mensaje de petición al abrir el stream: el cliente irá enviando los datos después, uno por uno, con `stream.Send`.

```go
36     for i := 1; i <= 6; i++ {
37         tempSimulada := 18.0 + rand.Float64()*14.0
39         lectura := &pb.LecturaSensor{
40             SensorId:    sensorID,
41             Temperatura: tempSimulada,
42             Timestamp:   time.Now().Format("15:04:05"),
43         }
```
Bucle que simula 6 lecturas de sensor, con una temperatura aleatoria entre 18.0°C y 32.0°C en cada vuelta.

```go
47         if err := stream.Send(lectura); err != nil {
48             log.Fatalf("❌ Error enviando dato: %v", err)
49         }
```
`stream.Send` empuja cada lectura al servidor por el canal abierto, sin esperar ninguna respuesta intermedia: en client-streaming el servidor no contesta hasta el final.

```go
52         time.Sleep(600 * time.Millisecond)
```
Pausa entre lecturas para simular que los datos llegan de forma espaciada en el tiempo, como lo haría un sensor real.

```go
57     respuesta, err := stream.CloseAndRecv()
58     if err != nil {
59         log.Fatalf("❌ Error al recibir respuesta final: %v", err)
60     }
```
`CloseAndRecv` cierra el extremo de envío del cliente (equivalente a decirle al servidor "ya no mando más datos") y bloquea la ejecución hasta recibir la única respuesta final del servidor. Es la contraparte exacta de `stream.SendAndClose` del lado servidor.

```go
64     log.Printf("📌 Total muestras procesadas : %d", respuesta.GetTotalMuestras())
65     log.Printf("📈 Temperatura Máxima        : %.2f°C", respuesta.GetTemperaturaMaxima())
66     log.Printf("📉 Temperatura Mínima        : %.2f°C", respuesta.GetTemperaturaMinima())
67     log.Printf("📊 Promedio                  : %.2f°C", respuesta.GetTemperaturaPromedio())
68     log.Printf("💬 Mensaje del Servidor      : %s\n", respuesta.GetMensaje())
```
Muestra en consola el resumen estadístico recibido, usando los getters generados (`GetTotalMuestras`, etc.) por ser *nil-safe*.

## License

MIT License.
