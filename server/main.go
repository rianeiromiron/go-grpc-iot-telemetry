package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"net"

	pb "proyecto3/proto"

	"google.golang.org/grpc"
)

type servidorTelemetria struct {
	pb.UnimplementedTelemetriaServiceServer
}

// Implementamos el método de Client Streaming
func (s *servidorTelemetria) RegistrarLecturas(stream pb.TelemetriaService_RegistrarLecturasServer) error {
	log.Println("📥 Servidor: Nueva conexión de streaming abierta por el sensor.")

	var totalMuestras int32 = 0
	var sumaTemperatura float64 = 0.0
	tempMax := -math.MaxFloat64
	tempMin := math.MaxFloat64

	// Bucle para recibir cada dato que el cliente vaya enviando
	for {
		lectura, err := stream.Recv()

		// Cuando el cliente termina de enviar todos sus datos, stream.Recv() devuelve io.EOF
		if err == io.EOF {
			var promedio float64 = 0.0
			if totalMuestras > 0 {
				promedio = sumaTemperatura / float64(totalMuestras)
			} else {
				tempMin = 0
				tempMax = 0
			}

			// Creamos el reporte estadístico final
			resumen := &pb.ResumenEstadistico{
				TotalMuestras:       totalMuestras,
				TemperaturaPromedio: promedio,
				TemperaturaMaxima:   tempMax,
				TemperaturaMinima:   tempMin,
				Mensaje:             fmt.Sprintf("Procesadas exitosamente %d lecturas.", totalMuestras),
			}

			log.Printf("📊 Servidor: Calculado resumen -> Total: %d, Prom: %.2f°C, Min: %.2f°C, Max: %.2f°C\n",
				totalMuestras, promedio, tempMin, tempMax)

			// SendAndClose envía la respuesta única y cierra el ciclo RPC
			return stream.SendAndClose(resumen)
		}

		if err != nil {
			log.Printf("❌ Error al recibir del cliente: %v\n", err)
			return err
		}

		// Procesamos la lectura recibida
		totalMuestras++
		temp := lectura.GetTemperatura()
		sumaTemperatura += temp

		if temp > tempMax {
			tempMax = temp
		}
		if temp < tempMin {
			tempMin = temp
		}

		log.Printf("🌡️ Servidor: Recibida muestra #%d de [%s] = %.2f°C (Hora: %s)\n",
			totalMuestras, lectura.GetSensorId(), temp, lectura.GetTimestamp())
	}
}

func main() {
	puerto := "127.0.0.1:50053"

	listener, err := net.Listen("tcp", puerto)
	if err != nil {
		log.Fatalf("❌ Error al abrir puerto %s: %v", puerto, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterTelemetriaServiceServer(grpcServer, &servidorTelemetria{})

	log.Printf("🚀 Servidor gRPC (Client-Streaming) escuchando en %s...\n", puerto)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("❌ Error al servir gRPC: %v", err)
	}
}
