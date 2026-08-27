package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	pb "proyecto3/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	direccionServidor := "127.0.0.1:50053"

	conn, err := grpc.NewClient(direccionServidor, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("❌ No se pudo conectar: %v", err)
	}
	defer conn.Close()

	cliente := pb.NewTelemetriaServiceClient(conn)

	// 1. Abrimos el canal de transmisión hacia el servidor
	stream, err := cliente.RegistrarLecturas(context.Background())
	if err != nil {
		log.Fatalf("❌ Error al abrir stream: %v", err)
	}

	sensorID := "SENSOR-SALA-01"
	log.Printf("📡 Cliente: Transmitiendo ráfaga de lecturas para %s...\n", sensorID)

	// 2. Simulamos el envío de 6 lecturas de temperatura en streaming
	for i := 1; i <= 6; i++ {
		// Temperatura simulada entre 18.0°C y 32.0°C
		tempSimulada := 18.0 + rand.Float64()*14.0

		lectura := &pb.LecturaSensor{
			SensorId:    sensorID,
			Temperatura: tempSimulada,
			Timestamp:   time.Now().Format("15:04:05"),
		}

		// Enviamos la lectura por el stream
		if err := stream.Send(lectura); err != nil {
			log.Fatalf("❌ Error enviando dato: %v", err)
		}

		log.Printf("📤 Cliente: Enviada lectura #%d -> %.2f°C\n", i, tempSimulada)
		time.Sleep(600 * time.Millisecond) // Pausa entre lecturas
	}

	// 3. Cerramos el stream del cliente y recibimos la respuesta única del servidor
	log.Println("🔒 Cliente: Terminamos de enviar datos. Esperando reporte final...")
	respuesta, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("❌ Error al recibir respuesta final: %v", err)
	}

	// 4. Mostramos el reporte recibido
	log.Printf("\n🎉 REPORTE RECIBIDO DEL SERVIDOR:")
	log.Printf("📌 Total muestras procesadas : %d", respuesta.GetTotalMuestras())
	log.Printf("📈 Temperatura Máxima        : %.2f°C", respuesta.GetTemperaturaMaxima())
	log.Printf("📉 Temperatura Mínima        : %.2f°C", respuesta.GetTemperaturaMinima())
	log.Printf("📊 Promedio                  : %.2f°C", respuesta.GetTemperaturaPromedio())
	log.Printf("💬 Mensaje del Servidor      : %s\n", respuesta.GetMensaje())
}
