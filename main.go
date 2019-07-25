package main

import (
	reader "kafka-hello-world/reader"
	writer "kafka-hello-world/writer"
)

func main() {
	writer.Send()
	reader.Read()
}
