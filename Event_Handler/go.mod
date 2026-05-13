module Event_Handler

go 1.26.2

replace Shared => ../Shared

require github.com/segmentio/kafka-go v0.4.51

require (
	Shared v0.0.0
	github.com/klauspost/compress v1.17.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/xdg-go/scram v1.2.0 // indirect
	golang.org/x/net v0.51.0 // indirect
)
