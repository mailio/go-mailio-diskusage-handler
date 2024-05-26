# Mailio DiskUsage Handler

GO DiskUsage handler for Mailio. 

This module implements an interface for retrieving disk usage information per user ID collected by the AWS Inventory System. It is designed to be integrated into a larger project. This module focuses on collecting disk usage data and storing it in memory.

## Install

```
go get github.com/mailio/go-mailio-diskusage-handler
```

## Features

- periodically retrieves the information from AWS Inventory System about disk usage 
- prepares the data user by user basis
- stores all accumulated file information in the memory

## Usage

**init**
```go
apiKey := os.Getenv("api_key")
secretKey := os.Getenv("secret")
region := os.Getenv("region")
handler := NewAwsDiskUsageHandler(apiKey, secretKey, region, "bucket/folder/inventory",24*60*60)
defer handler.Stop()
```

`NewAwsDiskUsageHandler` input parameters:
- *apiKey* (AWS API KEY) 
- *secretKey* (AWS SECRET)
- *region* (AWS REGION) of stored inventory
- *bucket/folder/inventory* (root folder of your inventory)
- *24*60*60* cron job to be repeated in seconds

**query**
```go
du, err := handler.GetDiskUsage(randomKey)
```
returns object of type: `mailiotypes "github.com/mailio/go-mailio-server/diskusage/types"`

```go
type DiskUsage struct {
	SizeBytes   int64  `json:"sizeBytes" validate:"required"`
	Address     string `json:"address" validate:"required"`
	NumberFiles int64  `json:"numberFiles,omitempty"`
}
```

## Development

Create .env file to store AWS keys and secrets: 
```env
api_key=AKI...
secret=aki...
region=us-east-1
```

Checkout the `handler_test.go` and `handler.go`




