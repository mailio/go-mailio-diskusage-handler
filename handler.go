package awsdiskusagehandler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/array"
	pfile "github.com/apache/arrow/go/v10/parquet/file"
	"github.com/apache/arrow/go/v10/parquet/pqarrow"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	mailiotypes "github.com/mailio/go-mailio-server/diskusage/types"
	"github.com/robfig/cron/v3"
)

type AwsDiskUsageHandler struct {
	s3Downloader  *s3manager.Downloader
	inventoryPath string
	cron          *cron.Cron
	diskUsageMap  map[string]mailiotypes.DiskUsage
}

// inventoryPath from docs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-location.html
// Parameters:
// - apiKey: Aws api key
// - secret: Aws secret
// - region: Aws region
// - inventoryPath: bucket/path/inventory
func NewAwsDiskUsageHandler(apiKey, secret, region, inventoryPath string, refreshPeriodSeconds int64) *AwsDiskUsageHandler {
	creds := credentials.NewStaticCredentials(apiKey, secret, "")
	config := aws.NewConfig().WithCredentials(creds).WithRegion(region)
	sess, err := session.NewSession(config)
	if err != nil {
		log.Fatalf("Error creating new aws session: %v", err)
	}
	downloader := s3manager.NewDownloader(sess)

	cron := cron.New(cron.WithChain(cron.Recover(cron.DefaultLogger)))

	handler := &AwsDiskUsageHandler{
		s3Downloader:  downloader,
		inventoryPath: inventoryPath,
		cron:          cron,
		diskUsageMap:  make(map[string]mailiotypes.DiskUsage), // empty map
	}
	pattern := fmt.Sprintf("@every %ds", refreshPeriodSeconds)
	handler.start(pattern)
	handler.executeJob() // execute at the start
	return handler
}

// start cron job
func (du *AwsDiskUsageHandler) start(pattern string) {
	du.cron.AddFunc(pattern, du.executeJob)
	du.cron.Start()
}

// stop cron job
// must be called from the external module
func (du *AwsDiskUsageHandler) Stop() {
	du.cron.Stop()
}

// executeJob downloads the manifest JSON file from the S3 bucket and processes the parquet files
// listed in the manifest JSON file. The parquet files are downloaded and parsed using the Arrow
// library. Results are stored in memory for further processing of the external project.
func (du *AwsDiskUsageHandler) executeJob() {
	manifestJson, err := du.getAWSManifestJson(time.Now())
	if err != nil {
		log.Printf("Error getting manifest json: %v", err)
		return
	}
	for _, file := range manifestJson.Files {
		log.Printf("Key: %s, Size: %d", file.Key, file.Size)
		parquetBytes, err := du.downloadBytes(manifestJson.SourceBucket, file.Key)
		if err != nil {
			log.Printf("Error downloading parquet file: %v", err)
			continue
		}
		du.parseParquet(parquetBytes)
	}
}

// getAWSManifestJson constructs the S3 object key for the AWS manifest JSON file
// based on the provided date and time. The S3 key follows the format:
// destination-prefix/source-bucket/config-ID/YYYY-MM-DDTHH-MMZ/manifest.json
//
// Parameters:
// - dt: The date and time for which to construct the manifest JSON key.
//
// Example usage:
// manifestKey := handler.getAWSManifestJson(time.Now())
func (du *AwsDiskUsageHandler) getAWSManifestJson(dt time.Time) (*Inventory, error) {
	// modify the time to be at 1AM UTC
	dt = time.Date(dt.Year(), dt.Month(), dt.Day(), 1, 0, 0, 0, time.UTC)

	formattedDate := dt.Format("2006-01-02T15-04Z")

	s3Path := strings.TrimPrefix(du.inventoryPath, "s3://")
	parts := strings.SplitN(s3Path, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid inventory path: %s", du.inventoryPath)
	}
	bucket := parts[0]
	item := parts[1]

	item += "/" + formattedDate + "/manifest.json"

	itemBytes, err := du.downloadBytes(bucket, item)
	if err != nil {
		if err == ErrNotFound {
			// If the manifest.json file is not found, try previous day's manifest.json file
			dt = dt.AddDate(0, 0, -1)
			formattedDate = dt.Format("2006-01-02T15-04Z")
			item = parts[1] + "/" + formattedDate + "/manifest.json"
			ib, ibErr := du.downloadBytes(bucket, item)
			if ibErr != nil {
				return nil, fmt.Errorf("error downloading manifest.json: %v", ibErr)
			}
			itemBytes = ib
		} else {
			return nil, fmt.Errorf("error downloading manifest.json: %v", err)
		}
	}

	var manifestJson Inventory
	errM := json.Unmarshal(itemBytes, &manifestJson)
	if errM != nil {
		return nil, fmt.Errorf("error unmarshalling manifest.json: %v", errM)
	}

	return &manifestJson, nil
}

// download bytes from s3 and return custom error in case ErrNotFound
func (du *AwsDiskUsageHandler) downloadBytes(bucket string, item string) ([]byte, error) {
	buff := &aws.WriteAtBuffer{}
	_, err := du.s3Downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(item),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case s3.ErrCodeNoSuchBucket:
				return nil, err
			case s3.ErrCodeNoSuchKey:
				return nil, ErrNotFound
			}
		}
		return nil, fmt.Errorf("error downloading manifest.json: %v", err)
	}
	return buff.Bytes(), nil
}

// parseParquet parses the parquet file and stores the disk usage information in memory.
// The parquet file contains multiple columns of which two are of our interest: key and size. The key column contains the user
// address and the size column contains the size of the attachment. The function reads the parquet file in batches and
// stores the disk usage information in a map.
func (du *AwsDiskUsageHandler) parseParquet(parquetBytes []byte) {
	// Create a bytes reader
	buf := bytes.NewReader(parquetBytes)
	// Create a Parquet file reader from the bytes reader
	fr, err := pfile.NewParquetReader(buf)
	if err != nil {
		log.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer fr.Close()

	// Create a memory allocator
	mem := memory.NewGoAllocator()

	// Create an Arrow Table reader
	arrowReader, err := pqarrow.NewFileReader(fr, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		log.Fatalf("Failed to create Arrow file reader: %v", err)
	}

	// Read the entire file into an Arrow Table
	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		log.Fatalf("Failed to read Parquet file into Arrow Table: %v", err)
	}
	log.Printf("Num rows read: %d", table.NumRows())

	batchSize := int64(5)
	tr := array.NewTableReader(table, batchSize)
	defer tr.Release()

	fileKeysArray := []string{}
	sizesArray := []int64{}

	for tr.Next() {
		rec := tr.Record()
		for i, col := range rec.Columns() {
			arrData := col.Data()
			switch rec.ColumnName(i) {
			case "size":
				for bi := 0; bi < arrData.Len(); bi++ {
					size := array.NewInt64Data(arrData).Value(bi)
					sizesArray = append(sizesArray, size)
				}
			case "key":
				for bi := 0; bi < arrData.Len(); bi++ {
					key := array.NewStringData(arrData).Value(bi)
					fileKeysArray = append(fileKeysArray, key)
				}
			default:
				continue
			}
		}
	}

	log.Printf("files: %d, sizes: %d", len(fileKeysArray), len(sizesArray))
	if len(fileKeysArray) != len(sizesArray) { // sanity check
		log.Printf("Error: fileKeysArray and sizesArray have different lengths")
		return // skip this batch
	}

	// Create a map of addresses to sizes
	diskUsageMap := make(map[string]mailiotypes.DiskUsage)
	for i, fileKey := range fileKeysArray {
		size := sizesArray[i]
		fileKeyParts := strings.Split(fileKey, "/")
		if len(fileKeyParts) < 2 {
			log.Printf("Error: invalid file key: %s", fileKey)
			continue
		}
		address := fileKeyParts[0]
		if du, ok := diskUsageMap[address]; ok {
			du.SizeBytes += size
			du.NumberFiles += 1
			diskUsageMap[address] = du
		} else {
			diskUsageMap[address] = mailiotypes.DiskUsage{
				Address:     address,
				SizeBytes:   size,
				NumberFiles: 1,
			}
		}
	}
	du.diskUsageMap = diskUsageMap
}

// GetDiskUsage returns the disk usage for the given user address.
// If the disk usage is not found, the function returns an NotFoundError.
func (h *AwsDiskUsageHandler) GetDiskUsage(userAddress string) (*mailiotypes.DiskUsage, error) {
	if du, ok := h.diskUsageMap[userAddress]; ok {
		return &du, nil
	}
	return nil, ErrNotFound
}
