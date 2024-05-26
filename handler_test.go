package awsdiskusagehandler

import (
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func TestInit(t *testing.T) {
	apiKey := os.Getenv("api_key")
	secretKey := os.Getenv("secret")
	region := os.Getenv("region")
	handler := NewAwsDiskUsageHandler(apiKey, secretKey, region, "mailiosmtpattachments/mailiosmtpattachments/AttachmentSizesInventory", 5)
	defer handler.Stop()

	// random key selection
	keys := make([]string, 0, len(handler.diskUsageMap))
	for k := range handler.diskUsageMap {
		keys = append(keys, k)
	}
	randomKey := keys[rand.Intn(len(handler.diskUsageMap))]

	du, err := handler.GetDiskUsage(randomKey)
	if err != nil {
		t.Fatalf("Error getting disk usage: %v", err)
	}
	t.Logf("Disk usage: %v", du)
	time.Sleep(10 * time.Second)
}
