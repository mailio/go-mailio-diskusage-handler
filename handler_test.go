package awsdiskusagehandler

import (
	"log"
	"os"
	"testing"

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
	handler := NewAwsDiskUsageHandler(apiKey, secretKey, "us-west-2", "mailiosmtpattachments/mailiosmtpattachments/AttachmentSizesInventory", 60)
	defer handler.Stop()
}
