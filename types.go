package awsdiskusagehandler

import "errors"

// custom errors
var ErrNotFound = errors.New("not found")

// file structure specifically for Parquest format
// Example:
//
//	{
//	    "sourceBucket": "example-source-bucket",
//	    "destinationBucket": "arn:aws:s3:::example-destination-bucket",
//	    "version": "2016-11-30",
//	    "creationTimestamp" : "1514944800000",
//	    "fileFormat": "Parquet",
//	    "fileSchema": "message s3.inventory { required binary bucket (UTF8); required binary key (UTF8); optional binary version_id (UTF8); optional boolean is_latest; optional boolean is_delete_marker; optional int64 size; optional int64 last_modified_date (TIMESTAMP_MILLIS); optional binary e_tag (UTF8); optional binary storage_class (UTF8); optional boolean is_multipart_uploaded; optional binary replication_status (UTF8); optional binary encryption_status (UTF8); optional int64 object_lock_retain_until_date (TIMESTAMP_MILLIS); optional binary object_lock_mode (UTF8); optional binary object_lock_legal_hold_status (UTF8); optional binary intelligent_tiering_access_tier (UTF8); optional binary bucket_key_status (UTF8); optional binary checksum_algorithm (UTF8); optional binary object_access_control_list (UTF8); optional binary object_owner (UTF8);}",
//	    "files": [
//	        {
//	           "key": "inventory/example-source-bucket/data/d754c470-85bb-4255-9218-47023c8b4910.parquet",
//	            "size": 56291,
//	            "MD5checksum": "5825f2e18e1695c2d030b9f6eexample"
//	        }
//	    ]
//	}
//
// docs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-location.html
type File struct {
	Key         string `json:"key"`
	Size        int    `json:"size"`
	MD5Checksum string `json:"MD5checksum"`
}

// Inventory represents the entire JSON structure.
type Inventory struct {
	SourceBucket      string `json:"sourceBucket"`
	DestinationBucket string `json:"destinationBucket"`
	Version           string `json:"version"`
	CreationTimestamp string `json:"creationTimestamp"`
	FileFormat        string `json:"fileFormat"`
	FileSchema        string `json:"fileSchema"`
	Files             []File `json:"files"`
}
