# Mailio DiskUsage Handler

GO DiskUsage handler for Mailio. 

This module implements an interface for retrieving disk usage information per user ID collected by the AWS Inventory System. It is designed to be integrated into a larger project. This module focuses on collecting disk usage data without storing it.

## Install

```
go get github.com/mailio/go-mailio-diskusage-handler
```

## Features

- periodically retrieves the information from AWS Inventory System about disk usage 
- prepares the data user by user basis







