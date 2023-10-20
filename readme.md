# AWS Lambda Function for YouTube Watch History Processing

## Overview

This repository contains the code for an AWS Lambda function that is designed to manage YouTube watch history data. The
function performs multiple operations such as downloading data from an S3 bucket, preprocessing it, and then queuing
messages to an SQS queue for further processing.

## Features

- Download and preprocess YouTube watch history data from an S3 bucket
- Avoid duplicates by keeping track of existing video IDs
- Queue new video IDs to an Amazon SQS for further processing
- Log various statistics and outcomes for monitoring

## Requirements

- Python 3.10 or above
- AWS SDK for Python (Boto3)
- An existing AWS Lambda environment
- Amazon S3 and SQS services
- AWS managed pandas layer (Version ARN: `arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python310:4`)

## Getting Started

1. Clone the repository
2. Configure the Lambda function to use the AWS managed pandas layer (Layer
   ARN: `arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python310:4`)
3. Deploy the Lambda function to your AWS environment

## Usage

The Lambda function will automatically trigger upon receiving an event, preprocess the data, and perform the
above-mentioned features.

## Logging

Logs are recorded to monitor the function's execution and to debug in case of errors.
