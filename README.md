# SupplyChainLib

SupplyChainLib is a specialized Python library designed to provide reusable AWS service integrations—such as DynamoDB, S3, SNS, and Lambda—for the Supply Chain Django web application. The library encapsulates AWS SDK interactions using OOP and best practices to facilitate robust and scalable cloud-native development.

## Features

- DynamoDB resource and client operations wrapper
- S3 file upload, download, and bucket management
- SNS topic publish and subscription management
- AWS Lambda invocation utilities
- Designed for modular and maintainable integration in Django projects

## Installation

Install from local editable source during development:
pip install -e ./supplychainlib/src/

## Usage

Import classes like:
from supplychainlib.aws_dynamodb import DynamoDBManager
from supplychainlib.aws_s3 import S3Manager

Refer to project documentation for details.

## License

MIT License
