import boto3
import json
import requests
from botocore.exceptions import ClientError


class LambdaInvoker:
    """
    Invokes AWS Lambda functions directly (via boto3)
    """

    def __init__(self, region_name="us-east-1"):
        self.lambda_client = boto3.client('lambda', region_name=region_name)

    # --- Direct Invocation (Lambda ARN) ---
    def invoke_function(self, function_name, payload, invocation_type='Event'):
        """Invoke Lambda directly via AWS SDK"""
        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,  # 'Event' = async, 'RequestResponse' = sync
                Payload=json.dumps(payload).encode('utf-8')
            )
            print(f" Invoked Lambda: {function_name}")
            return response
        except ClientError as e:
            print(f" Lambda invocation failed: {e}")
            return None
