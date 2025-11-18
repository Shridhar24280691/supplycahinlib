import boto3
from botocore.exceptions import ClientError
import json

class SNSManager:
    """
    AWS SNS utility for managing topics, subscriptions, and sending notifications.
    """

    def __init__(self, topic_arn=None, region_name="us-east-1"):
        """
        Initialize SNS manager.
        Args:
            topic_arn (str): Optional. ARN of an existing SNS topic.
            region_name (str): AWS region.
        """
        self.sns_client = boto3.client('sns', region_name=region_name)
        self.topic_arn = topic_arn

    def create_topic(self, topic_name):
        """
        Create a new SNS topic or fetch an existing one.
        Returns the ARN of the topic.
        """
        try:
            response = self.sns_client.create_topic(Name=topic_name)
            self.topic_arn = response['TopicArn']
            return self.topic_arn
        except ClientError as e:
            raise RuntimeError(f"Error creating topic: {e}")

    def list_topics(self):
        """
        Returns a list of SNS topic ARNs.
        """
        try:
            response = self.sns_client.list_topics()
            return [t['TopicArn'] for t in response.get('Topics', [])]
        except ClientError as e:
            raise RuntimeError(f"Error listing topics: {e}")

    def subscribe(self, protocol, endpoint, filter_policy=None):
        """
        Subscribe an endpoint to the SNS topic.
        Args:
            protocol (str): 'email', 'sms', 'lambda', 'https', etc.
            endpoint (str): Email address, phone number, or ARN.
        Returns the subscription ARN.
        """
        if not self.topic_arn:
            raise ValueError("Topic ARN not set. Set or create a topic before subscribing.")
            
        attributes = {}
        
        if filter_policy:
            attributes["FilterPolicy"] = json.dumps(filter_policy)
        try:
            response = self.sns_client.subscribe(
                TopicArn=self.topic_arn,
                Protocol=protocol,
                Endpoint=endpoint,
                Attributes=attributes,
                ReturnSubscriptionArn=True
            )
            return response['SubscriptionArn']
        except ClientError as e:
            raise RuntimeError(f"Error subscribing endpoint: {e}")

    def list_subscriptions(self):
        """
        Returns a list of subscriptions for the current SNS topic.
        """
        if not self.topic_arn:
            raise ValueError("Topic ARN not set.")
        try:
            response = self.sns_client.list_subscriptions_by_topic(TopicArn=self.topic_arn)
            return response.get('Subscriptions', [])
        except ClientError as e:
            raise RuntimeError(f"Error listing subscriptions: {e}")

    def publish_message(self, subject, message, message_attributes=None):
        """
        Publish a message to the SNS topic.
        Args:
            subject (str): Message subject.
            message (str or dict): Message body.
            json_mode (bool): If True, message is sent as JSON.
        Returns the SNS publish response.
        """
        if not self.topic_arn:
            raise ValueError("Topic ARN not set.")
        params = {
            "TopicArn": self.topic_arn,
            "Subject": subject,
            "Message": message,
        }
        if message_attributes:
            params["MessageAttributes"] = message_attributes

        try:
            response = self.sns_client.publish(**params)
            return response
        except ClientError as e:
            raise RuntimeError(f"Failed to send SNS message: {e}")

    def send_low_stock_alert(self, product_name, quantity):
        """
        Send a stock alert notification via SNS.
        """
        message = f"Product '{product_name}' is below the reorder level ({quantity} left)."
        return self.publish_message("Low Stock Alert", message)
    
    def ensure_customer_subscription(self, customer_email):
        """
        Ensure a subscription exists for this customer email
        with a filter policy on customer_email.
        """
        if not self.topic_arn:
            raise ValueError("Topic ARN not set.")

        # Check if this email is already subscribed
        existing = self.list_subscriptions()
        for sub in existing:
            if sub.get("Endpoint") == customer_email:
                return sub.get("SubscriptionArn")

        filter_policy = {
            "customer_email": [customer_email]
        }

        return self.subscribe(
            protocol="email",
            endpoint=customer_email,
            filter_policy=filter_policy,
        )