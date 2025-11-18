from typing import Optional, Dict, List, Any
import boto3
import os
from decimal import Decimal


class DynamoDBManager:
    """Wrapper class for DynamoDB table operations."""

    def __init__(self, table_name: str, region_name: Optional[str] = None):
        region = region_name or os.getenv("AWS_REGION", "us-east-1")
        resource = boto3.resource("dynamodb", region_name=region)
        self.table = resource.Table(table_name)

    # -------------------- Basic CRUD --------------------

    def get_item(self, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch a single item by its key."""
        try:
            response = self.table.get_item(Key=key)
            return response.get("Item")
        except Exception as e:
            print(f"Get error in {self.table.name}: {e}")
            return None

    def put_item(self, item: Dict[str, Any]) -> None:
        """Insert or replace an item."""
        try:
            item = self._prepare_for_dynamo(item)
            self.table.put_item(Item=item)
        except Exception as e:
            print(f"Put error in {self.table.name}: {e}")

    def update_item(self, key: Dict[str, Any], update_expr: str, values: Dict[str, Any], names: Optional[Dict[str, str]] = None,) -> None:
        """Update an item with an expression."""
        try:
            params = {
                "Key": key,
                "UpdateExpression": update_expr,
                "ExpressionAttributeValues": values,
            }
            if names:
                params["ExpressionAttributeNames"] = names
            self.table.update_item(**params)
        except Exception as e:
            print(f"Update error in {self.table.name}: {e}")

    def delete_item(self, key: Dict[str, Any]) -> None:
        """Delete an item."""
        try:
            self.table.delete_item(Key=key)
        except Exception as e:
            print(f"Delete error in {self.table.name}: {e}")

    # -------------------- Scan --------------------

    def scan_table(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """Scan all items in a table."""
        items: List[Dict[str, Any]] = []
        try:
            response = self.table.scan(**kwargs)
            items.extend(response.get("Items", []))

            while "LastEvaluatedKey" in response:
                response = self.table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                items.extend(response.get("Items", []))

            return [self._deserialize_item(i) for i in items]
        except Exception as e:
            print(f"Scan error in {self.table.name}: {e}")
            return []

    def scan(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """Alias for scan_table."""
        return self.scan_table(**kwargs)

    # -------------------- Helpers --------------------

    def _prepare_for_dynamo(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert floats to Decimal."""
        def convert(value):
            if isinstance(value, float):
                return Decimal(str(value))
            if isinstance(value, dict):
                return {k: convert(v) for k, v in value.items()}
            if isinstance(value, list):
                return [convert(v) for v in value]
            return value

        return {k: convert(v) for k, v in item.items()}

    def _deserialize_item(self, item: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Convert Decimals back to numeric types."""
        if not item:
            return None

        def convert(value):
            if isinstance(value, Decimal):
                return int(value) if value % 1 == 0 else float(value)
            if isinstance(value, dict):
                return {k: convert(v) for k, v in value.items()}
            if isinstance(value, list):
                return [convert(v) for v in value]
            return value

        return {k: convert(v) for k, v in item.items()}


# -------------------- Domain Managers --------------------

class SupplierManager(DynamoDBManager):
    def __init__(self):
        super().__init__("Suppliers")

    def list_active(self) -> List[dict]:
        return self.scan_table()


class RawMaterialManager(DynamoDBManager):
    def __init__(self):
        super().__init__("RawMaterials")

    def list_for_supplier(self, supplier_id: str) -> List[dict]:
        items = self.scan_table()
        return [m for m in items if m.get("supplier_id") == supplier_id]


class FinishedProductManager(DynamoDBManager):
    def __init__(self):
        super().__init__("FinishedProducts")


class PurchaseOrderManager(DynamoDBManager):
    def __init__(self):
        super().__init__("PurchaseOrders")

    def get_item(self, po_id: str) -> Optional[Dict[str, Any]]:
        return super().get_item({"po_id": po_id})

    def delete_item(self, po_id: str) -> None:
        super().delete_item({"po_id": po_id})


class DistributorManager(DynamoDBManager):
    def __init__(self):
        super().__init__("Distributors")


class DistributorOrderManager(DynamoDBManager):
    def __init__(self):
        super().__init__("DistributorOrders")


class DistributorInventoryManager(DynamoDBManager):
    def __init__(self):
        super().__init__("DistributorInventory")

    def add_stock(self, distributor_id: str, product_id: str, quantity: int) -> None:
        key = {"id": f"{distributor_id}#{product_id}"}
        self.update_item(
            key=key,
            update_expr="SET quantity = if_not_exists(quantity, :z) + :q",
            values={":q": int(quantity), ":z": 0},
        )


class CustomerOrderManager(DynamoDBManager):
    def __init__(self):
        super().__init__("CustomerOrders")
