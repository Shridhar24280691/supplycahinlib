import random
import string
from datetime import datetime


class TrackingUtility:
    """Helpers for generating unique tracking and order identifiers."""

    @staticmethod
    def generate(prefix: str) -> str:
        """Return an identifier with prefix, date and random suffix."""
        date_part = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        random_part = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
        return f"{prefix}-{date_part}-{random_part}"


class InventoryUtility:
    """Helpers for simple stock checks."""

    @staticmethod
    def below_threshold(quantity: int, threshold: int) -> bool:
        try:
            return int(quantity) <= int(threshold)
        except (TypeError, ValueError):
            return False
