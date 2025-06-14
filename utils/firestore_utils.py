import logging
from google.cloud import firestore
from datetime import datetime
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s"  # Define the log message format
)

class FirestoreInventoryManager:
    def __init__(self):
        self.db = firestore.Client()
        self.timezone = pytz.timezone("America/El_Salvador")

    def resolve_synonyms(self, item, quality):
        synonyms = self.db.collection("inventory_synonyms").stream()
        for doc in synonyms:
            data = doc.to_dict()
            if data["alias"].lower() == item.lower():
                return data["item"], data.get("quality", quality)
        return item, quality

    def deduct_inventory(self, sales, transaction_id):
        issues = []
        for sale in sales:
            item, quality = self.resolve_synonyms(sale["item"], sale.get("quality", "regular"))
            quantity = sale.get("quantity", 0)

            inventory_doc = self.db.collection("inventory").document(f"{item}_{quality}").get()
            if not inventory_doc.exists:
                issues.append({
                    "timestamp": self.current_cst_iso(),
                    "transaction_id": transaction_id,
                    "item": item,
                    "quality": quality,
                    "requested_qty": quantity,
                    "reason": "no existe en inventario"
                })
                continue

            inventory_data = inventory_doc.to_dict()
            # Ensure quantity is an integer for comparison and arithmetic
            try:
                inventory_qty = int(inventory_data["quantity"])
            except (ValueError, TypeError, KeyError) as e:
                logging.warning(
                    "Failed to convert inventory quantity to integer. Defaulting to 0. "
                    f"Transaction ID: {transaction_id}, Item: {item}, Quality: {quality}, "
                    f"Error: {e}, Inventory Data: {inventory_data}"
                )
                inventory_qty = 0
            if inventory_qty < quantity:
                issues.append({
                    "timestamp": self.current_cst_iso(),
                    "transaction_id": transaction_id,
                    "item": item,
                    "quality": quality,
                    "requested_qty": quantity,
                    "reason": "no hay suficiente inventario"
                })

            # Deduct inventory
            new_quantity = max(0, inventory_qty - quantity)
            self.db.collection("inventory").document(f"{item}_{quality}").set(
                {"quantity": new_quantity, "lastUpdated": self.current_cst_iso()}, merge=True
            )

        for issue in issues:
            self.db.collection("inventory_issues").add(issue)

        return issues

    def update_inventory(self, item, quality, quantity):
        item, quality = self.resolve_synonyms(item, quality)
        self.db.collection("inventory").document(f"{item}_{quality}").set(
            {"item": item, "quality": quality, "quantity": quantity, "lastUpdated": self.current_cst_iso()}, merge=True
        )

    def restore_inventory(self, item, quality, quantity):
        item, quality = self.resolve_synonyms(item, quality)
        inventory_doc = self.db.collection("inventory").document(f"{item}_{quality}").get()
        if inventory_doc.exists:
            inventory_data = inventory_doc.to_dict()
            new_quantity = max(0, inventory_data["quantity"] + quantity)
            self.db.collection("inventory").document(f"{item}_{quality}").set(
                {"quantity": new_quantity, "lastUpdated": self.current_cst_iso()}, merge=True
            )
        else:
            self.update_inventory(item, quality, quantity)
            logging.info(f"Inventory document not found for item '{item}', quality '{quality}'. Calling update_inventory with quantity {quantity}.")

    def current_cst_iso(self):
        return datetime.now(self.timezone).isoformat()

    def log_inventory_loss(self, user_id, user_name, chat_id, item, quality, quantity, original_message, timestamp):
        """
        Logs a single inventory loss entry to the 'inventory_loss' Firestore collection.
        """
        self.db.collection("inventory_loss").add({
            "timestamp": timestamp,
            "user_id": user_id,
            "user_name": user_name,
            "item": item,
            "quality": quality,
            "quantity": quantity,
            "original_message": original_message,
            "chat_id": chat_id,
        })
