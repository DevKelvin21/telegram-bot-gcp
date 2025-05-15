from google.cloud import firestore
from datetime import datetime
import pytz

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
            if inventory_data["quantity"] < quantity:
                issues.append({
                    "timestamp": self.current_cst_iso(),
                    "transaction_id": transaction_id,
                    "item": item,
                    "quality": quality,
                    "requested_qty": quantity,
                    "reason": "no hay suficiente inventario"
                })

            # Deduct inventory
            new_quantity = max(0, inventory_data["quantity"] - quantity)
            self.db.collection("inventory").document(f"{item}_{quality}").set(
                {"quantity": new_quantity}, merge=True
            )

        for issue in issues:
            self.db.collection("inventory_issues").add(issue)

        return issues

    def update_inventory(self, item, quality, quantity):
        item, quality = self.resolve_synonyms(item, quality)
        self.db.collection("inventory").document(f"{item}_{quality}").set(
            {"item": item, "quality": quality, "quantity": quantity}, merge=True
        )

    def restore_inventory(self, item, quality, quantity):
        item, quality = self.resolve_synonyms(item, quality)
        inventory_doc = self.db.collection("inventory").document(f"{item}_{quality}").get()
        if inventory_doc.exists:
            inventory_data = inventory_doc.to_dict()
            new_quantity = max(0, inventory_data["quantity"] + quantity)
            self.db.collection("inventory").document(f"{item}_{quality}").set(
                {"quantity": new_quantity}, merge=True
            )
        else:
            self.update_inventory(item, quality, quantity)

    def current_cst_iso(self):
        return datetime.now(self.timezone).isoformat()
