from google.cloud import firestore

class FirestoreLoader:
    def __init__(self):
        self.db = firestore.Client()

    def load_allowed_user_ids(self):
        docs = self.db.collection("allowedUserIDs").stream()
        allowed_users = set()
        for doc in docs:
            data = doc.to_dict()
            allowed_users.add(int(data["ID"]))
        return allowed_users

    def load_bot_config(self):
        doc = self.db.collection("configs").document("telegram-bot").get()
        if not doc.exists:
            raise RuntimeError("Config document not found in Firestore.")
        return doc.to_dict()

    def load_owner_id(self):
        docs = self.db.collection("allowedUserIDs").stream()
        for doc in docs:
            data = doc.to_dict()
            if data["Role"] == "Owner":
                return int(data["ID"])
        raise RuntimeError("Owner ID not found in Firestore.")

