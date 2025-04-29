from google.cloud import firestore


def load_allowed_user_ids():
    db = firestore.Client()
    docs = db.collection("allowedUserIDs").stream()
    allowed_users = set()
    for doc in docs:
        data = doc.to_dict()
        allowed_users.add(int(data["ID"]))
    return allowed_users


def load_bot_config():
    db = firestore.Client()
    doc = db.collection("configs").document("telegram-bot").get()
    if not doc.exists:
        raise RuntimeError("Config document not found in Firestore.")
    return doc.to_dict()
