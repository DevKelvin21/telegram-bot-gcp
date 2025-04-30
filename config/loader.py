from google.cloud import firestore

class FirestoreLoader:
    """
    A utility class for interacting with Firestore to load configuration data,
    allowed user IDs, and the owner ID for the Telegram bot.
    """

    def __init__(self):
        """
        Initializes the FirestoreLoader with a Firestore client instance.
        """
        self.db = firestore.Client()

    def load_allowed_user_ids(self):
        """
        Loads the set of allowed user IDs from the Firestore collection "allowedUserIDs".

        Returns:
            set: A set of integers representing the allowed Telegram user IDs.
        """
        docs = self.db.collection("allowedUserIDs").stream()
        allowed_users = set()
        for doc in docs:
            data = doc.to_dict()
            allowed_users.add(int(data["ID"]))
        return allowed_users

    def load_bot_config(self):
        """
        Loads the bot configuration document from the Firestore collection "configs".

        Returns:
            dict: A dictionary containing the bot configuration.

        Raises:
            RuntimeError: If the configuration document is not found in Firestore.
        """
        doc = self.db.collection("configs").document("telegram-bot").get()
        if not doc.exists:
            raise RuntimeError("Config document not found in Firestore.")
        return doc.to_dict()

    def load_owner_id(self):
        """
        Loads the owner ID from the Firestore collection "allowedUserIDs".
        The owner is identified by the "Role" field set to "Owner".

        Returns:
            int: The Telegram user ID of the owner.

        Raises:
            RuntimeError: If no owner ID is found in Firestore.
        """
        docs = self.db.collection("allowedUserIDs").stream()
        for doc in docs:
            data = doc.to_dict()
            if data["Role"] == "Owner":
                return int(data["ID"])
        raise RuntimeError("Owner ID not found in Firestore.")

