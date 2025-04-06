class UserController:
    @staticmethod
    def authenticate(email, password):
        return email == "test@example.com" and password == "password"

    @staticmethod
    def authenticate_superuser(email, password):
        return email == "admin@example.com" and password == "adminpass"

    @staticmethod
    def verify_superuser_token(token):
        return True

    @staticmethod
    def get_users(**kwargs):
        return [{"email": "user1@example.com"}, {"email": "user2@example.com"}]


class ClientMachineController:
    @staticmethod
    def create_client_machine(**kwargs):
        return "machine_001"
