import json
import os

import jwt
from tornado.ioloop import IOLoop
from tornado.websocket import WebSocketHandler
from tornado.web import Application, RequestHandler
from tornado.web import StaticFileHandler


from lib.logger import Logger
from lib.controller import UserController, ClientMachineController
from lib.db_handler import HandleDB
from lib.conf import settings

logger_instance = Logger(
    **{"file_name": "error.log", "stream_handler": True, "file_handler": True}
)
logger_instance_info = Logger(
    **{"file_name": "tornado_server.log", "stream_handler": True, "file_handler": False}
)


active_clients = []
hdb_instance = HandleDB()


class SocketOutputHandler(WebSocketHandler):

    def check_origin(self):
        return True

    def open(self):
        try:
            token = self.request.headers.get("Authorization")
            if token is None:
                self.close()
                return

            payload = jwt.decode(token, verify=False)
            if "email" not in payload:
                self.close()
                return

            client_params = {}
            client_params["secret_websocket_key"] = self.request.headers.get(
                "Sec-Websocket-Key"
            )
            client_params["client_id"] = self.request.headers.get("client_id")
            active_client_id = hdb_instance.add_active_client(**client_params)

            if active_client_id is None:
                print("could not activate client!")
                self.close()
            else:
                print("client activated: {}".format(active_client_id))

            if self not in active_clients:
                active_clients.append(self)

        except jwt.exceptions.DecodeError as error:
            self.close()

        except Exception as error:
            logger_instance.logger.error("SocketOutputHandler::open:{}".format(error))

    def on_message(self, message):
        try:
            logger_instance_info.logger.info("client message::{}".format(message))
        except Exception as error:
            logger_instance.logger.error(
                "SocketOutputHandler::on_message:{}".format(error)
            )

    def on_close(self):
        try:
            secret_websocket_key = self.request.headers.get("Sec-Websocket-Key")
            if hdb_instance.remove_active_client(secret_websocket_key):
                logger_instance_info.logger.info("client removed from server index.")
            else:
                logger_instance_info.logger.info("error in removing client!")

            logger_instance_info.logger.info("disconnecting client...")
        except Exception as error:
            logger_instance.logger.error(
                "SocketOutputHandler::on_close:{}".format(error)
            )

    def authenticate_client(self):
        try:
            pass
        except Exception as error:
            logger_instance.logger.error(
                "SocketOutputHandler::authenticate_client:{}".format(error)
            )


class ClientAuthentication(RequestHandler):

    def post(self, *args, **kwargs):
        try:
            post_param = json.loads(self.request.body)
            if "email" not in post_param or "password" not in post_param:
                raise KeyError

            if UserController.authenticate(post_param["email"], post_param["password"]):
                payload = {
                    "email": post_param["email"],
                    "password": post_param["password"],
                }
                response_json = {
                    "token": jwt.encode(payload, settings.JWT_SECRET, algorithm="HS512")
                }
                self.write(json.dumps(response_json))
            else:
                self.clear()
                self.set_status(403)
                self.write("<html><body>invalid credentials</body></html>")

        except Exception as error:
            logger_instance.logger.error("ClientAuthentication::post:{}".format(error))


class SuperUserAuthentication(RequestHandler):
    def post(self, *args, **kwargs):
        try:
            post_param = json.loads(self.request.body)
            if "email" not in post_param or "password" not in post_param:
                raise KeyError
            if UserController.authenticate_superuser(
                post_param["email"], post_param["password"]
            ):
                payload = {
                    "email": post_param["email"],
                    "password": post_param["password"],
                }
                response_json = {
                    "token": jwt.encode(payload, settings.JWT_SECRET, algorithm="HS512")
                }
                self.write(json.dumps(response_json))
            else:
                self.clear()
                self.set_status(403)
                self.write("<html><body>invalid credentials</body></html>")
        except Exception as error:
            logger_instance.logger.error(
                "SuperUserAuthentication::post:{}".format(error)
            )


class Machine(RequestHandler):
    """Machine class to create/list machine instances."""

    def post(self, *args, **kwargs):
        try:
            post_param = json.loads(self.request.body)
            token = self.request.headers.get("Authorization")
            if token is None:
                self.set_status(403)
                self.write("<html><body>missing token</body></html>")
                return

            token_payload = jwt.decode(token, verify=False)
            if "email" in token_payload:
                machine_id = ClientMachineController.create_client_machine(**post_param)
                if machine_id is not None:
                    self.clear()
                    self.set_status(201)
                    self.write(machine_id)
                else:
                    self.clear()
                    self.set_status(500)
                    self.write("<html><body>server error: 500</body></html>")
            else:
                self.clear()
                self.set_status(403)
                self.write("<html><body>invalid token</body></html>")

        except Exception as error:
            logger_instance.logger.error("Machine::post:{}".format(error))


class SuperUser(RequestHandler):
    """SuperUser class for frontend rendering."""

    def get(self):
        self.render("./template/superuser.html")


class User(RequestHandler):
    """User request handler class."""

    def get(self, *args, **kwargs):
        token = self.request.headers.get("Authorization")
        if not UserController.verify_superuser_token(token):
            self.clear()
            self.set_status(403)
            self.write("<html><body>invalid token</body></html>")

        filter_params = {}
        users_list = UserController.get_users(**filter_params)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(users_list))


class MainApplication(Application):

    def __init__(self):
        handlers = [
            (r"/sock_server/", SocketOutputHandler),
            (r"/get_token/", ClientAuthentication),
            (r"/get_superuser_token/", SuperUserAuthentication),
            (r"/create_machine/", Machine),
            (r"/users/", User),
            (
                r"/static/(.*)",
                StaticFileHandler,
                {"path": os.path.join(os.curdir, "static")},
            ),
            (r"/superuser/", SuperUser),
        ]

        Application.__init__(self, handlers)


def main():
    app_instance = MainApplication()
    print("[*] started socket server at 8001")
    app_instance.listen(8001, address="0.0.0.0")
    IOLoop.instance().start()


if __name__ == "__main__":
    main()
