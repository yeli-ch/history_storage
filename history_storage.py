
import zmq
import psycopg2 as pg
from common.config import Config
from common.activity_history import ActivityHistory, InvalidActivityException


class HistoryStorage:

    configs = [
        {
            "long": "--server",
            "short": "-s",
            "action": "store",
            "type": str,
            "help": "Hostname of the PostgreSQL server."
        },
        {
            "long": "--port",
            "short": "-p",
            "action": "store",
            "type": int,
            "help": "Port of the PostgreSQL server."
        },
        {
            "long": "--user",
            "short": "-u",
            "action": "store",
            "type": str,
            "help": "Username of the PostgreSQL user."
        },
        {
            "long": "--name",
            "short": "-n",
            "action": "store",
            "type": str,
            "help": "Name of the PostgreSQL database."
        },
        {
            "long": "--password",
            "short": "-w",
            "action": "store",
            "type": str,
            "help": "Password of the PostgreSQL database."
        }
    ]

    def __init__(self):
        self.config = Config("Yeli History Storage",
                             "Stores the activity history for the yeli bot.",
                             self.configs)

        self.history = ActivityHistory(host = self.config.server,
                                       port = self.config.port,
                                       dbname = self.config.name,
                                       user = self.config.user,
                                       password = self.config.password)
        self.zmq_context = zmq.Context.instance()
        self.zmq_in = self.zmq_context.socket(zmq.PULL)
        self.zmq_out = self.zmq_context.socket(zmq.PUB)

    def poll(self):
        self.zmq_in.bind("tcp://localhost:8001")
        self.zmq_out.bind("tcp://*:8001")
        try:
            while True:
                activity = self.zmq_in.recv_json()
                if self.history.is_activity_new(activity):
                    self.history.insert_activity(activity)
                    self.zmq_out.send_json(activity)
        except KeyboardInterrupt:
            pass
