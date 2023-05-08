from opc_client import UaClient
import logging
import sys
import os
from dotenv import load_dotenv

def get_log():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger



logger = get_log()

class EventHandler(object):
    def __init__(self):
        self.events = dict()
        self.notifications = dict()

    def datachange_notification(self, node, val, data):
        logger.info(f">>>>data change >>>>>>>>>>>>>>>>>>>>:{node}, {val}")

    def event_notification(self, event):
        if event.MessageId and isinstance(event.MessageId, int):
            self.events[event.MessageId] = event
            logger.info(f">>>>event >>>>>>>>>>>>>>>>>>>>:{event}")


def connect(url):
    c = UaClient()
    c.connect(url)
    return c


#ua.ObjectIds.BaseEventType

def subscribe(c, node_id):
    node = c.get_node(node_id)
    handler = EventHandler()
    c.subscribe_events(node, handler, 2000)


if __name__ == '__main__':
    load_dotenv()
    ip_addr = os.environ.get("MACHINE_IPS").split(",")[0]
    port = 4840
    url = f"opc.tcp://{ip_addr}:4840"
    c = connect(url)
    node_id = 'ns=2;s=LifeBeat'
    subscribe(c, node_id)