from opc_client import UaClient, ua
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


def get_value(ns, s, client):
    result = None
    try:
        uac = ua.ReadParameters()
        nodeid = ua.NodeId.from_string(f"ns={ns};s={s}")
        attr = ua.ReadValueId()
        attr.NodeId = nodeid
        attr.AttributeId = ua.AttributeIds.Value
        uac.NodesToRead.append(attr)

        result = client.read(uac)
    except Exception as ex:
        logger.warning(
            "Error in getting value {0} with error {1}{2}".format(s, ex, ex.args)
        )
    # we cannot return a specific type (like '' or 0) because the value that is returned here could be any data type relative to the corresponding tag in opcua server
    return None if not result else result[0].Value.Value

#ua.ObjectIds.BaseEventType

def subscribe(c, node_id):
    node = c.get_node(node_id)
    handler = EventHandler()
    c.subscribe_events(node, handler, 2000)


if __name__ == '__main__':
    load_dotenv()
    ip_addr = os.environ.get("MACHINE_IPS").split(",")[0]
    port = 4840
    url = f"opc.tcp://{ip_addr}:4841"
    c = connect(url)
    node_id = 'ns=2;s=LifeBeat'
    # subscribe(c, node_id)
    print(
        # get_value(2, 'CalDate', c)
        c.nodes.root.get_child(["0:Objects", "2:Automation", "2:CalDate"])
    )

    i = 0