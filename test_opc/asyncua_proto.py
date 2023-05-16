import asyncio
import os
import logging
import json
from asyncua import Client, Node, ua
from time import sleep
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('asyncua')


class SubHandler(object):
    """
    The SubscriptionHandler is used to handle the data that is received for the subscription.
    """
    def __init__(self):
        self.events = dict()
        # self.event = threading.Event()

    def datachange_notification(self, node: Node, val, data):
        """
        Callback for asyncua Subscription.
        This method will be called when the Client received a data change message from the Server.
        """
        _logger.info('datachange_notification %r %s', node, val)
        print("data change happened")

    def event_notification(self, event):
        if event.MessageId and isinstance(event.MessageId, int):
            self.events[event.MessageId] = event
            print("Python: New event", event)
            # self.event.set()
            # logger.trace("Python: New event", event)


async def main():
    """
    Main task of this Client-Subscription example.
    """
    load_dotenv()
    ip_addr = os.environ.get("MACHINE_IPS").split(",")[0]
    port = 4840
    url = f"opc.tcp://{ip_addr}:4841"

    async with Client(url=url) as client:
        async def get_event_filter() -> ua.EventFilter:
            # get the node id of the custom event type
            eventtype_id = f"ns=5;i=4200"
            literal_node_id = client.get_node(eventtype_id)
            # operand based on the node id
            operand = ua.LiteralOperand()
            operand.Value = ua.Variant(Value=literal_node_id.nodeid)
            # content filter element ties operand and operator together
            content_filter_element = ua.ContentFilterElement()
            content_filter_element.FilterOperator = ua.FilterOperator.OfType
            content_filter_element.FilterOperands = [operand]
            # a content filter has a list of elements that make up the filter conditional statement
            content_filter = ua.ContentFilter()
            content_filter.Elements = [content_filter_element]
            # select element specifies the desired fields to retrieve using node browse path(s)
            select_element = ua.SimpleAttributeOperand()
            eventtype_id_props = await literal_node_id.get_properties()
            """ the following changed from get_browse_name to read_browse_name """
            msg_id_browse_name = await eventtype_id_props[0].read_browse_name()
            select_element.BrowsePath = [msg_id_browse_name]
            # tell the server to return the value attribute instead of the node id (which is the default)
            select_element.AttributeId = ua.AttributeIds.Value
            # event filter ties content filter and select element together
            event_filter = ua.EventFilter()
            event_filter.SelectClauses = [select_element]
            event_filter.WhereClause = content_filter
            return event_filter
        
        # single tag val
        myvar = client.get_node("ns=2;s=SerialNo")
        print("***************************\n\n", await myvar.read_value(), "\n\n*************************")

        # CREATE HANDLER, SUBSCRIPTION WITH EVENT FILTER
        gany_event = await client.nodes.root.get_child(["0:Objects", "5:Ganymede", "5:GanymedeEvent", "5:GanymedeEventType"])
        handler = SubHandler()
        sub = await client.create_subscription(500, handler)
        filter = await get_event_filter()
        ev_handle = await sub.subscribe_events(evfilter=filter)

        # SEND MESSAGE
        methodNode = client.get_node("ns=5;s=SendMessage")
        rqNode = await methodNode.get_parent()
        msg = '{"cmd":"getResult","resultId":1}'
        response_str = await rqNode.call_method(
            methodNode, ua.Variant(msg, ua.VariantType.String)
        )

        # GET MESSAGE ID 
        response = json.loads(response_str, strict=False)
        message_id = response["messageId"]

        # WAIT FOR HANDLER TO SAY THAT MESSAGE ID IS AVAILABLE
        while not handler.events.get(message_id):
             print(message_id, handler.events.get(message_id))
             await asyncio.sleep(1)

        # REQUEST MESSAGE WITH MESSAGE id
        methodNode = client.get_node("ns=5;s=RequestMessage")
        rqNode = await methodNode.get_parent()

        response_str = await rqNode.call_method(methodNode, ua.Variant(message_id, ua.VariantType.UInt32))
        response = json.loads(response_str, strict=False)
        print("************************\n\n", response, "\n\n**********************")


if __name__ == "__main__":
    asyncio.run(main())