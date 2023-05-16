import asyncio
import logging
import json
from asyncua import Client, Node, ua
from time import sleep
import threading

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

    async with Client(url='opc.tcp://10.50.24.239:4841') as client:
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

        # CREATE HANDLER, SUBSCRIPTION WITH EVENT FILTER
        # obj = await client.nodes.root.get_child(["0:Objects", "5:Ganymede"])
        # print("obj>>>>>>>>>>>>>>>>> ", obj)
        gany_event = await client.nodes.root.get_child(["0:Objects", "5:Ganymede", "5:GanymedeEvent", "5:GanymedeEventType"])
        # life_beat_event = await client.nodes.root.get_child(["0:Objects", "2:Automation", "2:LifeBeat"])
        # print("my event>>>>>>>>>>>>>>>>>>>> ", gany_event)
        handler = SubHandler()
        sub = await client.create_subscription(500, handler)
        filter = await get_event_filter()
        # dc_handle = await sub.subscribe_data_change(gany_event)
        ev_handle = await sub.subscribe_events(evfilter=filter)#myevent)

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
        # TODO: How to make this async?
        # response_ready = await handler.events.get(message_id)
        while not handler.events:
             print(message_id, handler.events.get(message_id))
             sleep(1)

        # REQUEST MESSAGE WITH MESSAGE id
        methodNode = client.get_node("ns=5;s=RequestMessage")
        rqNode = await methodNode.get_parent()

        response_str = await rqNode.call_method(methodNode, ua.Variant(message_id, ua.VariantType.String))
        response = json.loads(response_str, strict=False)
        print(response)

        await sub.unsubscribe(handler)
        await sub.delete()


if __name__ == "__main__":
    asyncio.run(main())