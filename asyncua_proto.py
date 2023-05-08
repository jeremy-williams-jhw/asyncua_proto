import asyncio
import logging
import json
from asyncua import Client, Node, ua

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('asyncua')


class SubscriptionHandler:
    """
    The SubscriptionHandler is used to handle the data that is received for the subscription.
    """
    def datachange_notification(self, node: Node, val, data):
        """
        Callback for asyncua Subscription.
        This method will be called when the Client received a data change message from the Server.
        """
        _logger.info('datachange_notification %r %s', node, val)


async def main():
    """
    Main task of this Client-Subscription example.
    """
    client = Client(url='opc.tcp://10.50.24.239:4840')

    def get_event_filter() -> ua.EventFilter:
        # get the node id of the custom event type
        eventtype_id = f"ns=2;i=4200"
        literal_node_id = client.get_node(eventtype_id)
        # operand based on the node id
        operand = ua.LiteralOperand()
        operand.Value = ua.Variant(value=literal_node_id.nodeid)
        # content filter element ties operand and operator together
        content_filter_element = ua.ContentFilterElement()
        content_filter_element.FilterOperator = ua.FilterOperator.OfType
        content_filter_element.FilterOperands = [operand]
        # a content filter has a list of elements that make up the filter conditional statement
        content_filter = ua.ContentFilter()
        content_filter.Elements = [content_filter_element]
        # select element specifies the desired fields to retrieve using node browse path(s)
        select_element = ua.SimpleAttributeOperand()
        eventtype_id_props = client.get_node(eventtype_id).get_properties()
        msg_id_browse_name = eventtype_id_props[0].get_browse_name()
        select_element.BrowsePath = [msg_id_browse_name]
        # tell the server to return the value attribute instead of the node id (which is the default)
        select_element.AttributeId = ua.AttributeIds.Value
        # event filter ties content filter and select element together
        event_filter = ua.EventFilter()
        event_filter.SelectClauses = [select_element]
        event_filter.WhereClause = content_filter
        return event_filter

    async with client:
        # CREATE HANDLER, SUBSCRIPTION WITH EVENT FILTER
        handler = SubscriptionHandler()
        # We create a Client Subscription.
        subscription = await client.create_subscription(500, handler)
        subscription.subscribe_events(evfilter=get_event_filter())

        # SEND MESSAGE
        methodNode = client.get_node("ns=5;s=SendMessage")
        msg = '{"cmd":"getResult","resultId":1}'
        response_str = await rqNode.call_method(
            methodNode, ua.Variant(msg, ua.VariantType.String)
        )

        # GET MESSAGE ID 
        response = json.loads(response_str, strict=False)
        message_id = response["messageId"]

        # WAIT FOR HANDLER TO SAY THAT MESSAGE ID IS AVAILABLE
        response_ready = await handler.events.get(message_id)

        # REQUEST MESSAGE WITH MESSAGE id

        methodNode = client.get_node("ns=5;s=RequestMessage")
        rqNode = methodNode.get_parent()

        response_str = await rqNode.call_method(
            methodNode, ua.Variant(message_id, ua.VariantType.String)
        )
        response = json.loads(response_str, strict=False)
        print(response)

        await subscription.unsubscribe(handler)
        await subscription.delete()


if __name__ == "__main__":
    asyncio.run(main())