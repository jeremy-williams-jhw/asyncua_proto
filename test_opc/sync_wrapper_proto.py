import sys
import os
from dotenv import load_dotenv
import json
from time import sleep

from asyncua.sync import Client, ThreadLoop, ua


class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    Do not do expensive, slow or network operation there. Create another
    thread if you need to do such a thing
    """
    def __init__(self):
        self.events = dict()

    def datachange_notification(self, node, val, data):
        print("Python: New data change event", node, val)

    def event_notification(self, event):
        self.events[event.MessageId] = event
        print("Python: New event", event)


def get_event_filter(client) -> ua.EventFilter:
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
    eventtype_id_props = literal_node_id.get_properties()
    """ the following changed from get_browse_name to read_browse_name """
    msg_id_browse_name = eventtype_id_props[0].read_browse_name()
    select_element.BrowsePath = [msg_id_browse_name]
    # tell the server to return the value attribute instead of the node id (which is the default)
    select_element.AttributeId = ua.AttributeIds.Value
    # event filter ties content filter and select element together
    event_filter = ua.EventFilter()
    event_filter.SelectClauses = [select_element]
    event_filter.WhereClause = content_filter
    return event_filter


def get_single_tag_val(client):
    # Client has a few methods to get proxy to UA nodes that should always be in address space such as Root or Objects
    # Node objects have methods to read and write node attributes as well as browse or populate address space
    # print("Children of root are: ", client.nodes.root.get_children())

    # get a specific node knowing its node id
    #var = client.get_node(ua.NodeId(1002, 2))
    #var = client.get_node("ns=3;i=2002")
    #print(var)
    #var.read_data_value() # get value of node as a DataValue object
    #var.read_value() # get value of node as a python builtin
    #var.write_value(ua.Variant([23], ua.VariantType.Int64)) #set node value using explicit data type
    #var.write_value(3.9) # set node value using implicit data type

    # Now getting a variable node using its browse path
    # myvar = client.nodes.root.get_child(["0:Objects", "2:Automation", "2:SerialNo"])
    # print("myobj is: ", obj)

    myvar = client.get_node("ns=2;s=SerialNo")
    # obj = client.nodes.root.get_child(["0:Objects", "2:MyObject"])
    return myvar.read_value()

def get_event_val(client):
    handler = SubHandler()
    sub = client.create_subscription(500, handler)
    filter = get_event_filter(client)
    sub.subscribe_events(evfilter=filter)

    # SEND MESSAGE
    methodNode = client.get_node("ns=5;s=SendMessage")
    rqNode = methodNode.get_parent()
    msg = '{"cmd":"getResult","resultId":1}'
    response_str = rqNode.call_method(
        methodNode, ua.Variant(msg, ua.VariantType.String)
    )

    # GET MESSAGE ID 
    response = json.loads(response_str, strict=False)
    message_id = response["messageId"]

    while not handler.events.get(message_id):
        sleep(.05)

    # REQUEST MESSAGE WITH MESSAGE id
    methodNode = client.get_node("ns=5;s=RequestMessage")
    rqNode = methodNode.get_parent()

    response_str = rqNode.call_method(methodNode, ua.Variant(message_id, ua.VariantType.UInt32))
    return json.loads(response_str, strict=False)


if __name__ == "__main__":
    load_dotenv()
    ip_addr = os.environ.get("MACHINE_IPS").split(",")[0]
    port = 4840
    url = f"opc.tcp://{ip_addr}:4841"

    with ThreadLoop() as tloop:
        with Client(url=url) as client:
            """ **** PROTOTYPE 1 - SINGLE VALUE **** """
            print(
                get_single_tag_val(client)
            )

            """ **** PROTOTYPE 2 - EVENT SUBSCRIPTION **** """
            print(
                get_event_val(client)
            )