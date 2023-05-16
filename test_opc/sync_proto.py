import sys
sys.path.insert(0, "../..")
import os
from dotenv import load_dotenv


from asyncua.sync import Client

if __name__ == "__main__":
    load_dotenv()
    ip_addr = os.environ.get("MACHINE_IPS").split(",")[0]
    port = 4840
    url = f"opc.tcp://{ip_addr}:4841"

    with Client(url=url) as client:
    # client = Client("opc.tcp://admin@localhost:4840/freeopcua/server/") #connect using a user

        # Client has a few methods to get proxy to UA nodes that should always be in address space such as Root or Objects
        # Node objects have methods to read and write node attributes as well as browse or populate address space
        print("Children of root are: ", client.nodes.root.get_children())

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
        myvar = client.get_node("ns=2;s=SerialNo")
        # obj = client.nodes.root.get_child(["0:Objects", "2:MyObject"])
        print("myvar is: ", myvar.read_value())
        # print("myobj is: ", obj)

        # Stacked myvar access
        # print("myvar is: ", root.get_children()[0].get_children()[1].get_variables()[0].read_value())