# -*- coding: utf-8 -*-
from __future__ import print_function
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations
import sys

if options.set == "None":
    options.set = None

config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

key = (options.namespace, options.set, "op-sort")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    # create a record with an unordered list
    client.put(key, {"l": [1, 2, [3, 4]]})
    k, m, b = client.get(key)
    print("{}".format(b["l"]))
    # [1, 2, [3, 4]]

    k, m, b = client.operate(key, [list_operations.list_size("l")])
    print("\nThe size of the list is {}".format(b["l"]))
    # The size of the list is 3

    # get the size of the inner list (at index 2)
    ctx = [
        cdt_ctx.cdt_ctx_list_index(2)
    ]
    k, m, b = client.operate(key, [list_operations.list_size("l",ctx=ctx)])
    print("\nThe size of the sub-list is {}".format(b["l"]))
    # The size of the sub-list is 2
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
