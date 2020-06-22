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

key = (options.namespace, options.set, "op-set_order")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    # create a record with an unordered list
    client.put(key, {"l": [4, 5, 8, 1, 2, [3, 2], 9, 6]})
    k, m, b = client.get(key)
    print("{}".format(b["l"]))
    # [4, 5, 8, 1, 2, [3, 2], 9, 6]

    # set the inner list (at index 5) to ORDERED
    ctx = [cdt_ctx.cdt_ctx_list_index(5)]
    client.operate(
        key, [list_operations.list_set_order("l", aerospike.LIST_ORDERED, ctx)]
    )
    k, m, b = client.get(key)
    print("{}".format(b["l"]))
    # [4, 5, 8, 1, 2, [2, 3], 9, 6]

    # set the outer list to ORDERED
    client.operate(key, [list_operations.list_set_order("l", aerospike.LIST_ORDERED)])
    k, m, b = client.get(key)
    print("{}".format(b["l"]))
    # [1, 2, 4, 5, 6, 8, 9, [2, 3]]
    # note that list ordering puts integers elements before list elements
    # see https://www.aerospike.com/docs/guide/cdt-ordering.html
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
