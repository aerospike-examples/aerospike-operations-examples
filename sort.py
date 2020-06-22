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
    client.put(key, {"l": [5,1,8,2,7,[3,2,4,1],9,6,1,2]})
    k, m, b = client.get(key)
    print("{}".format(b["l"]))
    # [5, 1, 8, 2, 7, [3, 2, 4, 1], 9, 6, 1, 2]

    # sort the inner list (at index 5)
    ctx = [
        cdt_ctx.cdt_ctx_list_index(5)
    ]
    client.operate(key, [list_operations.list_sort("l",ctx=ctx)])
    k, m, b = client.get(key)
    print("{}".format(b["l"]))
    # [5, 1, 8, 2, 7, [1, 2, 3, 4], 9, 6, 1, 2]

    # sort the outer list and drop duplicates
    client.operate(key, [list_operations.list_sort("l", aerospike.LIST_SORT_DROP_DUPLICATES)])
    k, m, b = client.get(key)
    print("{}".format(b["l"]))
    # [1, 2, 5, 6, 7, 8, 9, [1, 2, 3, 4]]
    # note that list ordering puts integers elements before list elements
    # see https://www.aerospike.com/docs/guide/cdt-ordering.html
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
