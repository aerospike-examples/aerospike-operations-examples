# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations
import sys

if options.set == "None":
    options.set = None

config = {
    "hosts": [(options.host, options.port)],
    "policies": {
        "operate": {"key": aerospike.POLICY_KEY_SEND},
        "read": {"key": aerospike.POLICY_KEY_SEND},
    },
}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

key = (options.namespace, options.set, "list-set_order")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nset_order(bin, order[, context])\n")
    # create a record with an unordered list
    client.put(key, {"l": [4, 5, 8, 1, 2, [3, 2], 9, 6]})
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [4, 5, 8, 1, 2, [3, 2], 9, 6]

    # set the inner list (at index 5) to ORDERED
    ctx = [cdt_ctx.cdt_ctx_list_index(5)]
    client.operate(
        key, [list_operations.list_set_order("l", aerospike.LIST_ORDERED, ctx)]
    )
    key, metadata, bins = client.get(key)
    print("\nset_order('l', ORDERED, BY_LIST_INDEX(5))\n{}".format(bins["l"]))
    # set_order('l', ORDERED, BY_LIST_INDEX(5))
    # [4, 5, 8, 1, 2, [2, 3], 9, 6]

    # set the outer list to ORDERED
    client.operate(key, [list_operations.list_set_order("l", aerospike.LIST_ORDERED)])
    key, metadata, bins = client.get(key)
    print("\nset_order('l', ORDERED)\n{} ".format(bins["l"]))
    # [1, 2, 4, 5, 6, 8, 9, [2, 3]]
    # note that list ordering puts integers elements before list elements
    # see https://www.aerospike.com/docs/guide/cdt-ordering.html
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
