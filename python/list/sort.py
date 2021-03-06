# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations as listops
import sys

if options.set == "None":
    options.set = None

config = {
    "hosts": [(options.host, options.port)],
    "policies": {
        "operate": {"key": aerospike.POLICY_KEY_SEND},
        "read": {"key": aerospike.POLICY_KEY_SEND},
        "write": {"key": aerospike.POLICY_KEY_SEND},
    },
}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

key = (options.namespace, options.set, "list-sort")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nsort(bin[, sortFlags, context])\n")
    # create a record with an unordered list
    client.put(key, {"l": [5, 1, 8, 2, 7, [3, 2, 4, 1], 9, 6, 1, 2]})
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [5, 1, 8, 2, 7, [3, 2, 4, 1], 9, 6, 1, 2]

    # sort the inner list (at index 5)
    ctx = [cdt_ctx.cdt_ctx_list_index(5)]
    client.operate(key, [listops.list_sort("l", ctx=ctx)])
    key, metadata, bins = client.get(key)
    print("sort('l', ctx=BIN_LIST_INDEX(5))\n{}".format(bins["l"]))
    # [5, 1, 8, 2, 7, [1, 2, 3, 4], 9, 6, 1, 2]

    # sort the outer list and drop duplicates
    client.operate(
        key, [listops.list_sort("l", aerospike.LIST_SORT_DROP_DUPLICATES)]
    )
    key, metadata, bins = client.get(key)
    print("\nsort('l', DROP_DUPLICATES)\n{}".format(bins["l"]))
    # [1, 2, 5, 6, 7, 8, 9, [1, 2, 3, 4]]
    # note that list ordering puts integers elements before list elements
    # see https://www.aerospike.com/docs/guide/cdt-ordering.html
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
