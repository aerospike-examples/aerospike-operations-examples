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
    print("\nsize(bin[, context])\n")
    # create a record with an unordered list
    client.put(key, {"l": [1, 2, [3, 4]]})
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [1, 2, [3, 4]]

    key, metadata, bins = client.operate(key, [listops.list_size("l")])
    print("\nsize('l'): {}".format(bins["l"]))
    # The size of the list is 3

    # get the size of the inner list (at index 2)
    ctx = [cdt_ctx.cdt_ctx_list_index(2)]
    key, metadata, bins = client.operate(key, [listops.list_size("l", ctx=ctx)])
    print("\nsize('l', BY_LIST_INDEX(4)): {}".format(bins["l"]))
    # The size of the sub-list is 2
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
