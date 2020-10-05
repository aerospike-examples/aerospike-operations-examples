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

key = (options.namespace, options.set, "list-append")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nappend(bin, value[, writeFlags, context])\n")
    # create a new record with one element by upsert
    # by default a newly created list is unordered
    ret = client.operate(key, [list_operations.list_append("l", 1)])
    key, metadata, bins = client.get(key)
    print("append('l', 1): {}".format(bins["l"]))
    # [1]

    # append the same element with an ADD_UNIQUE and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_NO_FAIL,
        "list_order": aerospike.LIST_UNORDERED,
    }
    ret = client.operate(key, [list_operations.list_append("l", 1, policy)])
    print("\nappend('l', 1, ADD_UNIQUE|NO_FAIL)")
    print("The number of elements in the list is {}".format(ret[2]["l"]))
    # The number of elements in the list is 1

    # append a different element with an ADD_UNIQUE and NO_FAIL write flags
    # this should work
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_NO_FAIL,
        "list_order": aerospike.LIST_UNORDERED,
    }
    ret = client.operate(key, [list_operations.list_append("l", [2], policy)])
    key, metadata, bins = client.get(key)
    print("\nappend('l', [2], ADD_UNIQUE|NO_FAIL): {}".format(bins["l"]))
    # append('l', 1, ADD_UNIQUE|NO_FAIL):[1, [2]]

    # append to the list element at index 1
    ctx = [cdt_ctx.cdt_ctx_list_index(1)]
    ret = client.operate(key, [list_operations.list_append("l", 3, ctx=ctx)])
    key, metadata, bins = client.get(key)
    print("\nappend('l', 3 context=BY_LIST_INDEX(1)): {}".format(bins["l"]))
    # append('l', 3, context=BY_LIST_INDEX(1)): [1, [2, 3]]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
