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
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

key = (options.namespace, options.set, "list-append-items")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nappend_items(bin, items[, writeFlags, context])\n")
    # create a new record by upsert
    # by default a newly created list is unordered
    client.operate(key, [list_operations.list_append_items("l", [1, 2])])
    key, metadata, bins = client.get(key)
    print("\nappend_items('l', [1, 2]): {}".format(bins["l"]))
    # append_items('l', [1, 2]): [1, 2]

    # append elements, one of which repeats, with an ADD_UNIQUE and NO_FAIL
    # write flag. this should fail gracefully
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_NO_FAIL,
        "list_order": aerospike.LIST_UNORDERED,
    }
    client.operate(key, [list_operations.list_append_items("l", [1, 3], policy)])
    print("\nappend('l', [1, 3], ADD_UNIQUE|NO_FAIL)")
    print("Gracefully failed on a uniqueness violation")

    # adding the DO_PARTIAL policy should allow the non-repeating element to
    # be appended
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE
        | aerospike.LIST_WRITE_NO_FAIL
        | aerospike.LIST_WRITE_PARTIAL
    }
    client.operate(key, [list_operations.list_append_items("l", [1, [3]], policy)])
    key, metadata, bins = client.get(key)
    print("\nappend('l', [1, [3]], ADD_UNIQUE|DO_PARTIAL|NO_FAIL): {}".format(bins["l"]))
    # append('l', [1, 3], ADD_UNIQUE|DO_PARTIAL|NO_FAIL): [1, 2, [3]]

    # append items to the sub list at index 2
    ctx = [cdt_ctx.cdt_ctx_list_index(2)]
    ret = client.operate(key, [list_operations.list_append_items("l", [4, 5], ctx=ctx)])
    key, metadata, bins = client.get(key)
    print("\nappend('l', [4,5], context=BY_LIST_INDEX(2)): {}".format(bins["l"]))
    # append('l', [4,5] context=BY_LIST_INDEX(2)): [1, 2, [3,  4, 5]]
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
