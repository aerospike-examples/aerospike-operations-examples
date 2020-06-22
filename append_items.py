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

key = (options.namespace, options.set, "op-append-items")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    # create a new record by upsert
    # by default a newly created list is unordered
    client.operate(key, [list_operations.list_append_items("l", [1, 2])])
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [1, 2]

    # append elements, one of which repeats, with an ADD_UNIQUE and NO_FAIL
    # write flag. this should fail gracefully
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_NO_FAIL,
        "list_order": aerospike.LIST_UNORDERED,
    }
    client.operate(key, [list_operations.list_append_items("l", [1, 3], policy)])
    print("\nGracefully failed on a uniqueness violation")

    # adding the DO_PARTIAL policy should allow the non-repeating element to
    # be appended
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_NO_FAIL | aerospike.LIST_WRITE_PARTIAL
    }
    client.operate(key, [list_operations.list_append_items("l", [1, [3]], policy)])
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [1, 2, [3]]

    # append items to the sub list at index 2
    ctx = [cdt_ctx.cdt_ctx_list_index(2)]
    ret = client.operate(key, [list_operations.list_append_items("l", [4, 5], ctx=ctx)])
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [1, 2, [3,  4, 5]]
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
