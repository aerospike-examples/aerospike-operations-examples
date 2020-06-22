# -*- coding: utf-8 -*-
from __future__ import print_function
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import operations
import sys

if options.set == "None":
    options.set = None

config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

key = (options.namespace, options.set, "op-increment")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    # create a new record with one element by upsert
    # a list created using increment will be unordered, regardless of the
    # list order policy
    ret = client.operate(key, [list_operations.list_increment("l", 1, 2.1)])
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [None, 2.1] - Aerospike NIL represented as a Python None instance

    ops = [
        list_operations.list_set("l", 0, 1),
        list_operations.list_append_items("l", [[3, 4]]),
    ]
    client.operate(key, ops)
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [1, 2.1, [3, 4]]

    ops = [
        list_operations.list_increment("l", 0, 4),
        list_operations.list_increment("l", 1, 2),
        # the element at index 1 is 2.1. incrementing it by (integer) 2
        # will automatically type cast the delta value to 2.0
    ]
    client.operate(key, ops)
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [5, 4.1, [3, 4]]

    # increment the second element of the sublist at index 2
    ctx = [cdt_ctx.cdt_ctx_list_index(2)]
    client.operate(key, [list_operations.list_increment("l", 1, -2.0, ctx=ctx)])
    # the delta value was -2.0 and the element value was 4. the server type
    # cast the delta value into -2 to match its type to the type of the element
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [5, 4.1, [3, 2]]

    # increment cannot be used on ordered lists
    # change the list order then increment
    ops = [
        list_operations.list_set_order("l", aerospike.LIST_ORDERED),
        # [5, [3, 2], 4.1]
        # see https://www.aerospike.com/docs/guide/cdt-ordering.html
        operations.read("l"),
        list_operations.list_increment("l", 0, 4)
        # [9, [3, 2], 4.1]
    ]
    k, m, b = client.operate_ordered(key, ops)
    print("\nChanging to an ordered list:\n{}".format(b[0][1]))
    # [5, [3, 2], 4.1]
    k, m, b = client.get(key)
    print("\nAfter incrementing the 0th index the list re-sorts as:\n{}".format(b["l"]))
    # [9, [3, 2], 4.1]

    # switch back to unordered and see the effect of incrementing outside the
    # bounds of the list. First with INSERT_BOUNDED and NO_FAIL
    policy = {
        "write_flags": aerospike.LIST_WRITE_INSERT_BOUNDED
        | aerospike.LIST_WRITE_NO_FAIL
    }
    ops = [
        list_operations.list_set_order("l", aerospike.LIST_UNORDERED),
        list_operations.list_insert("l", 4, 2, policy),
    ]
    client.operate(key, ops)
    print("\nGracefully fails with INSERT_BOUNDED an NO_FAIL")
    k, m, b = client.get(key)
    print("{}".format(b["l"]))

    # it is a bit odd to use increment with ADD_UNIQUE, but it is possible
    # here we will purposefully created another list element that is the
    # integer 9, and that should throw an error
    policy = {"write_flags": aerospike.LIST_WRITE_ADD_UNIQUE}
    ops = [list_operations.list_insert("l", 3, 9, policy)]
    try:
        client.operate(key, ops)
    except ex.ElementExistsError as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))
        # AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS [24]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
