# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import operations
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

key = (options.namespace, options.set, "list-increment")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nincrement(bin, index, delta-value[, writeFlags, context])\n")
    # create a new record with one element by upsert
    # a list created using increment will be unordered, regardless of the
    # list order policy
    ret = client.operate(key, [list_operations.list_increment("l", 1, 2.1)])
    key, metadata, bins = client.get(key)
    print("increment('l', 1, 2.1)")
    # increment('l', 1, 2.1)
    print("{}".format(bins["l"]))
    # [None, 2.1] - Aerospike NIL represented as a Python None instance

    ops = [
        list_operations.list_set("l", 0, 1),
        list_operations.list_append_items("l", [[3, 4]]),
    ]
    client.operate(key, ops)
    key, metadata, bins = client.get(key)
    print("\nset('l', 0, 1)")
    # set('l', 0, 1)
    print("append_items('l', [[3, 4]])")
    # append_items('l', [[3, 4]])
    print("{}".format(bins["l"]))
    # [1, 2.1, [3, 4]]

    ops = [
        list_operations.list_increment("l", 0, 4),
        list_operations.list_increment("l", 1, 2),
        # the element at index 1 is 2.1. incrementing it by (integer) 2
        # will automatically type cast the delta value to 2.0
    ]
    client.operate(key, ops)
    key, metadata, bins = client.get(key)
    print("increment('l', 0, 4)")
    # increment('l', 0, 4)
    print("increment('l', 1, 2)")
    # increment('l', 1, 2)
    print("{}".format(bins["l"]))
    # [5, 4.1, [3, 4]]

    # increment the second element of the sublist at index 2
    ctx = [cdt_ctx.cdt_ctx_list_index(2)]
    client.operate(key, [list_operations.list_increment("l", 1, -2.0, ctx=ctx)])
    # the delta value was -2.0 and the element value was 4. the server type
    # cast the delta value into -2 to match its type to the type of the element
    key, metadata, bins = client.get(key)
    print("increment('l', 1, -2.0, BY_LIST_INDEX(2))")
    # increment('l', 1, -2.0, BY_LIST_INDEX(2))
    print("{}".format(bins["l"]))
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
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nset_order('l', ORDERED)")
    # set_order('l', ORDERED)
    print("read('l'): {}".format(bins[0][1]))
    # read('l'): [5, [3, 2], 4.1]
    print("increment('l', 0, 4)")
    # increment('l', 0, 4)
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [9, [3, 2], 4.1]

    # switch back to unordered and see the effect of incrementing outside the
    # bounds of the list. First with INSERT_BOUNDED and NO_FAIL
    policy = {
        "write_flags": aerospike.LIST_WRITE_INSERT_BOUNDED
        | aerospike.LIST_WRITE_NO_FAIL
    }
    ops = [
        list_operations.list_set_order("l", aerospike.LIST_UNORDERED),
        list_operations.list_increment("l", 4, 2, policy),
    ]
    client.operate(key, ops)
    print("\nset_order('l', ORDERED)")
    # set_order('l', ORDERED)
    print("increment('l', 4, 2, INSERT_BOUNDED|NO_FAIL)")
    # increment('l', 4, 2, INSERT_BOUNDED|NO_FAIL)
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [9, [3, 2], 4.1]

    # it is a bit odd to use increment with ADD_UNIQUE, but it is possible
    # here we will purposefully created another list element that is the
    # integer 9, and that should throw an error
    policy = {"write_flags": aerospike.LIST_WRITE_ADD_UNIQUE}
    ops = [list_operations.list_increment("l", 3, 9, policy)]
    try:
        print("\nincrement('l', 3, 9, ADD_UNIQUE)")
        # increment('l', 3, 9, ADD_UNIQUE)
        client.operate(key, ops)
    except ex.ElementExistsError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        # AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS [24]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
