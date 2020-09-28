# -*- coding: utf-8 -*-
from __future__ import print_function
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import operations
import pprint
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

key = (options.namespace, options.set, "list-get-by-index")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

pp = pprint.PrettyPrinter(indent=2)
try:
    # create a new record with a put. list policy can't be applied outside of
    # list operations, and a new list is unordered by default
    client.put(key, {"l": [1, 4, 7, 3, 9, 26, 11]})
    key, metadata, bins = client.get(key)
    print("\n{}".format(bins["l"]))
    # [1, 4, 7, 3, 9, 26, 11]

    # demonstrate the meaning of the different return types
    # for the list datatype read operations, by getting the element at index 1
    # of the list multiple times in the same transaction
    ops = [
        list_operations.list_get_by_index("l", 1, aerospike.LIST_RETURN_VALUE),
        list_operations.list_get_by_index("l", 1, aerospike.LIST_RETURN_INDEX),
        list_operations.list_get_by_index("l", 1, aerospike.LIST_RETURN_REVERSE_INDEX),
        list_operations.list_get_by_index("l", 1, aerospike.LIST_RETURN_RANK),
        list_operations.list_get_by_index("l", 1, aerospike.LIST_RETURN_REVERSE_RANK),
        list_operations.list_get_by_index("l", 1, aerospike.LIST_RETURN_COUNT),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    # in the python client the operate() command returns the result of the last
    # operation on a specific bin, so using operate_ordered instead, which
    # gives the results as ordered (bin-name, result) tuples
    pp.pprint(bins)
    # [ ('l', 4), VALUE at index 1 is 4
    #   ('l', 1), INDEX at index 1 is 1. redundant for this operation
    #   ('l', 5), REVERSE_INDEX the index position of the VALUE (4) in
    #             [11,26,9,3,7,4,1] is 5
    #   ('l', 2), RANK of the VALUE is 2 (the 3rd lowest) in [1,3,4,7,9,11,26]
    #   ('l', 4), REVERSE_RANK of the VALUE (4) in 4 [26,11,9,7,4,3,1] is 4
    #   ('l', 1)] COUNT of elements returned is 1. redundant for this operation

    # read the element at index -2 (second from the end) and also append a new
    # element to the end of the list
    ops = [
        list_operations.list_get_by_index("l", -2, aerospike.LIST_RETURN_VALUE),
        list_operations.list_append("l", [1, 3, 3, 7, 0]),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}".format(bins[0][1]))
    # 26
    print("\n{}".format(bins[2][1]))
    # [1, 4, 7, 3, 9, 26, 11, [1, 3, 3, 7, 0]]

    # find the reverse rank of the second element in the list nested within the
    # last element of the outer list
    ctx = [cdt_ctx.cdt_ctx_list_index(-1)]
    key, metadata, bins = client.operate(
        key,
        [list_operations.list_get_by_index("l", 1, aerospike.LIST_RETURN_RANK, ctx)],
    )
    print("\n{}".format(bins["l"]))
    # 1 which is the second highest rank (in reverse rank order)
    # element at index -2 would also have a rank of 1, as both have the value 3

    # try to perform a list operation on an index outside of the current list
    try:
        key, metadata, bins = client.operate(
            key,
            [list_operations.list_get_by_index("l", 11, aerospike.LIST_RETURN_VALUE)],
        )
        print("\n{}".format(bins["l"]))
    except ex.OpNotApplicable as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))
        # Error: 0.0.0.0:3000 AEROSPIKE_ERR_OP_NOT_APPLICABLE [26]

    # turn the list into an ordered list, then get the elements at index 1, -1
    ops = [
        list_operations.list_set_order("l", aerospike.LIST_ORDERED),
        list_operations.list_get_by_index("l", 1, aerospike.LIST_RETURN_VALUE),
        list_operations.list_get_by_index("l", -1, aerospike.LIST_RETURN_VALUE),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}".format(bins[0][1]))
    # second lowest index in the ordered list is 3
    print("\n{}".format(bins[1][1]))
    # highest index in the ordered list is [1, 3, 3, 7, 0]

    # get the entire ordered list
    key, metadata, bins = client.get(key)
    print("\n{}".format(bins["l"]))
    # [1, 3, 4, 7, 9, 11, 26, [1, 3, 3, 7, 0]]
    # notice that the set_order applied to the outer list, and did not modify
    # the order of the inner list. by the order commparison rules a list is
    # higher than integers.

    # to order the inner list, we need to apply set_order with a context
    # identifying the list element
    ctx = [cdt_ctx.cdt_ctx_list_index(-1)]
    ops = [
        list_operations.list_set_order("l", aerospike.LIST_ORDERED, ctx=ctx),
        list_operations.list_get_by_index("l", -1, aerospike.LIST_RETURN_VALUE, ctx),
    ]
    key, metadata, bins = client.operate(key, ops)
    print("\n{}".format(bins["l"]))
    # 7 is the highest index in the inner list, now that it is ordered

    key, metadata, bins = client.get(key)
    print("\n{}".format(bins["l"]))
    # [1, 3, 4, 7, 9, 11, 26, [0, 1, 3, 3, 7]]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
