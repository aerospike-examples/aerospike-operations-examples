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

key = (options.namespace, options.set, "list-get-by-index")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nget_by_index(bin, index[, returnType, context])\n")
    # create a new record with a put. list policy can't be applied outside of
    # list operations, and a new list is unordered by default
    client.put(key, {"l": [1, 4, 7, 3, 9, 26, 11]})
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # put('l', [1, 4, 7, 3, 9, 26, 11])
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

    print("\nget_by_index('l', 1, VALUE): {}".format(bins[0][1]))
    # get_by_index('l', 1, VALUE): 4
    print("get_by_index('l', 1, INDEX): {}".format(bins[1][1]))
    # get_by_index('l', 1, INDEX): 1
    print("get_by_index('l', 1, REVERSE_INDEX): {}".format(bins[2][1]))
    # get_by_index('l', 1, REVERSE_INDEX): 5
    print("get_by_index('l', 1, RANK): {}".format(bins[3][1]))
    # get_by_index('l', 1, RANK): 2
    print("get_by_index('l', 1, REVERSE_RANK): {}".format(bins[4][1]))
    # get_by_index('l', 1, REVERSE_RANK): 4
    print("get_by_index('l', 1, COUNT): {}".format(bins[5][1]))
    # get_by_index('l', 1, COUNT): 1

    # read the element at index -2 (second from the end) and also append a new
    # element to the end of the list
    ops = [
        list_operations.list_get_by_index("l", -2, aerospike.LIST_RETURN_VALUE),
        list_operations.list_append("l", [1, 3, 3, 7, 0]),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nget_by_index('l', -2, VALUE): {}".format(bins[0][1]))
    # get_by_index('l', -2, VALUE): 26
    print("list_append('l', [1, 3, 3, 7, 0]")
    # list_append('l', [1, 3, 3, 7, 0]
    print("read('l'): {}".format(bins[2][1]))
    # read('l'): [1, 4, 7, 3, 9, 26, 11, [1, 3, 3, 7, 0]]

    # find the reverse rank of the second element in the list nested within the
    # last element of the outer list
    ctx = [cdt_ctx.cdt_ctx_list_index(-1)]
    key, metadata, bins = client.operate(
        key,
        [list_operations.list_get_by_index("l", 1, aerospike.LIST_RETURN_REVERSE_RANK, ctx)],
    )
    print("\nget_by_index('l', 1, REVERSE_RANK, BY_LIST_INDEX(-1)): {}".format(bins["l"]))
    # get_by_index('l', 1, REVERSE_RANK, BY_LIST_INDEX(-1)): 2
    # 2 which is the third highest rank (in reverse rank order)

    # try to perform a list operation on an index outside of the current list
    try:
        key, metadata, bins = client.operate(
            key,
            [list_operations.list_get_by_index("l", 11, aerospike.LIST_RETURN_VALUE)],
        )
        print("\nget_by_index('l', 11, VALUE): {}".format(bins["l"]))
    except ex.OpNotApplicable as e:
        print("\nget_by_index('l', 11, VALUE)\nError: {0} [{1}]".format(e.msg, e.code))
        # Error: 0.0.0.0:3000 AEROSPIKE_ERR_OP_NOT_APPLICABLE [26]

    # turn the list into an ordered list, then get the elements at index 1, -1
    ops = [
        list_operations.list_set_order("l", aerospike.LIST_ORDERED),
        list_operations.list_get_by_index("l", 1, aerospike.LIST_RETURN_VALUE),
        list_operations.list_get_by_index("l", -1, aerospike.LIST_RETURN_VALUE),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nset_order('l', ORDERED)")
    # set_order('l', ORDERED)
    print("get_by_index('l', 1, VALUE): {}".format(bins[0][1]))
    # get_by_index('l', 1, VALUE): 3
    # second lowest index in the ordered list is the second lowest rank (3)
    print("get_by_index('l', -1, VALUE): {}".format(bins[1][1]))
    # get_by_index('l', -1, VALUE): [1, 3, 3, 7, 0]
    # highest index in the ordered list is [1, 3, 3, 7, 0], which has the
    # highest rank. lists rank higher than all integers, based on the ordering
    # rules: https://www.aerospike.com/docs/guide/cdt-ordering.html

    # get the entire ordered list
    key, metadata, bins = client.get(key)
    print("\nGet the full list: {}".format(bins["l"]))
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
    print("\nset_order('l', ORDERED, BY_LIST_INDEX(-1))")
    print("get_by_index('l', -1, VALUE, BY_LIST_INDEX(-1)): {}".format(bins["l"]))
    # set_order('l', ORDERED, BY_LIST_INDEX(-1))
    # get_by_index('l', -1, VALUE, BY_LIST_INDEX(-1)): 7
    # 7 is the highest index in the inner list (and highest rank), now
    # that it is ordered

    key, metadata, bins = client.get(key)
    print("\nGet the full list: {}".format(bins["l"]))
    # [1, 3, 4, 7, 9, 11, 26, [0, 1, 3, 3, 7]]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
