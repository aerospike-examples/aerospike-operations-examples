# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations as listops
from aerospike_helpers.operations import operations
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

key = (options.namespace, options.set, "list-get-by-rank")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nget_by_rank(bin, rank[, returnType, context])\n")
    # create a new record with a put. list policy can't be applied outside of
    # list operations, and a new list is unordered by default
    client.put(key, {"l": [1, 4, 7, 3, 9, 26, 11]})
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [1, 4, 7, 3, 9, 26, 11]

    # demonstrate the meaning of the different return types
    # for the list datatype read operations, by getting the element with rank 1
    # multiple times in the same transaction
    ops = [
        listops.list_get_by_rank("l", 1, aerospike.LIST_RETURN_VALUE),
        listops.list_get_by_rank("l", 1, aerospike.LIST_RETURN_INDEX),
        listops.list_get_by_rank("l", 1, aerospike.LIST_RETURN_REVERSE_INDEX),
        listops.list_get_by_rank("l", 1, aerospike.LIST_RETURN_RANK),
        listops.list_get_by_rank("l", 1, aerospike.LIST_RETURN_REVERSE_RANK),
        listops.list_get_by_rank("l", 1, aerospike.LIST_RETURN_COUNT),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    # in the python client the operate() command returns the result of the last
    # operation on a specific bin, so using operate_ordered instead, which
    # gives the results as ordered (bin-name, result) tuples

    print("\nget_by_rank('l', 1, VALUE): {}".format(bins[0][1]))
    # get_by_rank('l', 1, VALUE): 3

    print("get_by_rank('l', 1, INDEX): {}".format(bins[1][1]))
    # get_by_rank('l', 1, INDEX): 3

    print("get_by_rank('l', 1, REVERSE_INDEX): {}".format(bins[2][1]))
    # get_by_rank('l', 1, REVERSE_INDEX): 3

    print("get_by_rank('l', 1, RANK): {}".format(bins[3][1]))
    # get_by_rank('l', 1, RANK): 1

    print("get_by_rank('l', 1, REVERSE_RANK): {}".format(bins[4][1]))
    # get_by_rank('l', 1, REVERSE_RANK): 5

    print("get_by_rank('l', 1, COUNT): {}".format(bins[5][1]))
    # get_by_rank('l', 1, COUNT): 1

    # get the element with rank -2LUE (second highest) and also append a new
    # element to the end of the list
    ops = [
        listops.list_get_by_rank("l", -2, aerospike.LIST_RETURN_VALUE),
        listops.list_append("l", [1, 3, 3, 7]),
        operations.read("l"),
    ]

    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nget_by_rank('l', -2, VALUE): {}".format(bins[0][1]))
    # get_by_rank('l', -2, VALUE): 11
    print("list_append('l', [1, 3, 3, 7])")
    # list_append('l', [1, 3, 3, 7])
    print("read('l'): {}".format(bins[2][1]))
    # read('l'): [1, 4, 7, 3, 9, 26, 11, [1, 3, 3, 7]]

    # find the index position of the element with the second lowest rank
    # in the list nested within the last element of the outer list
    # also find the reverse index position of an element with the same rank
    ctx = [cdt_ctx.cdt_ctx_list_index(-1)]
    ops = [
        listops.list_get_by_rank("l", 1, aerospike.LIST_RETURN_INDEX, ctx),
        listops.list_get_by_rank("l", 1, aerospike.LIST_RETURN_REVERSE_INDEX, ctx),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nget_by_rank('l', 1, INDEX, BY_LIST_INDEX(-1)): {}".format(bins[0][1]))
    # get_by_rank('l', 1, INDEX, BY_LIST_INDEX(-1)): 1
    # 1, which is the position of the first appearance of the value 3
    print(
        "get_by_rank('l', 1, REVERSE_INDEX, BY_LIST_INDEX(-1)): {}".format(bins[1][1])
    )
    # get_by_rank('l', 1, REVERSE_INDEX, BY_LIST_INDEX(-1)): 2
    # 2, which is the position of the first appearance of the value 3 when
    # searched from the end to the left (reverse index, AKA -2)

    # try to perform a list operation on a rank larger than the number of ranks
    # for all the list elements
    try:
        key, metadata, bins = client.operate(
            key, [listops.list_get_by_rank("l", 11, aerospike.LIST_RETURN_VALUE)]
        )
        print("\n{}".format(bins["l"]))
    except ex.OpNotApplicable as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))
        # Error: 0.0.0.0:3000 AEROSPIKE_ERR_OP_NOT_APPLICABLE [26]

    # turn the list into an ordered list, then get the elements at rank 1, -1
    ops = [
        listops.list_set_order("l", aerospike.LIST_ORDERED),
        listops.list_get_by_rank("l", 1, aerospike.LIST_RETURN_INDEX),
        listops.list_get_by_rank("l", -1, aerospike.LIST_RETURN_INDEX),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nset_order('l', ORDERED)")
    # set_order('l', ORDERED)
    print("get_by_rank('l', 1, INDEX): {}".format(bins[0][1]))
    # get_by_rank('l', 1, INDEX): 1
    # the index of the second lowest ranked element is 1
    print("get_by_rank('l', -11, INDEX): {}".format(bins[1][1]))
    # get_by_rank('l', -11, INDEX): 7
    # the index of the highest ranked element is 7

    # get the entire ordered list
    key, metadata, bins = client.get(key)
    print("\nGet the list: {}".format(bins["l"]))
    # Get the list: [1, 3, 4, 7, 9, 11, 26, [1, 3, 3, 7]]
    # notice that the index and rank positions of the ordered list are the same

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
