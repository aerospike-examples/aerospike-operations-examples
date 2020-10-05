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
    },
}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

key = (options.namespace, options.set, "list-get-by-rank-range")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nget_by_rank_range(bin, rank[, returnType, count, context])\n")

    # create a new record with a put. list policy can't be applied outside of
    # list operations, and a new list is unordered by default
    client.put(key, {"l": [1, 4, 7, 3, 9, 9, 26, 11]})
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [1, 4, 7, 3, 9, 9, 26, 11]

    # demonstrate the meaning of the different return types
    # for the list data type read operations, by getting a range of 3 elements
    # starting with rank 3, multiple times in the same transaction
    ops = [
        listops.list_get_by_rank_range("l", 3, aerospike.LIST_RETURN_VALUE, 3),
        listops.list_get_by_rank_range("l", 3, aerospike.LIST_RETURN_INDEX, 3),
        listops.list_get_by_rank_range("l", 3, aerospike.LIST_RETURN_REVERSE_INDEX, 3),
        listops.list_get_by_rank_range("l", 3, aerospike.LIST_RETURN_RANK, 3),
        listops.list_get_by_rank_range("l", 3, aerospike.LIST_RETURN_REVERSE_RANK, 3),
        listops.list_get_by_rank_range("l", 3, aerospike.LIST_RETURN_COUNT, 3),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    # in the python client the operate() command returns the result of the last
    # operation on a specific bin, so using operate_ordered instead, which
    # gives the results as ordered (bin-name, result) tuples

    print("\nget_by_rank_range('l', 3, VALUE, 3): {}".format(bins[0][1]))
    # get_by_rank_range('l', 3, VALUE, 3): [7, 9, 9]

    print("get_by_rank_range('l', 3, INDEX, 3): {}".format(bins[1][1]))
    # get_by_rank_range('l', 3, INDEX, 3): [2, 4, 5]

    print("get_by_rank_range('l', 3, REVERSE_INDEX, 3): {}".format(bins[2][1]))
    # get_by_rank_range('l', 3, REVERSE_INDEX, 3): [5, 3, 2]

    print("get_by_rank_range('l', 3, RANK, 3): {}".format(bins[3][1]))
    # get_by_rank_range('l', 3, RANK, 3): [3, 4, 5]

    print("get_by_rank_range('l', 3, REVERSE_RANK, 3): {}".format(bins[4][1]))
    # get_by_rank_range('l', 3, REVERSE_RANK, 3): [2, 3, 4]

    print("get_by_rank_range('l', 3, COUNT, 3): {}".format(bins[5][1]))
    # get_by_rank_range('l', 3, COUNT, 3): 3

    # read all elements from rank -3 (third from the end in rank order)
    # and also append a new element to the end of the list
    ops = [
        listops.list_get_by_rank_range("l", -3, aerospike.LIST_RETURN_VALUE),
        listops.list_append("l", [1, 3, 3, 7, 0]),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nget_by_rank_range('l', -3, VALUE): {}".format(bins[0][1]))
    # get_by_rank_range('l', -3, VALUE): [9, 11, 26]
    print("list_append('l', [1, 3, 3, 7, 0])")
    # list_append('l', [1, 3, 3, 7, 0])
    print("read('l'): {}".format(bins[2][1]))
    # read('l'): [1, 4, 7, 3, 9, 9, 26, 11, [1, 3, 3, 7, 0]]

    # try to perform a list operation on an index range outside of the current list
    try:
        key, metadata, bins = client.operate(
            key,
            [listops.list_get_by_rank_range("l", 7, aerospike.LIST_RETURN_VALUE), 9],
        )
        print("\nget_by_rank_range('l', 7, VALUE, 9): {}".format(bins["l"]))
        # get_by_rank_range('l', 7, VALUE, 9): [26, [1, 3, 3, 7, 0]]
        # this is fine because it starts at a valid
        # index, but there are only 2 elements to return, not 9.
    except ex.OpNotApplicable as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))

    # try again starting outside of the current list
    try:
        ops = [
            listops.list_get_by_rank_range("l", 9, aerospike.LIST_RETURN_VALUE, 3),
            listops.list_get_by_rank_range("l", 9, aerospike.LIST_RETURN_COUNT, 3),
        ]
        key, metadata, bins = client.operate_ordered(key, ops)
        print("\nget_by_rank_range('l', 9, VALUE, 3): {}".format(bins[0][1]))
        # get_by_rank_range('l', 9, VALUE, 3): []
        # still fine, but there are no elements in this range
        print("get_by_rank_range('l', 9, COUNT, 3): {}".format(bins[1][1]))
        # get_by_rank_range('l', 9, COUNT, 3): 0
        # there are zero elements in this range
    except ex.OpNotApplicable as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))

    # turn into an ordered list, then get a range of 3 elements starting at rank -5
    ops = [
        listops.list_set_order("l", aerospike.LIST_ORDERED),
        operations.read("l"),
        listops.list_get_by_rank_range("l", -5, aerospike.LIST_RETURN_VALUE, 3),
        listops.list_get_by_rank_range("l", -5, aerospike.LIST_RETURN_VALUE, 3,
            inverted=True),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nset_order('l', ORDERED))")
    # set_order('l', ORDERED))
    print("read('l'): {}".format(bins[0][1]))
    # read('l'): [1, 3, 4, 7, 9, 9, 11, 26, [1, 3, 3, 7, 0]]
    print("get_by_rank_range('l', -5, VALUE, 3): {}".format(bins[1][1]))
    # get_by_rank_range('l', -5, VALUE, 3): [9, 9, 11]
    print("get_by_rank_range('l', -5, INVERTED|VALUE, 3): {}".format(bins[2][1]))
    # get_by_rank_range('l', -5, INVERTED|VALUE, 3): [1, 3, 4, 7, 26, [1, 3, 3, 7, 0]]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
