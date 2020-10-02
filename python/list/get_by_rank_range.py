# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations as listops
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

key = (options.namespace, options.set, "list-get-by-rank-range")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

pp = pprint.PrettyPrinter(indent=2)
try:
    # create a new record with a put. list policy can't be applied outside of
    # list operations, and a new list is unordered by default
    client.put(key, {"l": [1, 4, 7, 3, 9, 9, 26, 11]})
    key, metadata, bins = client.get(key)
    print("\n{}".format(bins["l"]))
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
    pp.pprint(bins)
    # [ ('l', [7, 9, 9]), VALUE 7 is the element with rank 3, followed by 9,
    #                     which appears twice
    #   ('l', [2, 4, 5]), INDEX of the sublist returned by rank. 7 is at index
    #                     2, and the 9s appear at indexes 4 and 5
    #   ('l', [5, 3, 2]), REVERSE_INDEX displays the index of the returned
    #                     elements in the reversed index list [11,26,9,9,3,7,4,1]
    #   ('l', [3, 4, 5]), RANK of the 3 sublist elements. redundant for this operation
    #   ('l', [2, 3, 4]), REVERSE_RANK displays the rank of the sublist elements [9, 9, 7]
    #                     in the reverse rank list [26,11,9,9,7,4,3,1] **
    #   ('l', 3)]         COUNT of elements in the sublist

    # read all elements from rank -3 (third from the end in rank order)
    # and also append a new element to the end of the list
    ops = [
        listops.list_get_by_rank_range("l", -3, aerospike.LIST_RETURN_VALUE),
        listops.list_append("l", [1, 3, 3, 7, 0]),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}".format(bins[0][1]))
    # [9, 11, 26]
    print("\n{}".format(bins[2][1]))
    # [1, 4, 7, 3, 9, 9, 26, 11, [1, 3, 3, 7, 0]]

    # try to perform a list operation on an index range outside of the current list
    try:
        key, metadata, bins = client.operate(
            key,
            [listops.list_get_by_rank_range("l", 7, aerospike.LIST_RETURN_VALUE), 9],
        )
        print("\n{}".format(bins["l"]))
        # [26, [1, 3, 3, 7, 0]] - this is fine because it starts at a valid
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
        print("\n{}".format(bins[0][1]))
        # [] - still fine, but there are no elements in this range
        print("\n{}".format(bins[1][1]))
        # 0 - there are zero elements in this range
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
    print("\n{}".format(bins[0][1]))
    # [1, 3, 4, 7, 9, 11, 26, [1, 3, 3, 7, 0]]
    print("\n{}".format(bins[1][1]))
    # [9, 9, 11]
    print("\n{}".format(bins[2][1]))
    # [1, 3, 4, 7, 26, [1, 3, 3, 7, 0]]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
