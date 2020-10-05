# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations as lops
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

key = (options.namespace, options.set, "list-get-by-value-rel-rank-range")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nget_by_value_rel_rank_range(bin, value, rank, returnType[, count])\n")

    ops = [
        operations.write("l", [2, 4, 7, 3, 8, 9, 26, 11]),
        operations.read("l"),
        lops.list_get_by_value_rank_range_relative(
            "l", 5, -2, aerospike.LIST_RETURN_VALUE, 3
        ),
        lops.list_get_by_value_rank_range_relative(
            "l", 5, -2, aerospike.LIST_RETURN_INDEX, 3
        ),
        lops.list_get_by_value_rank_range_relative(
            "l", 5, -2, aerospike.LIST_RETURN_REVERSE_INDEX, 3
        ),
        lops.list_get_by_value_rank_range_relative(
            "l", 5, -2, aerospike.LIST_RETURN_RANK, 3
        ),
        lops.list_get_by_value_rank_range_relative(
            "l", 5, -2, aerospike.LIST_RETURN_REVERSE_RANK, 3
        ),
        lops.list_get_by_value_rank_range_relative(
            "l", 5, -2, aerospike.LIST_RETURN_COUNT, 3
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    # in the python client the operate() command returns the result of the last
    # operation on a specific bin, so using operate_ordered instead, which
    # gives the results as ordered (bin-name, result) tuples

    # the operation itself starts with the rank (in this case -2) relative to the
    # value (in this example 5) and then returns _count_ elements (3 in this
    # example)
    #
    # demonstrate the meaning of the different return types
    # for this operation, but repeating it multiple times.
    #
    # notice that there is no value 5. the operation finds the rank relative to
    # it, if 5 _was_ in the list. -2 from 5 we get to 3, and then a count of
    # three elements from there would be 3, 4, and 7
    print("{}".format(bins[0][1]))
    # [2, 4, 7, 3, 8, 9, 26, 11]
    print("get_by_value_rel_rank_range('l', 5, -1, VALUE, 3): {}".format(bins[1][1]))
    # get_by_value_rel_rank_range('l', 5, -1, VALUE, 3): [7, 4, 3]
    # you can see that elements in the range don't necessarily return in index
    # order
    print("get_by_value_rel_rank_range('l', 5, -1, INDEX, 3): {}".format(bins[2][1]))
    # get_by_value_rel_rank_range('l', 5, -1, INDEX, 3): [1, 2, 3] which
    # corresponds to elements 4, 7 and 3
    print(
        "get_by_value_rel_rank_range('l', 5, -1, REVERSE_INDEX, 3): {}".format(
            bins[3][1]
        )
    )
    # get_by_value_rel_rank_range('l', 5, -1, REVERSE_INDEX, 3): [6, 5, 4],
    # which is the reverse index positions of 4, 3, and 7
    print("get_by_value_rel_rank_range('l', 5, -1, RANK, 3): {}".format(bins[4][1]))
    # get_by_value_rel_rank_range('l', 5, -1, RANK, 3): [1, 2, 3], which is the
    # rank positions of 4, 3 and 7
    print(
        "get_by_value_rel_rank_range('l', 5, -1, REVERSE_RANK, 3): {}".format(
            bins[5][1]
        )
    )
    # get_by_value_rel_rank_range('l', 5, -1, REVERSE_RANK, 3): [4, 5, 6],
    # which is the reverse rank positions of 4,3 and 7
    print("get_by_value_rel_rank_range('l', 5, -1, COUNT, 3): {}".format(bins[6][1]))
    # get_by_value_rel_rank_range('l', 5, -1, COUNT, 3): 3

    # perform the operation on an value outside of this list
    try:
        key, metadata, bins = client.operate(
            key,
            [
                lops.list_get_by_value_rank_range_relative(
                    "l", 99, -1, aerospike.LIST_RETURN_VALUE, 4
                )
            ],
        )
        print(
            "\nget_by_value_rel_rank_range('l', 99, -1, VALUE, 4): {}".format(bins["l"])
        )
        # get_by_value_rel_rank_range('l', 99, -1, VALUE, 4): [26]
    except ex.OpNotApplicable as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))

    # example with tuple values
    tuples = [["z", 26], ["d", 4], ["b", 2], ["a", 1], ["e", 3], ["b", 2]]
    nil = aerospike.null()
    ops = [
        operations.write("l", tuples),
        operations.read("l"),
        lops.list_get_by_value_rank_range_relative(
            "l", ["c", nil], -1, aerospike.LIST_RETURN_VALUE, 2
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}".format(bins[0][1]))
    # [['z', 26], ['d', 4], ['b', 2], ['a', 1], ['e', 3], ['b', 2]]
    print(
        "get_by_value_rel_rank_range('l', ['c', NIL], -1, VALUE, 2): {}".format(
            bins[1][1]
        )
    )
    # get_by_value_rel_rank_range('l', ['c', NIL], -1, VALUE, 2): [['d', 4], ['b', 2]]
    # The first element to the left of ['c', NIL] is ['b', 2] and from there
    # the operation returns 2 elements by rank

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
