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

key = (options.namespace, options.set, "list-remove-by-rank-range")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    # create a new record with a put. list policy can't be applied outside of
    # list operations, and a new list is unordered by default
    client.put(key, {"l": [1, 12, 13, 5, 4, 6, 8, 7, 9, 11, 10, 2, 3]})
    key, metadata, bins = client.get(key)
    print("\n{}".format(bins["l"]))
    # [1, 12, 13, 5, 4, 6, 8, 7, 9, 11, 10, 2, 3]

    # demonstrate the meaning of the different return types
    ops = [
        listops.list_remove_by_rank_range("l", 6, aerospike.LIST_RETURN_VALUE, 3),
        operations.read("l"),
        listops.list_remove_by_rank_range("l", 3, aerospike.LIST_RETURN_COUNT, 3),
        operations.read("l"),
        listops.list_remove_by_rank_range("l", 0, aerospike.LIST_RETURN_NONE, 3),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    # in the python client the operate() command returns the result of the last
    # operation on a specific bin, so using operate_ordered instead, which
    # gives the results as ordered (bin-name, result) tuples
    print(
        "\nlist remove_by_rank_range(VALUE, 6, 3)\nReturned: {}\nRemaining: {}".format(
            bins[0][1], bins[1][1]
        )
    )
    # list_remove_by_rank_range(VALUE, 6, 3)
    # Returned: [7, 8, 9]
    # Remaining: [1, 12, 13, 5, 4, 6, 11, 10, 2, 3]
    print(
        "\nlist remove_by_rank_range(COUNT, 3, 3)\nReturned: {}\nRemaining: {}".format(
            bins[2][1], bins[3][1]
        )
    )
    # list_remove_by_rank_range(COUNT, 3, 3)
    # Returned: 3
    # Remaining: [1, 12, 13, 11, 10, 2, 3]
    print("\nlist remove_by_rank_range(NONE, 0, 3)\nRemaining: {}".format(bins[4][1]))
    # list_remove_by_rank_range(NONE, 0, 3)
    # Remaining: [12, 13, 11, 10]

    # remove the top 2 ranked elements and also append a new
    # element to the end of the list
    ops = [
        listops.list_remove_by_rank_range("l", -2, aerospike.LIST_RETURN_NONE),
        listops.list_append("l", [1, 3, 3, 7, 0]),  # returns new list length
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}".format(bins[1][1]))
    # [11, 10, [1, 3, 3, 7, 0]]

    # remove all but the top 2 ranked elements from the list nested within the
    # last element of the outer list
    ctx = [cdt_ctx.cdt_ctx_list_index(-1)]
    ops = [
        listops.list_remove_by_rank_range(
            "l", -2, aerospike.LIST_RETURN_NONE, inverted=True, ctx=ctx
        ),
        listops.list_get_by_rank("l", -1, aerospike.LIST_RETURN_VALUE),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nInner list after the operation: {}".format(bins[0][1]))
    # Inner list after the operation: [3, 7]

    # try to perform a list operation on a rank range outside of the current list
    try:
        key, metadata, bins = client.operate(
            key,
            [
                listops.list_remove_by_rank_range("l", 3, aerospike.LIST_RETURN_NONE),
                2,
                operations.read("l"),
            ],
        )
        print("\nRemaining: {}".format(bins["l"]))
        # [11, 10, [3, 7]]
    except ex.OpNotApplicable as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))

    # turn into an ordered list, then remove the range of elements starting at
    # rank -2. this is the same as saying removing the top two ranked elements
    ops = [
        listops.list_set_order("l", aerospike.LIST_ORDERED),
        operations.read("l"),
        listops.list_remove_by_rank_range("l", -2, aerospike.LIST_RETURN_VALUE),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\nAfter becoming an ordered list: {}\nRemoved: {}\nRemaining: {}".format(
            bins[0][1], bins[1][1], bins[2][1]
        )
    )
    # Removed: [11, [3, 7]]
    # Remaining: [10]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
