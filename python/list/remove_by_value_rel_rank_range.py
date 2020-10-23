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
        "write": {"key": aerospike.POLICY_KEY_SEND},
    },
}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

key = (options.namespace, options.set, "list-remove-by-value-rel-rank-range")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nremove_by_value_rel_rank_range(bin, value, rank, returnType[, count])\n")

    ops = [
        operations.write("l", [2, 4, 7, 3, 8, 9, 26, 11]),
        operations.read("l"),
        lops.list_remove_by_value_rank_range_relative(
            "l", 5, -1, aerospike.LIST_RETURN_VALUE, 2
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_rel_rank_range('l', 5, -1, VALUE, 2): {}\n{}".format(
            bins[0][1], bins[1][1], bins[2][1]
        )
    )
    # [2, 4, 7, 3, 8, 9, 26, 11]
    # remove_by_value_rel_rank_range('l', 5, -1, VALUE, 2): [7, 4]
    # [2, 3, 8, 9, 26, 11]

    ops = [
        operations.write("l", [2, 4, 7, 3, 8, 9, 26, 11]),
        operations.read("l"),
        lops.list_remove_by_value_rank_range_relative(
            "l", 5, -1, aerospike.LIST_RETURN_INDEX, 2
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_rel_rank_range('l', 5, -1, INDEX, 2): {}\n{}".format(
            bins[0][1], bins[1][1], bins[2][1]
        )
    )
    # [2, 4, 7, 3, 8, 9, 26, 11]
    # remove_by_value_rel_rank_range('l', 5, -1, INDEX, 2): [1, 2]
    # [2, 3, 8, 9, 26, 11]

    ops = [
        operations.write("l", [2, 4, 7, 3, 8, 9, 26, 11]),
        operations.read("l"),
        lops.list_remove_by_value_rank_range_relative(
            "l", 5, -1, aerospike.LIST_RETURN_REVERSE_INDEX, 2
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_rel_rank_range('l', 5, -1, REVERSE_INDEX, 2): {}\n{}".format(
            bins[0][1], bins[1][1], bins[2][1]
        )
    )
    # [2, 4, 7, 3, 8, 9, 26, 11]
    # remove_by_value_rel_rank_range('l', 5, -1, REVERSE_INDEX, 2): [6, 5]
    # [2, 3, 8, 9, 26, 11]

    ops = [
        operations.write("l", [2, 4, 7, 3, 8, 9, 26, 11]),
        operations.read("l"),
        lops.list_remove_by_value_rank_range_relative(
            "l", 5, -1, aerospike.LIST_RETURN_RANK, 2
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_rel_rank_range('l', 5, -1, RANK, 2): {}\n{}".format(
            bins[0][1], bins[1][1], bins[2][1]
        )
    )
    # [2, 4, 7, 3, 8, 9, 26, 11]
    # remove_by_value_rel_rank_range('l', 5, -1, RANK, 2): [2, 3]
    # [2, 3, 8, 9, 26, 11]

    ops = [
        operations.write("l", [2, 4, 7, 3, 8, 9, 26, 11]),
        operations.read("l"),
        lops.list_remove_by_value_rank_range_relative(
            "l", 5, -1, aerospike.LIST_RETURN_REVERSE_RANK, 2
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_rel_rank_range('l', 5, -1, REVERSE_RANK, 2): {}\n{}".format(
            bins[0][1], bins[1][1], bins[2][1]
        )
    )
    # [2, 4, 7, 3, 8, 9, 26, 11]
    # remove_by_value_rel_rank_range('l', 5, -1, REVERSE_RANK, 2): [4, 5]
    # [2, 3, 8, 9, 26, 11]

    ops = [
        operations.write("l", [2, 4, 7, 3, 8, 9, 26, 11]),
        operations.read("l"),
        lops.list_remove_by_value_rank_range_relative(
            "l", 5, -1, aerospike.LIST_RETURN_COUNT, 2
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_rel_rank_range('l', 5, -1, REVERSE_COUNT, 2): {}\n{}".format(
            bins[0][1], bins[1][1], bins[2][1]
        )
    )
    # [2, 4, 7, 3, 8, 9, 26, 11]
    # remove_by_value_rel_rank_range('l', 5, -1, REVERSE_COUNT, 2): 2
    # [2, 3, 8, 9, 26, 11]

    ops = [
        operations.write("l", [2, 4, 7, 3, 8, 9, 26, 11]),
        operations.read("l"),
        lops.list_remove_by_value_rank_range_relative(
            "l", 5, -1, aerospike.LIST_RETURN_NONE, 2
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_rel_rank_range('l', 5, -1, REVERSE_NONE, 2)\n{}".format(
            bins[0][1], bins[1][1]
        )
    )
    # [2, 4, 7, 3, 8, 9, 26, 11]
    # remove_by_value_rel_rank_range('l', 5, -1, NONE, 2)
    # [2, 3, 8, 9, 26, 11]

    # perform the operation on an value outside of this list
    try:
        key, metadata, bins = client.operate(
            key,
            [
                lops.list_remove_by_value_rank_range_relative(
                    "l", 99, -1, aerospike.LIST_RETURN_VALUE, 4
                )
            ],
        )
        print(
            "\nremove_by_value_rel_rank_range('l', 99, -1, VALUE, 4): {}".format(
                bins["l"]
            )
        )
        # remove_by_value_rel_rank_range('l', 99, -1, VALUE, 4): [26]
    except ex.OpNotApplicable as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))

    # example with tuple values
    tuples = [["z", 26], ["d", 4], ["b", 2], ["a", 1], ["e", 3], ["b", 2]]
    nil = aerospike.null()
    ops = [
        operations.write("l", tuples),
        operations.read("l"),
        lops.list_remove_by_value_rank_range_relative(
            "l", ["c", nil], -1, aerospike.LIST_RETURN_NONE, 2, inverted=True
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}".format(bins[0][1]))
    # [['z', 26], ['d', 4], ['b', 2], ['a', 1], ['e', 3], ['b', 2]]
    print(
        "remove_by_value_rel_rank_range('l', ['c', NIL], -1, INVERTED|NONE, 2)\n{}".format(
            bins[1][1]
        )
    )
    # remove_by_value_rel_rank_range('l', ['c', NIL], -1, INVERTED|NONE, 2)
    # [['d', 4], ['b', 2]]
    # The first element to the left of ['c', NIL] is ['b', 2] and from there
    # the operation removes all but the 2 elements by rank

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
