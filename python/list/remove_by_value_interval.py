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

key = (options.namespace, options.set, "list-remove-by-value-interval")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nremove_by_value_interval(bin, valueStart, valueStop[, returnType, context])\n")
    # set the list, remove_by_value_interval('l', 2, VALUE, 4)
    ops = [
        operations.write("l", [1, 2, 3, 4, 5, 4, 3, 2, 1]),
        operations.read("l"),
        listops.list_remove_by_value_range("l", aerospike.LIST_RETURN_VALUE, 2, 4),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_value_interval('l', 2, VALUE, 4): {}\n{}".format(bins[0][1], bins[1][1], bins[2][1]))
    # [1, 2, 3, 4, 5, 4, 3, 2, 1]
    # remove_by_value_interval('l', 2, VALUE, 4): [2, 3, 3, 2]
    # [1, 4, 5, 4, 1]

    # set the list, remove_by_value_interval('l', 2, INDEX, 4)
    ops = [
        operations.write("l", [1, 2, 3, 4, 5, 4, 3, 2, 1]),
        operations.read("l"),
        listops.list_remove_by_value_range("l", aerospike.LIST_RETURN_INDEX, 2, 4),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_value_interval('l', 2, INDEX, 4): {}\n{}".format(bins[0][1], bins[1][1], bins[2][1]))
    # [1, 2, 3, 4, 5, 4, 3, 2, 1]
    # remove_by_value_interval('l', 2, INDEX, 4): [1, 2, 6, 7]
    # [1, 4, 5, 4, 1]

    # set the list, remove_by_value_interval('l', 2, REVERSE_INDEX, 4)
    ops = [
        operations.write("l", [1, 2, 3, 4, 5, 4, 3, 2, 1]),
        operations.read("l"),
        listops.list_remove_by_value_range("l", aerospike.LIST_RETURN_REVERSE_INDEX, 2, 4),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_value_interval('l', 2, REVERSE_INDEX, 4): {}\n{}".format(bins[0][1], bins[1][1], bins[2][1]))
    # [1, 2, 3, 4, 5, 4, 3, 2, 1]
    # remove_by_value_interval('l', 2, REVERSE_INDEX, 4): [7, 6, 2, 1]
    # [1, 4, 5, 4, 1]

    # set the list, remove_by_value_interval('l', 2, RANK, 4)
    ops = [
        operations.write("l", [1, 2, 3, 4, 5, 4, 3, 2, 1]),
        operations.read("l"),
        listops.list_remove_by_value_range("l", aerospike.LIST_RETURN_RANK, 2, 4),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_value_interval('l', 2, RANK, 4): {}\n{}".format(bins[0][1], bins[1][1], bins[2][1]))
    # [1, 2, 3, 4, 5, 4, 3, 2, 1]
    # remove_by_value_interval(RANK, 2, 4): [2, 3, 4, 5]
    # [1, 4, 5, 4, 1]

    # set the list, remove_by_value_interval('l', 2, REVERSE_RANK, 4)
    ops = [
        operations.write("l", [1, 2, 3, 4, 5, 4, 3, 2, 1]),
        operations.read("l"),
        listops.list_remove_by_value_range("l", aerospike.LIST_RETURN_REVERSE_RANK, 2, 4),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_value_interval('l', 2, REVERSE_RANK, 4): {}\n{}".format(bins[0][1], bins[1][1], bins[2][1]))
    # [1, 2, 3, 4, 5, 4, 3, 2, 1]
    # remove_by_value_interval(REVERSE_RANK, 2, 4): [3, 4, 5, 6]
    # [1, 4, 5, 4, 1]

    # set the list, remove_by_value_interval('l', 2, NONE, 4)
    ops = [
        operations.write("l", [1, 2, 3, 4, 5, 4, 3, 2, 1]),
        operations.read("l"),
        listops.list_remove_by_value_range("l", aerospike.LIST_RETURN_NONE, 2, 4),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_value_interval('l', 2, NONE, 4)\n{}".format(bins[0][1], bins[1][1]))
    # [1, 2, 3, 4, 5, 4, 3, 2, 1]
    # remove_by_value_interval('l', 2, NONE, 4)
    # [1, 4, 5, 4, 1]

    # switch to more complex example using a list of [epoch-timestamp, temp] tuples
    tuples = [
        [20000, 39.04],
        [21001, 39.78],
        [23003, 40.89],
        [24004, 40.93],
        [25005, 41.18],
        [26006, 40.07],
    ]
    nil = aerospike.null()
    infinite = aerospike.CDTInfinite()

    ops = [
        operations.write("l", tuples),
        operations.read("l"),
        listops.list_remove_by_value_range(
            "l", aerospike.LIST_RETURN_VALUE, [23003, nil], [25005, nil]
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_interval('l', [23003, NIL], [25005, NIL], VALUE): {}\n{}".format(
            bins[0][1], bins[1][1], bins[2][1]
        )
    )
    # [[20000, 39.04], [21001, 39.78], [23003, 40.89], [24004, 40.93], [25005, 41.18], [26006, 40.07]]
    # remove_by_value_interval('l', [23003, NIL], [25005, NIL], VALUE): [[23003, 40.89], [24004, 40.93]]
    # [[20000, 39.04], [21001, 39.78], [25005, 41.18], [26006, 40.07]]

    ops = [
        operations.write("l", tuples),
        operations.read("l"),
        listops.list_remove_by_value_range(
            "l", aerospike.LIST_RETURN_VALUE, [23003, nil], [25005, infinite]
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_interval('l', [23003, NIL], [25005, INF], VALUE): {}\n{}".format(
            bins[0][1], bins[1][1], bins[2][1]
        )
    )
    # [[20000, 39.04], [21001, 39.78], [23003, 40.89], [24004, 40.93], [25005, 41.18], [26006, 40.07]]
    # remove_by_value_interval('l', [23003, NIL], [25005, INF], VALUE): [[23003, 40.89], [24004, 40.93], [25005, 41.18]]
    # [[20000, 39.04], [21001, 39.78], [26006, 40.07]]

    ops = [
        operations.write("l", tuples),
        operations.read("l"),
        listops.list_remove_by_value_range(
            "l", aerospike.LIST_RETURN_VALUE, [23003, infinite], [25005, infinite]
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_interval('l', [23003, INF], [25005, INF], VALUE): {}\n{}".format(
            bins[0][1], bins[1][1], bins[2][1]
        )
    )
    # [[20000, 39.04], [21001, 39.78], [23003, 40.89], [24004, 40.93], [25005, 41.18], [26006, 40.07]]
    # remove_by_value_interval('l', [23003, INF], [25005, INF], VALUE): [[24004, 40.93], [25005, 41.18]]
    # [[20000, 39.04], [21001, 39.78], [23003, 40.89], [26006, 40.07]]

    # this is because lists are compared first by index, then by number of elements.
    # NIL has a lower order than float values, and infinite has higher order than all
    # values, according to the ordering rules
    #
    # NIL < (Boolean) < Integer < String < List < Map < Bytes < Double < GeoJSON < INF
    #
    # See: https://www.aerospike.com/docs/guide/cdt-ordering.html

    # demonstrate an inverted interval removal
    ops = [
        operations.write("l", tuples),
        operations.read("l"),
        listops.list_remove_by_value_range(
            "l", aerospike.LIST_RETURN_NONE, [23003, nil], [25005, infinite],
            inverted=True
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nremove_by_value_interval('l', [23003, NIL], [25005, INF], INVERTED|NONE)\n{}".format(
            bins[0][1], bins[1][1]
        )
    )
    # [[20000, 39.04], [21001, 39.78], [23003, 40.89], [24004, 40.93], [25005, 41.18], [26006, 40.07]]
    # remove_by_value_interval('l', [23003, NIL], [25005, INF], INVERTED|NONE)
    # [[23003, 40.89], [24004, 40.93], [25005, 41.18]]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
