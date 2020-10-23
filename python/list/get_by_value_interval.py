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

key = (options.namespace, options.set, "list-get-by-value-interval")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nget_by_value_interval(bin, valueStart, valueStop[, returnType, context])\n")
    # demonstrate the meaning of the different return types for the
    # list data type read operations, by getting all elements whose value
    # is in the interval [2, 4) multiple times in the same transaction
    ops = [
        operations.write("l", [1, 2, 3, 4, 5, 4, 3, 2, 1]),
        listops.list_get_by_value_range("l", aerospike.LIST_RETURN_VALUE, 2, 4),
        listops.list_get_by_value_range("l", aerospike.LIST_RETURN_INDEX, 2, 4),
        listops.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_REVERSE_INDEX, 2, 4
        ),
        listops.list_get_by_value_range("l", aerospike.LIST_RETURN_RANK, 2, 4),
        listops.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_REVERSE_RANK, 2, 4
        ),
        listops.list_get_by_value_range("l", aerospike.LIST_RETURN_COUNT, 2, 4),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    # in the python client the operate() command returns the result of the last
    # operation on a specific bin, so using operate_ordered instead, which
    # gives the results as ordered (bin-name, result) tuples
    print("[1, 2, 3, 4, 5, 4, 3, 2, 1]")
    # [1, 2, 3, 4, 5, 4, 3, 2, 1]
    print("get_by_value_interval('l', 2, 4, VALUE): {}".format(bins[0][1]))
    # get_by_value_interval('l', 2, 4, VALUE): [2, 3, 3, 2]
    print("get_by_value_interval('l', 2, 4, INDEX): {}".format(bins[1][1]))
    # get_by_value_interval('l', 2, 4, INDEX): [1, 2, 6, 7]
    print("get_by_value_interval('l', 2, 4, REVERSE_INDEX): {}".format(bins[2][1]))
    # get_by_value_interval('l', 2, 4, REVERSE_INDEX): [7, 6, 2, 1]
    print("get_by_value_interval('l', 2, 4, RANK): {}".format(bins[3][1]))
    # get_by_value_interval('l', 2, 4, RANK): [2, 3, 4, 5]
    print("get_by_value_interval('l', 2, 4, REVERSE_RANK): {}".format(bins[4][1]))
    # get_by_value_interval('l', 2, 4, REVERSE_RANK): [3, 4, 5, 6]
    print("get_by_value_interval('l', 2, 4, COUNT): {}".format(bins[5][1]))
    # get_by_value_interval('l', 2, 4, COUNT): 4

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
        listops.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_VALUE, [23003, nil], [25005, nil]
        ),
        listops.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_VALUE, [23003, nil], [25005, infinite]
        ),
        listops.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_VALUE, [23003, infinite], [25005, infinite]
        ),
        listops.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_VALUE, [23003, "rick"], [25005, "morty"]
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}".format(bins[0][1]))
    # [[20000, 39.04], [21001, 39.78], [23003, 40.89], [24004, 40.93], [25005, 41.18], [26006, 40.07]]
    print(
        "get_by_value_interval('l', [23003, NIL], [25005, NIL], VALUE): {}".format(
            bins[1][1]
        )
    )
    # get_by_value_interval('l', [23003, NIL], [25005, NIL], VALUE): [[23003, 40.89], [24004, 40.93]]
    print(
        "get_by_value_interval('l', [23003, NIL], [25005, NIL], VALUE): {}".format(
            bins[1][1]
        )
    )
    # get_by_value_interval('l', [23003, NIL], [25005, NIL], VALUE): [[23003, 40.89], [24004, 40.93]]
    print(
        "get_by_value_interval('l', [23003, NIL], [25005, INF], VALUE): {}".format(
            bins[2][1]
        )
    )
    # get_by_value_interval('l', [23003, NIL], [25005, INF], VALUE): [[23003, 40.89], [24004, 40.93], [25005, 41.18]]
    print(
        "get_by_value_interval('l', [23003, INF], [25005, INF], VALUE): {}".format(
            bins[3][1]
        )
    )
    # get_by_value_interval('l', [23003, INF], [25005, INF], VALUE): [[24004, 40.93], [25005, 41.18]]
    print(
        "get_by_value_interval('l', [23003, 'rick'], [25005, 'morty'], VALUE): {}".format(
            bins[4][1]
        )
    )
    # get_by_value_interval('l', [23003, 'rick'], [25005, 'morty'], VALUE): [[23003, 40.89], [24004, 40.93]]

    # this is because lists are compared first by index, then by number of elements.
    # NIL has a lower order than float values, and infinite has higher order than all
    # values, according to the ordering rules
    #
    # NIL < (Boolean) < Integer < String < List < Map < Bytes < Double < GeoJSON < INF
    #
    # similarly, 'rick' and 'morty' are lower order to any double values, which
    # is why the result of those boundary values is the same as NIL, NIL
    #
    # See: https://www.aerospike.com/docs/guide/cdt-ordering.html

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
