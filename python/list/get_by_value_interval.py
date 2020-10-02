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

key = (options.namespace, options.set, "list-get-by-value-interval")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    # demonstrate the meaning of the different return types for the
    # list data type read operations, by getting all elements whose value
    # is in the interval [2, 4) multiple times in the same transaction
    ops = [
        operations.write("l", [1, 2, 3, 4, 5, 4, 3, 2, 1]),
        list_operations.list_get_by_value_range("l", aerospike.LIST_RETURN_VALUE, 2, 4),
        list_operations.list_get_by_value_range("l", aerospike.LIST_RETURN_INDEX, 2, 4),
        list_operations.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_REVERSE_INDEX, 2, 4
        ),
        list_operations.list_get_by_value_range("l", aerospike.LIST_RETURN_RANK, 2, 4),
        list_operations.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_REVERSE_RANK, 2, 4
        ),
        list_operations.list_get_by_value_range("l", aerospike.LIST_RETURN_COUNT, 2, 4),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    # in the python client the operate() command returns the result of the last
    # operation on a specific bin, so using operate_ordered instead, which
    # gives the results as ordered (bin-name, result) tuples
    print("\nlist get_by_value_interval(VALUE, 2, 4): {}".format(bins[0][1]))
    # list get_by_value_interval(VALUE, 2, 4): [2, 3, 3, 2]
    print("list get_by_value_interval(INDEX, 2, 4): {}".format(bins[1][1]))
    # list get_by_value_interval(INDEX, 2, 4): [1, 2, 6, 7]
    print("list get_by_value_interval(REVERSE_INDEX, 2, 4): {}".format(bins[2][1]))
    # list get_by_value_interval(REVERSE_INDEX, 2, 4): [7, 6, 2, 1]
    print("list get_by_value_interval(RANK, 2, 4): {}".format(bins[3][1]))
    # list get_by_value_interval(RANK, 2, 4): [2, 3, 4, 5]
    print("list get_by_value_interval(REVERSE_RANK, 2, 4): {}".format(bins[4][1]))
    # list get_by_value_interval(REVERSE_RANK, 2, 4): [3, 4, 5, 6]
    print("list get_by_value_interval(COUNT, 2, 4): {}".format(bins[5][1]))
    # list get_by_value_interval(COUNT, 2, 4): 4

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
    infinity = aerospike.CDTInfinite()
    ops = [
        operations.write("l", tuples),
        operations.read("l"),
        list_operations.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_VALUE, [23003, nil], [25005, nil]
        ),
        list_operations.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_VALUE, [23003, nil], [25005, infinity]
        ),
        list_operations.list_get_by_value_range(
            "l", aerospike.LIST_RETURN_VALUE, [23003, infinity], [25005, infinity]
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}".format(bins[0][1]))
    # [[20000, 39.04], [21001, 39.78], [23003, 40.89], [24004, 40.93], [25005, 41.18], [26006, 40.07]]
    print(
        "\nlist get_by_value_interval(VALUE, [23003, nil], [25005, nil]): {}".format(
            bins[1][1]
        )
    )
    # list get_by_value_interval(VALUE, [23003, nil], [25005, nil]): [[23003, 40.89], [24004, 40.93]]
    print(
        "list get_by_value_interval(VALUE, [23003, nil], [25005, infinity]): {}".format(
            bins[2][1]
        )
    )
    # list get_by_value_interval(VALUE, [23003, nil], [25005, infinity]): [[23003, 40.89], [24004, 40.93], [25005, 41.18]]
    print(
        "list get_by_value_interval(VALUE, [23003, infinity], [25005, infinity]): {}".format(
            bins[3][1]
        )
    )
    # list get_by_value_interval(VALUE, [23003, infinity], [25005, infinity]): [[24004, 40.93], [25005, 41.18]]

    # this is because lists are compared first by index, then by number of elements.
    #nil has a lower order than float values, and infinity has higher order than all
    # values, according to the ordering rules
    #
    # Nil < (Boolean) < Integer < String < List < Map < Bytes < Double < GeoJSON
    #
    # See: https://www.aerospike.com/docs/guide/cdt-ordering.html

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
