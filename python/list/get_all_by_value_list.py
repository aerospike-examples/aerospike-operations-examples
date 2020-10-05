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

key = (options.namespace, options.set, "list-get-all-by-value-list")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nget_all_by_value_list(bin, values [, returnType, context])\n")
    # create a new record with a put. list policy can't be applied outside of
    # list operations, and a new list is unordered by default
    client.put(key, {"l": [2, 3, 2, 1, 2, 3, 2]})
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [2, 3, 2, 1, 2, 3, 2]

    # demonstrate the meaning of the different return types for the
    # list data type read operations, by getting all elements with value 2
    # from list multiple times in the same transaction
    ops = [
        list_operations.list_get_by_value_list(
            "l", [1, 2], aerospike.LIST_RETURN_VALUE
        ),
        list_operations.list_get_by_value_list(
            "l", [1, 2], aerospike.LIST_RETURN_INDEX
        ),
        list_operations.list_get_by_value_list(
            "l", [1, 2], aerospike.LIST_RETURN_REVERSE_INDEX
        ),
        list_operations.list_get_by_value_list("l", [1, 2], aerospike.LIST_RETURN_RANK),
        list_operations.list_get_by_value_list(
            "l", [1, 2], aerospike.LIST_RETURN_REVERSE_RANK
        ),
        list_operations.list_get_by_value_list(
            "l", [1, 2], aerospike.LIST_RETURN_COUNT
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    # in the python client the operate() command returns the result of the last
    # operation on a specific bin, so using operate_ordered instead, which
    # gives the results as ordered (bin-name, result) tuples
    print("\nget_all_by_value_list('l', [1,2], VALUE): {}".format(bins[0][1]))
    # get_all_by_value_list('l', [1,2], VALUE): [2, 2, 1, 2, 2]
    print("get_all_by_value_list('l', [1,2], INDEX): {}".format(bins[1][1]))
    # get_all_by_value_list('l', [1,2], INDEX): [0, 2, 3, 4, 6]
    print("get_all_by_value_list('l', [1,2], REVERSE_INDEX): {}".format(bins[2][1]))
    # get_all_by_value_list('l', [1,2], REVERSE_INDEX): [6, 4, 3, 2, 0]
    print("get_all_by_value_list('l', [1,2], RANK): {}".format(bins[3][1]))
    # get_all_by_value_list('l', [1,2], RANK): [0, 1, 2, 3, 4]
    print("get_all_by_value_list('l', [1,2], REVERSE_RANK): {}".format(bins[4][1]))
    # get_all_by_value_list('l', [1,2], REVERSE_RANK): [6, 5, 4, 3, 2]
    print("get_all_by_value_list('l', [1,2], COUNT): {}".format(bins[5][1]))
    # get_all_by_value_list('l', [1,2], COUNT): 5

    # switch to more complex values, a list of (mostly) ordered pairs
    tuples = [["v1", 1], ["v2", 1], ["v1", 2], ["v2", 2], ["v1", 3, "z", 8], ["v1"]]
    wildcard = aerospike.CDTWildcard()
    ops = [
        operations.write("l", tuples),
        operations.read("l"),
        list_operations.list_get_by_value_list(
            "l", [["v1"]], aerospike.LIST_RETURN_INDEX
        ),
        list_operations.list_get_by_value_list(
            "l", [["v1"], ["v1", wildcard]], aerospike.LIST_RETURN_VALUE
        ),
        list_operations.list_get_by_value_list(
            "l", [["v1"], ["v1", wildcard]], aerospike.LIST_RETURN_VALUE, inverted=True
        ),
        list_operations.list_get_by_value_list(
            "l", [["v1", 3, wildcard]], aerospike.LIST_RETURN_VALUE
        ),
        list_operations.list_get_by_value_list(
            "l", [[wildcard, 2]], aerospike.LIST_RETURN_VALUE
        ),
        list_operations.list_get_by_value_list(
            "l", [["z"], ["y"]], aerospike.LIST_RETURN_COUNT
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}".format(bins[0][1]))
    # [['v1', 1], ['v2', 1], ['v1', 2], ['v2', 2], ['v1', 3, 'z', 8], ['v1']]
    print("get_all_by_value_list('l', [['v1']], INDEX): {}".format(bins[1][1]))
    # list get_all_by_value_list('l', [['v1']], INDEX): [5]
    print(
        "get_all_by_value_list('l', [['v1'], ['v1', *]], VALUE): {}".format(bins[2][1])
    )
    # list get_all_by_value_list('l', [['v1'], ['v1', *]], VALUE): [['v1', 1], ['v1', 2], ['v1', 3, 'z', 8], ['v1']]
    # The wildcard operation acts as a glob
    print(
        "get_all_by_value_list('l', [['v1'], ['v1', *]], INVERTED|VALUE): {}".format(
            bins[3][1]
        )
    )
    # list get_all_by_value_list('l', [['v1'], ['v1', *]], INVERTED|VALUE): [['v2', 1], ['v2', 2]]
    print("get_all_by_value_list('l', [['v1', 3, *]], VALUE): {}".format(bins[4][1]))
    # list get_all_by_value_list('l', [['v1', 3, *]], VALUE): [['v1', 3, 'z', 8]]
    print("get_all_by_value_list('l', [[*,2]], VALUE): {}".format(bins[5][1]))
    # the wildcard matches everything to the end of the list, so anything to its
    # right is matched. We can't use get_by_value_list to find the elements which
    # are a list with a last element of 2. [*, 2] is the same as [*]
    # list get_all_by_value_list('l', [[*,2]], VALUE): [['v1', 1], ['v2', 1], ['v1', 2], ['v2', 2], ['v1', 3, 'z', 8], ['v1']]
    print("get_all_by_value_list('l', [['z'], ['y']], COUNT): {}".format(bins[6][1]))
    # list get_all_by_value_list('l', [['z'], ['y']], COUNT): 0

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
