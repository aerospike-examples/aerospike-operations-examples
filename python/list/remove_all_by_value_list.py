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

key = (options.namespace, options.set, "list-remove-all-by-value-list")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    sample = [1, 2, 3, 4, 3, 2, 1]

    # set the list value, try the VALUE return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_value_list(
            "l", [2, 3], aerospike.LIST_RETURN_VALUE
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nlist remove_all_by_value_list(VALUE, [2, 3])\n{}".format(
            bins[0][1], bins[1][1]
        )
    )
    # [1, 2, 3, 4, 3, 2, 1]
    # [2, 3, 3, 2]

    # reset the list value, try the INDEX return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_value_list(
            "l", [2, 3], aerospike.LIST_RETURN_INDEX
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nlist remove_all_by_value_list(INDEX, [2, 3])\n{}".format(
            bins[0][1], bins[1][1]
        )
    )
    # [1, 2, 3, 4, 3, 2, 1]
    # [1, 2, 4, 5]

    # reset the list value, try the REVERSE_INDEX return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_value_list(
            "l", [2, 3], aerospike.LIST_RETURN_REVERSE_INDEX
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nlist remove_all_by_value_list(REVERSE_INDEX, [2, 3])\n{}".format(
            bins[0][1], bins[1][1]
        )
    )
    # [1, 2, 3, 4, 3, 2, 1]
    # [5, 4, 2, 1]

    # reset the list value, try the RANK return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_value_list(
            "l", [2, 3], aerospike.LIST_RETURN_RANK
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nlist remove_all_by_value_list(RANK, [2, 3])\n{}".format(
            bins[0][1], bins[1][1]
        )
    )
    # [1, 2, 3, 4, 3, 2, 1]
    # [2, 3, 4, 5]

    # reset the list value, try the REVERSE_RANK return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_value_list(
            "l", [2, 3], aerospike.LIST_RETURN_REVERSE_RANK
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nlist remove_all_by_value_list(REVERSE_RANK, [2, 3])\n{}".format(
            bins[0][1], bins[1][1]
        )
    )
    # [1, 2, 3, 4, 3, 2, 1]
    # [4, 3, 2, 1]

    # reset the list value, try the COUNT return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_value_list(
            "l", [2, 3], aerospike.LIST_RETURN_COUNT
        ),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print(
        "\n{}\nlist remove_all_by_value_list(COUNT, [2, 3])\n{}".format(
            bins[0][1], bins[1][1]
        )
    )
    # [1, 2, 3, 4, 3, 2, 1]
    # 4

    # reset the list value, try the NONE return type
    ops = [
        operations.write("l", sample),
        list_operations.list_remove_by_value_list(
            "l", [2, 3], aerospike.LIST_RETURN_NONE
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate(key, ops)
    print(
        "\nlist remove_all_by_value_list(NONE, [2, 3])\nNo return. All operations result in {}".format(
            bins["l"]
        )
    )
    # No return. All operations result in [1, 4, 1]

    # switch to more complex values, a list of (mostly) ordered pairs
    tuples = [["v1", 1], ["v2", 1], ["v1", 2], ["v2", 2], ["v1", 3, 9], ["v2"]]
    wildcard = aerospike.CDTWildcard()
    ops = [
        operations.write("l", tuples),
        operations.read("l"),
        list_operations.list_remove_by_value_list(
            "l", [["v1", wildcard], ["v2", wildcard]], aerospike.LIST_RETURN_VALUE
        ),
        list_operations.list_remove_by_value_list(
            "l", [["z"]], aerospike.LIST_RETURN_COUNT
        ),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nList before: {}".format(bins[0][1]))
    # List before: [['v1', 1], ['v2', 1], ['v1', 2], ['v2', 2], ['v1', 3, 9], ['v2']]
    print(
        "list remove_all_by_value_list(VALUE, [['v1', *], ['v2', *]]): {}".format(
            bins[1][1]
        )
    )
    # list remove_all_by_value_list(VALUE, [['v1', *], ['v2', *]]): [['v1', 1], ['v2', 1], ['v1', 2], ['v2', 2], ['v1', 3, 9]]
    print("list remove_all_by_value_list(COUNT, [['z']]): {}".format(bins[2][1]))
    # list remove_all_by_value_list(COUNT, [['z']]): 0
    print("List after: {}".format(bins[3][1]))
    # List after: [['v2']]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
