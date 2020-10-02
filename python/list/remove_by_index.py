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

key = (options.namespace, options.set, "list-remove-by-index")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    sample = [9, 8, 7, 6, 5, 4, 3, 2]

    # set the list value, try the VALUE return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_index("l", 0, aerospike.LIST_RETURN_VALUE),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nlist remove_by_index(VALUE, 0)\n{}".format(bins[0][1], bins[1][1]))
    # [9, 8, 7, 6, 5, 4, 3, 2]
    # 9

    # reset the list value, try the INDEX return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_index("l", 0, aerospike.LIST_RETURN_INDEX),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nlist remove_by_index(INDEX, 0)\n{}".format(bins[0][1], bins[1][1]))
    # [9, 8, 7, 6, 5, 4, 3, 2]
    # 0

    # reset the list value, try the REVERSE_INDEX return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_index("l", 0, aerospike.LIST_RETURN_REVERSE_INDEX),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nlist remove_by_index(REVERSE_INDEX, 0)\n{}".format(bins[0][1], bins[1][1]))
    # [9, 8, 7, 6, 5, 4, 3, 2]
    # 7

    # reset the list value, try the RANK return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_index("l", 0, aerospike.LIST_RETURN_RANK),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nlist remove_by_index(RANK, 0)\n{}".format(bins[0][1], bins[1][1]))
    # [9, 8, 7, 6, 5, 4, 3, 2]
    # 7

    # reset the list value, try the REVERSE_RANK return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_index("l", 0, aerospike.LIST_RETURN_REVERSE_RANK),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nlist remove_by_index(REVERSE_RANK, 0)\n{}".format(bins[0][1], bins[1][1]))
    # [9, 8, 7, 6, 5, 4, 3, 2]
    # 0

    # reset the list value, try the COUNT return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_index("l", 0, aerospike.LIST_RETURN_COUNT),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nlist remove_by_index(COUNT, 0)\n{}".format(bins[0][1], bins[1][1]))
    # [9, 8, 7, 6, 5, 4, 3, 2]
    # 1

    # reset the list value, try the NONE return type
    ops = [
        operations.write("l", sample),
        list_operations.list_remove_by_index("l", 0, aerospike.LIST_RETURN_NONE),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate(key, ops)
    print("\nlist remove_by_index(NONE, 0)\nNo return. All operations result in {}".format(bins["l"]))
    # No return. All operations result in [8, 7, 6, 5, 4, 3, 2]

    # remove the element at index -2 (second from the end) and also append a new
    # element to the end of the list
    ops = [
        list_operations.list_remove_by_index("l", -2, aerospike.LIST_RETURN_VALUE),
        list_operations.list_append("l", [1, 3, 3, 7, 0]),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nlist remove_by_index(VALUE, -2)\n{}\n{}".format(bins[0][1], bins[2][1]))
    # 3
    # [8, 7, 6, 5, 4, 2, [1, 3, 3, 7, 0]]

    # remove the middle element of the inner list at last element of the outer list
    ctx = [cdt_ctx.cdt_ctx_list_index(-1)]
    ops = [
        list_operations.list_remove_by_index("l", 2, aerospike.LIST_RETURN_NONE, ctx),
        list_operations.list_get_by_index("l", -1, aerospike.LIST_RETURN_VALUE),
    ]
    key, metadata, bins = client.operate(key, ops)
    print("\nInner list after the element at index 2 is removed\n{}".format(bins["l"]))

    # try to perform a remove by index operation on an index outside of the current list
    try:
        key, metadata, bins = client.operate(
            key,
            [list_operations.list_remove_by_index("l", 11, aerospike.LIST_RETURN_VALUE)],
        )
        print("\n{}".format(bins["l"]))
    except ex.OpNotApplicable as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))
        # Error: 0.0.0.0:3000 AEROSPIKE_ERR_OP_NOT_APPLICABLE [26]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
