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

key = (options.namespace, options.set, "list-remove-by-rank")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nremove_by_rank(bin, rank[, returnType, context])\n")
    sample = [6, 10, 5, 11, 4, 12, 3]

    # set the list value, try the VALUE return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_rank("l", 0, aerospike.LIST_RETURN_VALUE),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_rank('l', 0, VALUE): {}".format(bins[0][1], bins[1][1]))
    # [6, 10, 5, 11, 4, 12, 3]
    # remove_by_rank('l', 0, VALUE): 3

    # reset the list value, try the INDEX return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_rank("l", 0, aerospike.LIST_RETURN_INDEX),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_rank('l', 0, INDEX): {}".format(bins[0][1], bins[1][1]))
    # [6, 10, 5, 11, 4, 12, 3]
    # remove_by_rank('l', 0, INDEX): 6

    # reset the list value, try the REVERSE_INDEX return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_rank("l", 0, aerospike.LIST_RETURN_REVERSE_INDEX),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_rank('l', 0, REVERSE_INDEX): {}".format(bins[0][1], bins[1][1]))
    # [6, 10, 5, 11, 4, 12, 3]
    # remove_by_rank('l', 0, REVERSE_INDEX): 0

    # reset the list value, try the RANK return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_rank("l", 0, aerospike.LIST_RETURN_RANK),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_rank('l', 0, RANK): {}".format(bins[0][1], bins[1][1]))
    # [6, 10, 5, 11, 4, 12, 3]
    # remove_by_rank('l', 0, RANK): 0

    # reset the list value, try the REVERSE_RANK return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_rank("l", 0, aerospike.LIST_RETURN_REVERSE_RANK),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_rank('l', 0, REVERSE_RANK): {}".format(bins[0][1], bins[1][1]))
    # [6, 10, 5, 11, 4, 12, 3]
    # remove_by_rank('l', 0, REVERSE_RANK): 6

    # reset the list value, try the COUNT return type
    ops = [
        operations.write("l", sample),
        operations.read("l"),
        list_operations.list_remove_by_rank("l", 0, aerospike.LIST_RETURN_COUNT),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\n{}\nremove_by_rank('l', 0, COUNT): {}".format(bins[0][1], bins[1][1]))
    # [6, 10, 5, 11, 4, 12, 3]
    # remove_by_rank('l', 0, COUNT): 1

    # reset the list value, try the NONE return type
    ops = [
        operations.write("l", sample),
        list_operations.list_remove_by_rank("l", 0, aerospike.LIST_RETURN_NONE),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate(key, ops)
    print("\nremove_by_rank('l', 0, NONE)\nNo return. All operations result in {}".format(bins["l"]))
    # remove_by_rank('l', 0, NONE)
    # No return. All operations result in [6, 10, 5, 11, 4, 12]

    # remove the second highest ranked element (-2) and also append a new
    # element to the end of the list
    ops = [
        list_operations.list_remove_by_rank("l", -2, aerospike.LIST_RETURN_VALUE),
        list_operations.list_append("l", [1, 3, 3, 7, 0]),
        operations.read("l"),
    ]
    key, metadata, bins = client.operate_ordered(key, ops)
    print("\nremove_by_rank('l', -2, VALUE): {}\n{}".format(bins[0][1], bins[2][1]))
    # remove_by_rank('l', -2, VALUE): 11
    # [6, 10, 5, 4, 12, [1, 3, 3, 7, 0]]

    # remove the highest ranked element of the inner list
    # located at the last element of the outer list
    ctx = [cdt_ctx.cdt_ctx_list_index(-1)]
    ops = [
        list_operations.list_remove_by_rank("l", -1, aerospike.LIST_RETURN_NONE, ctx),
        list_operations.list_get_by_index("l", -1, aerospike.LIST_RETURN_VALUE),
    ]
    key, metadata, bins = client.operate(key, ops)
    print("\nremove_by_rank('l', -1, NONE, BY_LIST_INDEX(-1))")
    print("get_by_index('l', -1, VALUE)")
    print("Inner list after the highest ranked element is removed\n{}".format(bins["l"]))
    # remove_by_rank('l', -1, NONE, BY_LIST_INDEX(-1))
    # get_by_index('l', -1, VALUE)
    # [1, 3, 3, 0]

    # try to perform a remove by index operation on an index outside of the current list
    try:
        print("\nremove_by_rank('l', 11, VALUE)")
        key, metadata, bins = client.operate(
            key,
            [list_operations.list_remove_by_rank("l", 11, aerospike.LIST_RETURN_VALUE)],
        )
        print("\n{}".format(bins["l"]))
    except ex.OpNotApplicable as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        # Error: 0.0.0.0:3000 AEROSPIKE_ERR_OP_NOT_APPLICABLE [26]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
