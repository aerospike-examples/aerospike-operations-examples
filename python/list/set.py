# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations
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

key = (options.namespace, options.set, "list-set")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nset(bin, index, value[, writeFlags, context])\n")
    # create a new record with one element by upsert
    # a list created using set will be unordered, regardless of the list order
    # policy
    ret = client.operate(key, [list_operations.list_set("l", 1, "b")])
    key, metadata, bins = client.get(key)
    print("set('l', 1, 'b'): {}".format(bins["l"]))
    # set('l', 1, 'b'): [None, 'b'] - Aerospike NIL represented as a Python None instance

    # set an element right at the boundary of the current list (index == count)
    # with a INSERT_BOUNDED write flags
    # this should work like an append
    policy = {
        "write_flags": aerospike.LIST_WRITE_INSERT_BOUNDED
        | aerospike.LIST_WRITE_NO_FAIL
    }
    ret = client.operate(key, [list_operations.list_set("l", 2, "c", policy)])
    key, metadata, bins = client.get(key)
    print("\nset('l', 2, 'c', INSERT_BOUNDED|NO_FAIL): {}".format(bins["l"]))
    # set('l', 2, 'c', INSERT_BOUNDED|NO_FAIL): [None, 'b', 'c']

    # set the same element with an ADD_UNIQUE write flags
    # catch the element exists error code 24
    policy = {"write_flags": aerospike.LIST_WRITE_ADD_UNIQUE}
    try:
        print("\nset('l', 3, 'b', ADD_UNIQUE)")
        ret = client.operate(key, [list_operations.list_set("l", 3, "b", policy)])
    except ex.ElementExistsError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        # Error: AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS [24]

    # set an element outside the boundary of the current list
    # with an INSERT_BOUNDED and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "write_flags": aerospike.LIST_WRITE_INSERT_BOUNDED
        | aerospike.LIST_WRITE_NO_FAIL
    }
    ret = client.operate(key, [list_operations.list_set("l", 4, "e", policy)])
    print("\nSet outside the list boundaries with INSERT_BOUNDED failed gracefully")
    print("set('l', 4, 'e', INSERT_BOUNDED|NO_FAIL)")

    # set an element outside the boundary of the current list
    # with no INSERT_BOUNDED
    # this should work
    ret = client.operate(key, [list_operations.list_set("l", 4, [])])
    key, metadata, bins = client.get(key)
    print("\nset('l', 4, []): {}".format(bins["l"]))
    # set('l', 4, []): [None, 'b', 'c', None, []]

    # set a list element at index 4 of the current list
    ctx = [cdt_ctx.cdt_ctx_list_index(4)]
    ret = client.operate(key, [list_operations.list_set("l", 0, "e", ctx=ctx)])
    key, metadata, bins = client.get(key)
    print("\nset('l', 0, 'e', BY_LIST_INDEX(4)): {}".format(bins["l"]))
    # set('l', 0, 'e', BY_LIST_INDEX(4)): [None, 'b', 'c', None, ['e']]

    # set cannot be used on ordered lists
    # change the list order and try to insert
    ops = [
        list_operations.list_set_order("l", aerospike.LIST_ORDERED),
        list_operations.list_set("l", 2, "z"),
    ]
    try:
        print("\nset_order('l', ORDERED)")
        print("set('l', 2, 'z')")
        client.operate(key, ops)
    except ex.OpNotApplicable as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        # AEROSPIKE_ERR_OP_NOT_APPLICABLE [26]
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
