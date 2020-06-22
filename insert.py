# -*- coding: utf-8 -*-
from __future__ import print_function
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations
import sys

if options.set == "None":
    options.set = None

config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

key = (options.namespace, options.set, "op-insert")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    # create a new record with one element by upsert
    # a list created using insert will be unordered, regardless of the
    # list order policy
    ret = client.operate(key, [list_operations.list_insert("l", 1, "c")])
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [None, 'c'] - Aerospike NIL represented as an instance of Python None

    # insert another element into the existing list
    ret = client.operate(key, [list_operations.list_insert("l", 1, "b")])
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [None, 'b', 'c']

    # insert the same element with an ADD_UNIQUE and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_NO_FAIL,
        "list_order": aerospike.LIST_UNORDERED,
    }
    ret = client.operate(key, [list_operations.list_insert("l", 1, "b", policy)])
    print("\nThe number of elements in the list is {}".format(ret[2]["l"]))
    # The number of elements in the list is 3

    # insert an element right at the boundary of the current list
    # (index == count) with an INSERT_BOUNDED write flag
    # this should work like an append
    policy = {
        "write_flags": aerospike.LIST_WRITE_INSERT_BOUNDED
        | aerospike.LIST_WRITE_NO_FAIL
    }
    ret = client.operate(key, [list_operations.list_insert("l", 3, "d", policy)])
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [None, 'b', 'c', 'd']

    # insert an element outside the boundary of the current list
    # with INSERT_BOUNDED and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "write_flags": aerospike.LIST_WRITE_INSERT_BOUNDED
        | aerospike.LIST_WRITE_NO_FAIL
    }
    ret = client.operate(key, [list_operations.list_insert("l", 5, "f", policy)])
    print("\nThe number of elements in the list is {}".format(ret[2]["l"]))
    # The number of elements in the list is 4

    # insert an element outside the boundary of the current list
    # with no INSERT_BOUNDED. this should work
    ret = client.operate(key, [list_operations.list_insert("l", 5, [])])
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [None, 'b', 'c', 'd', None, []]

    # insert a value into the list element at index 5 of the current list
    ctx = [cdt_ctx.cdt_ctx_list_index(5)]
    ret = client.operate(key, [list_operations.list_insert("l", 0, "f", ctx=ctx)])
    k, m, b = client.get(key)
    print("\n{}".format(b["l"]))
    # [None, 'b', 'c', 'd', None, ['f']]

    # try to add an element that already exist, using ADD_UNIQUE
    # catch the element exists error code 24
    policy = {"write_flags": aerospike.LIST_WRITE_ADD_UNIQUE}
    try:
        ret = client.operate(key, [list_operations.list_insert("l", 3, "b", policy)])
    except ex.ElementExistsError as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))
        # Error: AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS [24]

    # insert cannot be used on ordered lists
    # changing the order and trying to insert
    ops = [
        list_operations.list_set_order("l", aerospike.LIST_ORDERED),
        list_operations.list_insert("l", 2, "z"),
    ]
    try:
        client.operate(key, ops)
    except ex.OpNotApplicable as e:
        print("\nError: {0} [{1}]".format(e.msg, e.code))
        # AEROSPIKE_ERR_OP_NOT_APPLICABLE [26]
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
