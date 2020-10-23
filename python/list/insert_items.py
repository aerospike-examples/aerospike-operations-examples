# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import list_operations listops
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

key = (options.namespace, options.set, "list-insert_items")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\ninsert_items(bin, index, items[, writeFlags, context])\n")
    # create a new record by upsert
    # by default a newly created list is unordered
    client.operate(key, [listops.list_insert_items("l", 1, ["a", "e"])])
    key, metadata, bins = client.get(key)
    print("insert_items('l', 1, ['a', 'e'])")
    # insert_items('l', 1, ['a', 'e'])
    print("{}".format(bins["l"]))
    # [None, 'a', 'e'] - Aerospike NIL represented as an instance of Python None

    # insert other elements into the existing list
    client.operate(key, [listops.list_insert_items("l", 2, ["b", "c"])])
    print("\ninsert_items('l', 2, ['b', 'c'])")
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [None, 'a', 'b', 'c', 'e']

    # insert a mix of existing and new items using
    # the ADD_UNIQUE, DO_PARTIAL and NO_FAIL write flags
    # this should gracefully skip the non-unique elements and add the new items
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE
        | aerospike.LIST_WRITE_NO_FAIL
        | aerospike.LIST_WRITE_PARTIAL,
        "list_order": aerospike.LIST_UNORDERED,
    }
    client.operate(
        key, [listops.list_insert_items("l", 4, ["a", "d", "e"], policy)]
    )
    print("\ninsert_items('l', 4, ['a', 'd', 'e'], ADD_UNIQUE|NO_FAIL|PARTIAL)")
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [None, 'a', 'b', 'c', 'd', 'e']

    # insert elements right at the boundary of the current list
    # (index == count) with an INSERT_BOUNDED write flag
    # this should work like an append
    policy = {
        "write_flags": aerospike.LIST_WRITE_INSERT_BOUNDED
        | aerospike.LIST_WRITE_NO_FAIL
    }
    client.operate(key, [listops.list_insert_items("l", 6, ["f", "g"], policy)])
    key, metadata, bins = client.get(key)
    print("\ninsert_items('l', 6, ['f', 'g'], INSERT_BOUNDED|NO_FAIL)")
    print("{}".format(bins["l"]))
    # [None, 'a', 'b', 'c', 'd', 'e', 'f', 'g']

    # insert elements outside the boundary of the current list
    # with INSERT_BOUNDED and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "write_flags": aerospike.LIST_WRITE_INSERT_BOUNDED
        | aerospike.LIST_WRITE_NO_FAIL
    }
    client.operate(key, [listops.list_insert_items("l", 9, ["i", "j"], policy)])
    print("\ninsert_items('l', 9, ['i', 'j'], INSERT_BOUNDED|NO_FAIL)")

    # insert an element outside the boundary of the current list
    # with no INSERT_BOUNDED. this should work
    client.operate(key, [listops.list_insert_items("l", 9, [[]])])
    print("\ninsert_items('l', 9, [[]])")
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [None, 'a', 'b', 'c', 'd', 'e', 'f', 'g', None, []]

    # insert items into the list element at index 9 of the current list
    ctx = [cdt_ctx.cdt_ctx_list_index(9)]
    ret = client.operate(
        key, [listops.list_insert_items("l", 0, ["i", "j"], ctx=ctx)]
    )
    print("\ninsert_items('l', 0, ['i', 'j'], ctx=BY_LIST_INDEX(9))")
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # [None, 'a', 'b', 'c', 'd', 'e', 'f', 'g', None, ['i', 'j']]

    # try to add elements, some of which already exist, using ADD_UNIQUE and no
    # NO_FAIL. catch the element exists error code 24
    policy = {"write_flags": aerospike.LIST_WRITE_ADD_UNIQUE}
    try:
        print("\ninsert_items('l', 3, ['b', 'z'], ADD_UNIQUE)")
        ret = client.operate(
            key, [listops.list_insert_items("l", 3, ["b", "z"], policy)]
        )
    except ex.ElementExistsError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        # Error: AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS [24]

    # insert cannot be used on ordered lists
    ops = [
        listops.list_set_order("l", aerospike.LIST_ORDERED),
        listops.list_insert_items("l", 2, ["z"]),
    ]
    try:
        print("\nset_order('l', ORDERED)")
        print("insert_items('l', 2, ['z'])")
        client.operate(key, ops)
    except ex.OpNotApplicable as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        # AEROSPIKE_ERR_OP_NOT_APPLICABLE [26]
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
