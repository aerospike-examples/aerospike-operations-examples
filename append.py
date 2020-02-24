# -*- coding: utf-8 -*-
from __future__ import print_function
from args import options
import aerospike
from aerospike import exception as ex
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

key = (options.namespace, options.set, "op-append")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    # create a record by upsert with a one element list
    ret = client.operate(key, [list_operations.list_append("l", 1)])
    print("The number of elements in the list is {}".format(ret[2]["l"]))

    # append the same element with an ADD_UNIQUE and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_NO_FAIL,
        "list_order": aerospike.LIST_UNORDERED,
    }
    ret = client.operate(key, [list_operations.list_append("l", 1, policy)])
    print("The number of elements in the list is {}".format(ret[2]["l"]))

    # append the different element with an ADD_UNIQUE and NO_FAIL write flags
    # this should work
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_NO_FAIL,
        "list_order": aerospike.LIST_UNORDERED,
    }
    ret = client.operate(key, [list_operations.list_append("l", 2, policy)])
    print("The number of elements in the list is {}".format(ret[2]["l"]))

    k, m, b = client.get(key)
    print("{}".format(b["l"]))
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
