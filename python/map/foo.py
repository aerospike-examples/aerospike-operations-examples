# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import map_operations as mapops
from aerospike_helpers.operations import list_operations as listops
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

key = (options.namespace, options.set, "fooz")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    wildcard = aerospike.CDTWildcard()
    client.put(key, {"l": [[1, 'a'], [2, 'b'], [4, 'd'], [2, 'bb']]})
    ctx = [cdt_ctx.cdt_ctx_list_value([2, wildcard])]
    _, _, bins = client.operate(key, [listops.list_append("l", {'c': 3},
        aerospike.MAP_RETURN_NONE, ctx=ctx)])
    print("{}".format(bins["l"]))
    _, _, bins = client.get(key)
    print("{}".format(bins["l"]))
    sys.exit(18)

    # put the same element with the CREATE_ONLY and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "map_write_flags": aerospike.MAP_WRITE_FLAGS_CREATE_ONLY | aerospike.MAP_WRITE_FLAGS_NO_FAIL,
    }
    _, _, bins = client.operate(key, [mapops.map_put("l", "a", 11, policy)])
    print("\nput('l','a', 11, CREATE_ONLY|NO_FAIL. Number of elements in the map: {}".format(bins["l"]))
    # put('l','a', 1, CREATE_ONLY|NO_FAIL). Number of elements in the map: 1
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # {'a': 1}

    # put a different element with the UPDATE_ONLY and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "map_write_flags": aerospike.MAP_WRITE_FLAGS_UPDATE_ONLY | aerospike.MAP_WRITE_FLAGS_NO_FAIL,
    }
    _, _, bins = client.operate(key, [mapops.map_put("l", "b", 2, policy)])
    print("\nput('l','b', 2, UPDATE_ONLY|NO_FAIL). Number of elements in the map: {}".format(bins["l"]))
    # put('l','b', 2, UPDATE_ONLY|NO_FAIL). Number of elements in the map: 1
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # {'a': 1}

    # put a different element with no write flags. the default MODIFY_DEFAULT
    # acts as 'create or update'
    #policy = {
    #    "map_write_flags": aerospike.MAP_WRITE_FLAGS_DEFAULT,
    #}
    _, _, bins = client.operate(key, [mapops.map_put("l", "b", 2)])
    print("\nput('l','b', 2). Number of elements in the map: {}".format(bins["l"]))
    # put('l','b', 2). Number of elements in the map: 2
    key, metadata, bins = client.get(key)
    print("{}".format(bins["l"]))
    # {'a': 1, 'b': 2}
    sys.exit(21)

    # put a different element with an ADD_UNIQUE and NO_FAIL write flags
    # this should work
    policy = {
        "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_NO_FAIL,
        "map_order": aerospike.LIST_UNORDERED,
    }
    ret = client.operate(key, [mapops.map_put("l", [2], policy)])
    key, metadata, bins = client.get(key)
    print("\nput('l', [2], ADD_UNIQUE|NO_FAIL): {}".format(bins["l"]))
    # put('l', 1, ADD_UNIQUE|NO_FAIL):[1, [2]]

    # put to the list element at index 1
    ctx = [cdt_ctx.cdt_ctx_map_index(1)]
    ret = client.operate(key, [mapops.map_put("l", 3, ctx=ctx)])
    key, metadata, bins = client.get(key)
    print("\nput('l', 3 context=BY_LIST_INDEX(1)): {}".format(bins["l"]))
    # put('l', 3, context=BY_LIST_INDEX(1)): [1, [2, 3]]

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
