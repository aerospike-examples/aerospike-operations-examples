# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import map_operations as mapops
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

key = (options.namespace, options.set, "map-put-items")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nput_items(bin, items[, writeFlags, createType, context])\n")
    # upsert a new record with two elements
    # by default a newly created map is unordered, so declare the map order to
    # be  K_ORDERED
    # the map order policy is only used when creating a new map bin
    policy = {
        "map_order": aerospike.MAP_KEY_ORDERED,
    }
    items = {"a": 1, "b": 2}
    _, _, bins = client.operate(key, [mapops.map_put_items("m", items, policy)])
    print("put_items('m', {}, createType=K_ORDERED). Number of elements in the map: {}".format(items, bins["m"]))
    # put_items('m', {'a': 1, 'b': 2}, createType=K_ORDERED). Number of elements in the map: 2
    key, metadata, bins = client.get(key)
    print("{}".format(bins["m"]))
    # {'a': 1, 'b': 2}

    # put the same elements with the CREATE_ONLY and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "map_write_flags": aerospike.MAP_WRITE_FLAGS_CREATE_ONLY | aerospike.MAP_WRITE_FLAGS_NO_FAIL,
    }
    _, _, bins = client.operate(key, [mapops.map_put_items("m", items, policy)])
    print("\nput_items('m', {}, CREATE_ONLY|NO_FAIL. Number of elements in the map: {}".format(items, bins["m"]))
    # put_items('m', {'a': 1, 'b': 2}, CREATE_ONLY|NO_FAIL. Number of elements in the map: 2
    key, metadata, bins = client.get(key)
    print("{}".format(bins["m"]))
    # {'a': 1, 'b': 2}

    # put items a different element with the UPDATE_ONLY and NO_FAIL write flags
    # this should fail gracefully
    items2 = {"c": 3, "d": 4}
    policy = {
        "map_write_flags": aerospike.MAP_WRITE_FLAGS_UPDATE_ONLY | aerospike.MAP_WRITE_FLAGS_NO_FAIL,
    }
    _, _, bins = client.operate(key, [mapops.map_put_items("m", items2, policy)])
    print("\nput_items('m', {}, UPDATE_ONLY|NO_FAIL). Number of elements in the map: {}".format(items2, bins["m"]))
    # put_items('m', {'c': 3, 'd': 4}, UPDATE_ONLY|NO_FAIL). Number of elements in the map: 2
    key, metadata, bins = client.get(key)
    print("{}".format(bins["m"]))
    # {'a': 1, 'b': 2}
    sys.exit(9)

    # put items that partially overlap the existing elements in the map
    # the NO_FAIL | DO_PARTIAL policy will ensure the non-colliding elements will
    # get added to the map, while the operation will gracefully fail for the
    # existing elements
    policy = {
        "map_write_flags": aerospike.MAP_WRITE_FLAGS_PARTIAL | aerospike.MAP_WRITE_FLAGS_NO_FAIL,
    }
    _, _, bins = client.operate(key, [mapops.map_put_items("m", "b", 2)])
    print("\nput_items('m','b', 2). Number of elements in the map: {}".format(bins["m"]))
    # put_items('m','b', 2). Number of elements in the map: 2
    key, metadata, bins = client.get(key)
    print("{}".format(bins["m"]))
    # {'a': 1, 'b': 2}

    # add a nested map element and operate on it
    ctx = [cdt_ctx.cdt_ctx_map_key("c")]
    ops = [
        mapops.map_put_items("m", "c", {}),
        mapops.map_put_items("m", "d", 4, ctx=ctx),
    ]
    _, _, bins = client.operate_ordered(key, ops)
    print("\nput_items('m', 'c', {}). Number of elements in the map: {}".format("{}", bins[0][1]))
    print("put_items('m', 'd', 4, context=BY_MAP_KEY('c')). Number of elements in the inner map: {}".format(bins[1][1]))
    key, metadata, bins = client.get(key)
    print("{}".format(bins["m"]))

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
