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

key = (options.namespace, options.set, "map-put")
try:
    client.remove(key)
except ex.RecordError as e:
    pass

try:
    print("\nput(bin, key, value[, writeFlags, createType, context])\n")
    # create a new record with one element by upsert
    # by default a newly created map is unordered, so declare the map order to
    # be  K_ORDERED
    # the map order policy is only used when creating a new map bin
    policy = {
        "map_order": aerospike.MAP_KEY_ORDERED,
    }
    _, _, bins = client.operate(key, [mapops.map_put("m", "a", 1, policy)])
    print("put('m','a', 1, createType=K_ORDERED). Number of elements in the map: {}".format(bins["m"]))
    # put('m','a', 1, createType=K_ORDERED). Number of elements in the map: 1
    key, metadata, bins = client.get(key)
    print("{}".format(bins["m"]))
    # {'a': 1}

    # put the same element with the CREATE_ONLY and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "map_write_flags": aerospike.MAP_WRITE_FLAGS_CREATE_ONLY | aerospike.MAP_WRITE_FLAGS_NO_FAIL,
    }
    _, _, bins = client.operate(key, [mapops.map_put("m", "a", 11, policy)])
    print("\nput('m','a', 11, CREATE_ONLY|NO_FAIL). Number of elements in the map: {}".format(bins["m"]))
    # put('m','a', 1, CREATE_ONLY|NO_FAIL). Number of elements in the map: 1
    key, metadata, bins = client.get(key)
    print("{}".format(bins["m"]))
    # {'a': 1}

    # put a different element with the UPDATE_ONLY and NO_FAIL write flags
    # this should fail gracefully
    policy = {
        "map_write_flags": aerospike.MAP_WRITE_FLAGS_UPDATE_ONLY | aerospike.MAP_WRITE_FLAGS_NO_FAIL,
    }
    _, _, bins = client.operate(key, [mapops.map_put("m", "b", 2, policy)])
    print("\nput('m','b', 2, UPDATE_ONLY|NO_FAIL). Number of elements in the map: {}".format(bins["m"]))
    # put('m','b', 2, UPDATE_ONLY|NO_FAIL). Number of elements in the map: 1
    key, metadata, bins = client.get(key)
    print("{}".format(bins["m"]))
    # {'a': 1}

    # put a different element with no write flags. the default MODIFY_DEFAULT
    # acts as 'create or update'
    #policy = {
    #    "map_write_flags": aerospike.MAP_WRITE_FLAGS_DEFAULT,
    #}
    _, _, bins = client.operate(key, [mapops.map_put("m", "b", 2)])
    print("\nput('m','b', 2). Number of elements in the map: {}".format(bins["m"]))
    # put('m','b', 2). Number of elements in the map: 2
    key, metadata, bins = client.get(key)
    print("{}".format(bins["m"]))
    # {'a': 1, 'b': 2}

    # add a nested map element and operate on it
    ctx = [cdt_ctx.cdt_ctx_map_key("c")]
    ops = [
        mapops.map_put("m", "c", {}),
        mapops.map_put("m", "d", 4, ctx=ctx),
    ]
    _, _, bins = client.operate_ordered(key, ops)
    print("\nput('m', 'c', {}). Number of elements in the map: {}".format("{}", bins[0][1]))
    print("put('m', 'd', 4, context=BY_MAP_KEY('c')). Number of elements in the inner map: {}".format(bins[1][1]))
    key, metadata, bins = client.get(key)
    print("{}".format(bins["m"]))

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
