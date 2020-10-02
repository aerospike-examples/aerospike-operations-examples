# Aerospike Operations Examples in Python

These directories contain code examples in Python for each operation of the
Aerospike [data types](https://www.aerospike.com/docs/guide/data-types.html) API.

Prerequisites
-------------
 * [Aerospike Database](https://www.aerospike.com/docs/) >= 4.6.0
 * Aerospike Python client >= 3.10.0

```bash
pip install -r requirements.txt
```

Naming of the Python Scripts
============================
The names of the Python scripts match the generic operation name, not the name
of the method in the Python client. The goal is to allow users to easily locate
the code examples for each method name documented on https://docs.aerospike.com

So, examples for `List get_all_by_value` live in `list/get_all_by_value.py`,
while the corresponding Python client method is
`list_operations.list_get_by_value`.
