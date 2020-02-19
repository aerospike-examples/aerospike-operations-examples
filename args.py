import argparse

argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument(
    "--help", dest="help", action="store_true", help="Displays this message."
)
argparser.add_argument(
    "-U",
    "--username",
    dest="username",
    metavar="<USERNAME>",
    help="Username to connect to database.",
)
argparser.add_argument(
    "-P",
    "--password",
    dest="password",
    metavar="<PASSWORD>",
    help="Password to connect to database.",
)
argparser.add_argument(
    "-h",
    "--host",
    dest="host",
    default="127.0.0.1",
    metavar="<ADDRESS>",
    help="Address of Aerospike server.",
)
argparser.add_argument(
    "-p",
    "--port",
    dest="port",
    type=int,
    default=3000,
    metavar="<PORT>",
    help="Port of the Aerospike server.",
)
argparser.add_argument(
    "-n",
    "--namespace",
    dest="namespace",
    default="test",
    metavar="<NS>",
    help="Port of the Aerospike server.",
)
argparser.add_argument(
    "-s",
    "--set",
    dest="set",
    default="samples",
    metavar="<SET>",
    help="Port of the Aerospike server.",
)
options = argparser.parse_args()
if options.help:
    argparser.print_help()
    print()
    sys.exit(1)

