#!/usr/bin/python

import sys
import getopt
import json

from deepmerge import Merger


def merge_list(merger, path, base, nxt):
    extensions = dict((d['name'], dict(d, index=index)) for (index, d) in enumerate(nxt))
    result = []
    for base_field in base:
        extension_field = extensions.get(base_field['name'])
        if extension_field:
            merged_field = merger.merge(base_field, extension_field)
            del merged_field['index']
            result.append(merged_field)
        else:
            result.append(base_field)
    return result

schemas_merger = Merger(
    [
        (list, merge_list),
        (dict, ["merge"])
    ],
    ["override"],
    ["override"]
)

def help():
    print('merge-schemas.py -s <schema-path> -e <extension-path> -o <out-schema-path>')


def main(argv):
    schema = ''
    extension = ''
    out_schema = ''
    try:
        opts, args = getopt.getopt(argv, "hs:e:o", ["schema=", "extension=", "out="])
    except getopt.GetoptError:
        help()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            help()
            sys.exit()
        elif opt in ("-s", "--schema"):
            schema = json.loads(arg)
        elif opt in ("-e", "--extension"):
            extension = json.loads(arg)
        elif opt in ("-o", "--out"):
            out_schema = arg
    
    # TODO: validate args
    
    merged_schema = schemas_merger.merge(schema, extension)
    with open(out_schema, 'w') as out_schema_file:
        json.dump(merged_schema, out_schema_file)

if __name__ == "__main__":
    main(sys.argv[1:])
