import json


# Reference converter utility

def get_by_ref(ref, obj):
    assert (ref[0:2] == '#/')

    path = ref[2:].split('/')

    walk = obj

    for p in path:
        walk = walk[p]

    return walk.copy()


def resolver(json, current=None):
    if current == None:
        current = json.copy()

    result = dict()

    for k, v in current.items():
        if k == '$ref':
            result.update(resolver(json, get_by_ref(v, json)))
        elif isinstance(v, dict):
            result[k] = resolver(json, v.copy())
        else:
            result[k] = v

    return result


import argparse

parser = argparse.ArgumentParser()

parser.add_argument('-f', '--f', help='path to a file to parse', required=True)

args = parser.parse_args()

with open(args.f, 'r') as f:
    j = json.load(f)
    new = resolver(j)
    if 'definitions' in new:
        del new['definitions']
    print(new)
