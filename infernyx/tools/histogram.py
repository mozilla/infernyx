#!/usr/bin/python
import sys
import re
import logging
from optparse import OptionParser

options = None


def add_key_value(keys_index, value_index, line_items, stats):
    # make a key
    try:
        key_items = (line_items[i] for i in keys_index)
        if options.reorder:
            key_items = sorted(key_items)
        key = ",".join(key_items)
        val = int(line_items[value_index])
        if key not in stats["keys"]:
            stats["keys"][key] = 0
        stats["keys"][key] += val
        stats["total"] += val
    except:
        pass
    return stats


def read_lines(keys_index, value_index):
    stats = {'total': 0, 'keys': {}}
    for line in sys.stdin:
        try:
            items = re.split(',', line)
            add_key_value(keys_index, value_index, items, stats)
        except Exception as e:
            logging.error("Error processing line '%s': %s" % (line, e))
    return stats


def print_stats(stats):
    if options.debug:
        print "TOTAL: %d" % stats["total"]
    for key in sorted(stats["keys"].iterkeys()):
        if options.debug:
            print "%s,%d,%.2f" % (key, stats["keys"][key], stats["keys"][key] * 100 / stats["total"])
        else:
            print "%s,%d" % (key, stats["keys"][key])


def read_args():
    parser = OptionParser()
    parser.add_option("-k", "--key", dest="keys", help="coma separated key fields")
    parser.add_option("-v", "--value", dest="value", help="value field")
    parser.add_option("-r", "--reorder", action="store_true", default=False, dest="reorder", help="reorder keys")
    parser.add_option("-d", "--debug", action="store_true", default=False, dest="debug", help="show extra verbose info")
    return parser.parse_args()


if __name__ == '__main__':

    (options, args) = read_args()
    keys = [int(i) - 1 for i in re.split(',', options.keys)]
    value = int(options.value) - 1
    s = read_lines(keys, value)
    print_stats(s)

