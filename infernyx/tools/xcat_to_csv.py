#!/usr/bin/python
import sys
import re
import logging


def read_lines():
    line = sys.stdin.readline()
    while len(line):
        try:
            o1, o2 = re.split('\t', line)
            items = eval(o1) + eval(o2)
            print ",".join(str(item) for item in items)
        except Exception as e:
            logging.error("Error processing line '%s': %s" % (line, e))

        line = sys.stdin.readline()


if __name__ == '__main__':
    read_lines()

