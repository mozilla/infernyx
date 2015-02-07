#!/usr/bin/python
import sys
import re

def readLines():
  line = sys.stdin.readline()
  while (len(line) > 0):
    o1, o2 = re.split('\t', line)
    items = eval(o1) + eval(o2)
    print ",".join(str(item) for item in items)
    line = sys.stdin.readline()

if __name__ == '__main__':
    readLines()

