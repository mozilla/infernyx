#!/usr/bin/python
import sys
import re
import logging
from optparse import OptionParser

stats = {'total': 0, 'keys':{}}

def addKeyValue(keysIndex, valueIndex, lineItems):
  ## make a key
  try:
    key = ",".join(lineItems[i] for i in keysIndex)
    value = int(lineItems[valueIndex])
    if key not in stats["keys"]:
      stats["keys"][key] = 0
    stats["keys"][key] += value
    stats["total"] += value
  except:
    pass

def readLines(keysIndex, valueIndex):
  line = sys.stdin.readline()
  while (len(line) > 0):
    try:
      items = re.split(',', line)
      addKeyValue(keysIndex, valueIndex, items)
    except Exception as e:
      logging.error("Error processing line '%s': %s" % (line, e))
    line = sys.stdin.readline()

def printStats():
  print "TOTAL: %d" % stats["total"]
  for key in sorted(stats["keys"].iterkeys()):
    print "%s %d %.2f" % (key, stats["keys"][key], stats["keys"][key] * 100 / stats["total"])

def readArgs():
  parser = OptionParser()
  parser.add_option("-k", "--key", dest="keys", help="coma separated key fields")
  parser.add_option("-v", "--value", dest="value", help="value field")
  return parser.parse_args()

if __name__ == '__main__':
  (options, args) = readArgs()
  keys = [int(i) for i in re.split(',', options.keys)]
  value = int(options.value)
  readLines(keys, value)
  printStats()

