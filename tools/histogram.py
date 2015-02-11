#!/usr/bin/python
import sys
import re
import logging
from optparse import OptionParser

stats = {'total': 0, 'keys':{}}
options = None

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
  if (options.debug):
    print "TOTAL: %d" % stats["total"]
  for key in sorted(stats["keys"].iterkeys()):
    if (options.debug):
      print "%s,%d,%.2f" % (key, stats["keys"][key], stats["keys"][key] * 100 / stats["total"])
    else:
      print "%s,%d" % (key, stats["keys"][key])

def readArgs():
  parser = OptionParser()
  parser.add_option("-k", "--key", dest="keys", help="coma separated key fields")
  parser.add_option("-v", "--value", dest="value", help="value field")
  parser.add_option("-d", "--debug", action="store_true", default=False, dest="debug", help="show extra verbose info")
  return parser.parse_args()

if __name__ == '__main__':
  (options, args) = readArgs()
  keys = [int(i) - 1 for i in re.split(',', options.keys)]
  value = int(options.value) - 1
  readLines(keys, value)
  printStats()

