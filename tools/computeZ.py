#!/usr/bin/python
import sys
import re
import logging
from optparse import OptionParser
from math import sqrt

total = 0.0
histogram = [0];
sites = {}

def readHist(file):
  global total
  f = open(file, "r")
  line = f.readline()
  while(len(line) > 0):
    chunk = re.split(',', line)
    count = int(chunk[1]) * 1.0
    total += count
    histogram.append(count)
    line = f.readline()

def computeFreqMean(freq, site):
  mean = 0
  for i in range(1,len(histogram)):
    mean += histogram[i] * (1 - (1 - freq) ** i)
  return mean  

def estimateFreq(siteCount, site):
  freq = 0.5
  left =  0.0000001
  right = 0.9999999
  mean = 0
  while ((right - left) > 0.0000001):
    mean = computeFreqMean(freq, site)
    if (mean < siteCount):
      left = freq
    else:
      right = freq
    freq = (left + right) / 2;
  return freq

def computeDistrib(site1, site2):
  freq1 = sites[site1]
  freq2 = sites[site2]
  var = 0
  mean = 0
  for i in range(2, len(histogram)):
    mean += histogram[i] * (1 - (1 - freq1) ** i) * (1 - (1 - freq2) ** (i-1))  
    var  += histogram[i] * (1 - (1 - freq1) ** i) * (1 - (1 - freq2) ** (i-1)) * ((1 - freq2) ** (i-1))
  #print site1, site2, mean , var
  #print "=========="
  return mean, var

def readSites(file):
  f = open(file, "r")
  line = f.readline()
  while(len(line) > 0):
    chunk = re.split(',', line)
    site = chunk[0]
    count = int(chunk[1]) * 1.0
    sites[site] = estimateFreq(count, site)
    line = f.readline()

def readLines():
  line = sys.stdin.readline()
  while (len(line) > 0):
    try:
      site1, site2, count = re.split(',', line)
      if (site1 in sites and site2 in sites):
        mean, var = computeDistrib(site1, site2)
        count = int(count)
        Z = (count - mean) / sqrt(var)
        print "%s,%s,%d,%.2f" % (site1, site2, count, Z)
        mean, var = computeDistrib(site2, site1)
        Z = (count - mean) / sqrt(var)
        print "%s,%s,%d,%.2f" % (site2, site1, count, Z)
    except Exception as e:
      logging.error("Error processing line '%s': %s" % (line, e))
    line = sys.stdin.readline()

def printStats():
  pass

def readArgs():
  parser = OptionParser()
  parser.add_option("-s", "--sites", dest="siteFile", help="site counts")
  parser.add_option("-x", "--hist", dest="histFile", help="history size historgram")
  return parser.parse_args()

if __name__ == '__main__':
  (options, args) = readArgs()
  readHist(options.histFile)
  readSites(options.siteFile)
  readLines()
  #readLines(keys, value)
  #printStats()

