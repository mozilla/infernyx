
from sys import stdin, stdout
from csv import writer

csvout = writer(stdout)

print "date, locale, country_code, url_a, url_b, count"
for line in stdin:
    key, value = [eval(x) for x in line.split('\t')]
    key.extend(value)
    key = key[1:]
    csvout.writerow(key)