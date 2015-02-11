#!/usr/bin/bash

locale="en-US"
country="US"

# extract distinct counts
zcat all_distinct.gz | grep "$locale,$country" | ./tools/histogram.py -k 5 -v 6 | tr "," " " | sort  -k1 -n | tr " " "," > "$locale.$country".hist
# extract tuples for locale+country
zcat all_tuples.gz | grep "$locale,$country" | sed -e 's/,www\./,/g' | ./tools/histogram.py -k 5,6 -v 7 -r | sort -t"," -k3 -nr > "$locale.$country".tuple_count

# separte sites from pairs
cat "$locale.$country".tuple_count | awk -F"," '{if($1 == $2) print $0;}' | cut -d"," -f2-3 > "$locale.$country".sites_count
cat "$locale.$country".tuple_count | awk -F"," '{if($1 != $2) print $0;}' > "$locale.$country".sites_pairs

# only pay attention to sites that occured at least 200 times
awk -F"," '{if ($2>=200) print $0}' < "$locale.$country".sites_count | sort -t"," -k2 -nr > "$locale.$country".sites_count.200

# compute Zs
cat "$locale.$country".sites_pairs | ./tools/computeZ.py -s "$locale.$country".sites_count.200 -x "$locale.$country".hist > "$locale.$country".Z

