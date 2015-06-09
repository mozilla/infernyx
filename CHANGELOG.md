0.1.41
======

* adding an ip_click_counter daily job to detect suspicious IP addresses
* adding an blacklisted_impression_stats daily job to negate traffic from suspicious IPs
* note that suspicious IPs are tracked for a maximum of 7 days

0.1.40
======

fixed double IP address bug in data cleaner

0.1.37
======

- added csdash.py - a nightly script to update the Metrics Dashboard

**Note**
this patch expects new variables defined in the config_infernyx module:

RS_PORT = 5432
DASH_USER = 'read_only'
DASH_PASSWORD = <password for redshift user>
DASH_BUCKET = 'net-mozaws-prod-metrics-data'
DASH_KEY_ID = <access key for dashboard s3 bucket>
DASH_ACCESS_KEY = <secret key for s3 bucket>


0.1.35
======

- contains unit test suite
- bug fixes or parse_tiles and parse_urls

0.1.33
======

- bugfix for URL processing - only storing the netloc instead of entire url

0.1.29
======

- bugfix for setup.py - misspelled postgres driver module

0.1.28
======

- adding rule for tracking application_stats, which include all 'fetch' events in Onyx including errors

0.1.32
======

- this feature adds 'site' stats functionality, both as a keyset when analysing impression_stats, and as a separate 
 daily job to process the combinations of sites that appear on the same screen (the 'site_tuples' rule)