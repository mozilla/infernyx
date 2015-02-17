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