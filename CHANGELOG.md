0.2.8
=====
* process profile_creation_date for ping_centre_main

0.2.7
=====
* process activity stream router ping

0.2.6
=====
* disable firefox onboarding session2 job

0.2.5
=====
* handle database connection errors

0.2.4
=====
* fix logging in database.py

0.2.3
=====
* optimize result_processor in database.py

0.2.2
=====
* process value for assa_events_daily

0.2.1
=====
* add support for multi-table inserts
* add a new destination table for assa_sessions_daily

0.2.0
=====
* fix the manifest naming issue in result processor 

0.1.99
======
* fix an issue introduced in 0.1.98

0.1.98
======
* use unix gzip to replace the python gzip
* use multifile COPY for AWS Redshift

0.1.97
======
* fix uuid validation so that it recognizes "n/a"

0.1.96
======
* clean up tmp files upon exception in result_processor
* process custom_screenshot, topsites_pinned, profile_creation_date, region for activity stream

0.1.95
======
* add uuid validations

0.1.94
======
* process firefox onboarding v2

0.1.93
======
* check invalid timestamps for firefox onboarding

0.1.92
======
* gzip the database result processor

0.1.91
======
* use ujson in the result processor for better performance

0.1.90
======
* process various new columns for firefox onboarding

0.1.89
======
* process impression_id for AS impression stats
* remove rules for activity stream test pilot and test pilot

0.1.88
======
* process no_image for AS session

0.1.87
======
* process is_preloaded, is_prerendered, topsites_data_late_by_ms, highlights_data_late_by_ms, and topsites_icon_stats from AS session
* add hour and minute for AS impression_stats

0.1.86
======
* process ping_centre_main

0.1.85
======
* process shield_id for activity stream tables
* do not overwrite "version" if provided

0.1.84
======
* exclude all the non-topstories impressions for activity stream system addon

0.1.83
======
* remove tour_source from firefox onboarding events

0.1.82
======
* processs session_begin, session_end, and tour_source for firefox onboarding

0.1.81
======
* process release_channel for activity stream tables
* process firefox onboarding pings

0.1.80
======
* process impression pings for activity stream system addon

0.1.79
======
* process user_prefs for activity stream system addon pings

0.1.78
======
* handle topsites_first_painted_ts for assa_session pings

0.1.77
======
* handle double fields to avoid precision loss for assa_session pings

0.1.76
======
* handle the nested "perf" object for assa_session pings

0.1.75
======
* add a new rule to process pings from activity stream system addon

0.1.74
======
* add support for batch mode payloads and enable it with Activity Stream and Ping-centre

0.1.73
======
* save experiment_id to activity_stream_impression_daily

0.1.72
======
* process impression logs for activity stream

0.1.71
======
* process "topsites_lowresicon" and "topsites_pinned" for activity stream

0.1.70
======
* process "topsites" stats and "user_prefs" for activity stream

0.1.69
======
* treat the "raw" field as optional in Test Pilot pings

0.1.68
======
* fix the missing "event_id" in the masga ping

0.1.67
======
* fix the "null" values in the optional fields

0.1.66
======
* fix the "raw" filed for Test Pilot pings

0.1.65
======
* rename "raw" to "raw_ping" for Test Pilot pings
* add "release_channel" to Activity Stream mobile

0.1.64
======
* process testpilots pings from Ping-Centre
* process Activity Stream Mobile pings from Ping-Centre

0.1.63
======
* fix float values in the performance ping of activity stream

0.1.62
======
* fix import logic for required_modules

0.1.61
======
* bugfix for 2^31 maximums for signed ints in redshift


0.1.59
======
* populate the default value for the `event_id` field in `masga`

0.1.58
======
* process highlights_size for activity stream session pings
* process undesired states pings for activity stream

0.1.57
======
* process tp_version for activity stream shield study

0.1.56
======
* process shield study pings for activity stream

0.1.55
======
* add integer overflow cleaning to activity stream 

0.1.54
======
* process and persist the metadata_source for activity stream

0.1.53
======
* process and persist the share_provider and highlight_type for activity stream

0.1.52
======
* process and persist the recommendation ping for activity stream

0.1.51
======
* add a keyset for application_stats to process activity_stream_performance pings

0.1.50
======
* include the expire_ddfs_da.py into the shipping script

0.1.49
======
* add a script to expire data in ddfs

0.1.48
======
* hotfix: wrap module level funcitons into filters

0.1.47
======
* add a new keyset "activity_stream_event" for rule application_stats
* rename the keyset "activity_stream_stats" to "activity_stream_session"

0.1.46
======
* fix the missing country_code in activity_stream_stats
* add load_latency to activity_stream_stats
* change tab_id type from str to int
* change click_position from str to int

0.1.45
======
* moved the session_duration to key_parts for activity_stream_stats

0.1.44
======
* added a keyset for Activity Stream in rule application_stats

0.1.43
======
* added validation for the position field

0.1.42
======

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
