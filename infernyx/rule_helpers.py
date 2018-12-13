def clean_data(parts, params, imps=True):
    import datetime
    try:
        parts['blacklisted'] = getattr(params, 'blacklisted', False)
        if imps:
            assert parts['tiles'][0] is not None
        if getattr(params, 'ip_pattern', False):
            ip = parts['ip'].split(',')[0].strip()
            assert params.ip_pattern.match(ip)
        assert datetime.datetime.fromtimestamp(parts['timestamp'] / 1000.0)
        if parts.get('action'):
            parts['action'] = parts['action'][:254]
        yield parts

    except Exception as e:
        pass


def count(parts, params):
    parts['count'] = 1
    yield parts


def parse_date(parts, params):
    from datetime import datetime

    try:
        dt = datetime.strptime(parts['date'], "%Y-%m-%d")
        parts['year'] = dt.year
        parts['month'] = dt.month
        parts['week'] = dt.isocalendar()[1]
        yield parts
    except:
        pass


def parse_time(parts, params):
    from datetime import datetime

    try:
        received_at = datetime.fromtimestamp(parts['timestamp'] / 1000.0)
        parts['hour'] = received_at.hour
        parts['minute'] = received_at.minute
        yield parts
    except:
        pass


def parse_locale(parts, params):
    try:
        parts['locale'] = parts['locale'][:12]
        # make sure locale starts with alphabetic characters only
        assert parts['locale'][0].isalpha()
        yield parts
    except:
        pass


def check_locale_whitelist(parts, params):
    # skip locales that are not on the whitelist
    try:
        if parts['locale'].lower() in params.locale_whitelist:
            yield parts
    except:
        pass


def parse_ip(parts, params):
    ips = parts.get('ip', None)
    try:
        ip = ips.split(',')[0].strip()
        resp = params.geoip_db.country(ip)
        parts['country_code'] = resp.country.iso_code or "ERROR"  # Note: resp might return None
    except Exception as e:
        #  print "Error parsing ip address: %s %s" % (ips, e)
        parts['country_code'] = 'ERROR'
    yield parts


def parse_ua(parts, params):
    from ua_parser import user_agent_parser
    ua = parts.get('ua', None)
    try:
        result_dict = user_agent_parser.Parse(ua)
        parts['os'] = result_dict['os']['family'][:64]
        # Do not overwrite "version" if provided
        if parts.get("version") is None:
            parts['version'] = ("%s.%s" % (result_dict['user_agent']['major'], result_dict['user_agent']['minor']))[:64]
        parts['browser'] = result_dict['user_agent']['family'][:64]
        parts['device'] = result_dict['device']['family'][:64]
    except:
        # print "Error parsing UA: %s" % ua
        parts.setdefault('os', 'n/a')
        parts.setdefault('version', 'n/a')
        parts.setdefault('browser', 'n/a')
        parts.setdefault('device', 'n/a')
    yield parts


def parse_tiles(parts, params):
    import sys
    """If we have a 'click', 'block', 'pin' or 'pocket' action, just emit
       one record, otherwise it's an impression, emit all of the records"""
    tiles = parts.get('tiles')

    one = 1
    if parts.get('blacklisted', False):
        one = -1

    position = None
    vals = {'clicks': 0, 'impressions': 0, 'pinned': 0, 'blocked': 0,
            'sponsored': 0, 'sponsored_link': 0, 'newtabs': 0, 'enhanced': False,
            'pocketed': 0}
    view = parts.get('view', sys.maxint)

    try:

        # now prepare values for emitting this particular event
        if parts.get('click') is not None:
            position = parts['click']
            vals['clicks'] = one
            tiles = [tiles[position]]
        elif parts.get('pin') is not None:
            position = parts['pin']
            vals['pinned'] = one
            tiles = [tiles[position]]
        elif parts.get('block') is not None:
            position = parts['block']
            vals['blocked'] = one
            tiles = [tiles[position]]
        elif parts.get('pocket') is not None:
            position = parts['pocket']
            vals['pocketed'] = one
            tiles = [tiles[position]]
        elif parts.get('sponsored') is not None:
            position = parts['sponsored']
            vals['sponsored'] = one
            tiles = [tiles[position]]
        elif parts.get('sponsored_link') is not None:
            position = parts['sponsored_link']
            vals['sponsored_link'] = one
            tiles = [tiles[position]]
        else:
            vals['impressions'] = one
            cparts = parts.copy()
            del cparts['tiles']
            cparts['newtabs'] = one
            yield cparts

        del parts['tiles']

        # emit all relavant tiles for this action
        for i, tile in enumerate(tiles):
            # print "Tile: %s" % str(tile)
            cparts = parts.copy()
            cparts.update(vals)

            # the position can be specified implicity or explicity
            if tile.get('pos') is not None:
                slot = tile['pos']
            elif position is None:
                slot = i
            else:
                slot = position
            assert position < 1024
            assert isinstance(slot, (long, int)), "invalid postion %s, of type %s" % (slot, type(slot))
            assert slot < 1024
            cparts['position'] = slot

            url = tile.get('url')
            if url:
                cparts['enhanced'] = True
                cparts['url'] = url

            tile_id = tile.get('id')
            if tile_id is not None and isinstance(tile_id, int) and tile_id < 1000000 and slot <= view:
                cparts['tile_id'] = tile_id
            yield cparts
    except:
        print "Error parsing tiles: %s" % str(tiles)


def parse_batch(parts, params):
    batch_mode = parts.pop("batch-mode", False)
    if batch_mode:
        payloads = parts.pop("payloads", [])
        try:
            for payload in payloads:
                payload.update(parts)
                yield payload
        except:
            print "Error parsing batch payloads %s" % str(payloads)
    else:
        yield parts


def parse_urls(parts, params):
    def combos(arr):
        for i, ela in enumerate(arr):
            rest = arr[i:]
            for elb in rest:
                if elb < ela:
                    yield elb, ela
                else:
                    yield ela, elb

    # only process 'impression' records
    if "view" in parts:
        tiles = parts.get('tiles')
        date = parts.get('date')
        locale = parts.get('locale')
        country_code = parts.get('country_code')

        urls = set(tile.get('url') for tile in tiles if tile.get('url'))
        for url_a, url_b in combos(list(urls)):
            # print date, locale, country_code, url_a, url_b
            yield {'date': date, 'locale': locale, 'country_code': country_code, 'url_a': url_a, 'url_b': url_b,
                   'count': 1}


def parse_distinct(parts, params):
    if "view" in parts:
        tiles = parts.get('tiles')
        date = parts.get('date')
        locale = parts.get('locale')
        country_code = parts.get('country_code')
        urls = set(tile.get('url') for tile in tiles if tile.get('url'))
        yield {'date': date, 'locale': locale, 'country_code': country_code, 'distinct_urls': len(urls), 'count': 1}


def parse_ip_clicks(parts, params):
    tiles = parts.get('tiles')

    try:
        del parts['tiles']
        cparts = parts.copy()
        cparts['impressions'] = 1 if not any(ind in parts for ind in
                                             ('click', 'pin', 'block', 'sponsored', 'sponsored_link')) else 0
        cparts['clicks'] = 1 if 'click' in parts else 0

        # only count ips if there is an explicit tile_id
        if any('id' in tile for tile in tiles):
            yield cparts

    except Exception as e:
        print "Error parsing tiles: %s %s" % (e, str(tiles))


def filter_clicks(keys, (imps, clicks), params, click_threshold=20, impression_threshold=250):
    if imps >= impression_threshold or clicks >= click_threshold:
        yield keys, [imps, clicks]


def filter_blacklist(parts, params):
    ip = parts.get('ip', 'n/a')
    if ip in params.blacklisted_ips:
        yield parts


def clean_activity_stream_mobile_session(parts, params):
    try:
        assert parts["client_id"]
        assert parts["app_version"]
        assert parts["build"]

        # check those required fields
        assert 0 <= parts["session_duration"] < 2 ** 31

        # check those optional fields
        for f in ['release_channel']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_activity_stream_mobile_event(parts, params):
    try:
        assert parts["client_id"]
        assert parts["app_version"]
        assert parts["page"]

        # check those required fields
        assert parts["event"]

        # check those optional fields
        for f in ['action_position', 'source', 'release_channel']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_firefox_onboarding_session(parts, params):
    import sys

    try:
        assert parts["client_id"]
        assert parts["addon_version"]

        # check those required fields
        assert parts["event"]
        assert parts["page"]
        assert parts["category"]
        assert parts["tour_source"]

        # check those required big integer fields
        for f in ['session_begin', 'session_end']:
            # cast to integer in case the client sends other types
            parts[f] = int(round(parts[f]))
            assert parts[f] < sys.maxint

        assert parts['session_end'] >= parts['session_begin']

        # check those optional integer fields
        for f in ['impression']:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                # some addon versions might send floating point values by mistake, we do the conversion here
                parts[f] = int(round(parts[f]))
                if parts[f] >= 2 ** 31 or parts[f] < 0:
                    parts[f] = -1

        for f in ['session_id', 'tour_type']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_firefox_onboarding_event(parts, params):
    import sys

    try:
        assert parts["client_id"]
        assert parts["addon_version"]

        # check those required fields
        assert parts["event"]
        assert parts["page"]
        assert parts["category"]

        # check those optional big integer fields
        for f in ['timestamp']:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                # cast to integer in case the client sends other types
                parts[f] = int(round(parts[f]))
                assert parts[f] < sys.maxint

        # check those optional integer fields
        for f in ['impression']:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                # some addon versions might send floating point values by mistake, we do the conversion here
                parts[f] = int(round(parts[f]))
                if parts[f] >= 2 ** 31 or parts[f] < 0:
                    parts[f] = -1

        for f in ['session_id', 'tour_id', 'tour_type', 'tour_source', 'bubble_state',
                  'notification_state']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_firefox_onboarding_session_v2(parts, params):
    import sys

    try:
        assert parts["client_id"]
        assert parts["addon_version"]

        # check those required fields
        assert parts["page"]
        assert parts["category"]
        assert parts["tour_type"]
        assert parts["type"]
        assert parts["parent_session_id"]
        assert parts["root_session_id"]

        # check those required big integer fields
        for f in ['session_begin', 'session_end']:
            # cast to integer in case the client sends other types
            parts[f] = int(round(parts[f]))
            assert parts[f] < sys.maxint

        assert parts['session_end'] >= parts['session_begin']

        for f in ['session_id']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_firefox_onboarding_event_v2(parts, params):
    import sys

    try:
        assert parts["client_id"]
        assert parts["addon_version"]

        # check those required fields
        assert parts["bubble_state"]
        assert parts["page"]
        assert parts["category"]
        assert parts["current_tour_id"]
        assert parts["logo_state"]
        assert parts["notification_state"]
        assert parts["parent_session_id"]
        assert parts["root_session_id"]
        assert parts["target_tour_id"]
        assert parts["tour_type"]
        assert parts["type"]

        # check those optional big integer fields
        for f in ['timestamp']:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                # cast to integer in case the client sends other types
                parts[f] = int(round(parts[f]))
                assert parts[f] < sys.maxint

        # check those optional integer fields
        for f in ['notification_impression', 'width']:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                # some addon versions might send floating point values by mistake, we do the conversion here
                parts[f] = int(round(parts[f]))
                if parts[f] >= 2 ** 31 or parts[f] < 0:
                    parts[f] = -1

        yield parts
    except Exception:
        pass


def clean_ping_centre_main(parts, params):
    try:
        # check those required fields
        for field in ["client_id", "release_channel", "event"]:
            assert field in parts

        # check those optional fields
        for field in ['value', 'shield_id']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(field, None) is None:
                parts[field] = "n/a"

        # check those optional integer fields
        for f in ["profile_creation_date"]:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                # some addon versions might send floating point values by mistake, we do the conversion here
                parts[f] = int(round(parts[f]))
                if parts[f] >= 2 ** 31 or parts[f] < 0:
                    parts[f] = -1

        yield parts
    except Exception:
        pass


def clean_shield_study_fields(parts, params):
    for f in ['tp_version']:
        # Populate the optional fields with default values if they are missing or with value "null"
        # This is necessary as Disco doesn't support "null"/"None" in the key part
        if parts.get(f, None) is None:
            parts[f] = "n/a"
    yield parts


def create_timestamp_str(parts, params):
    import datetime

    try:
        ts = datetime.datetime.fromtimestamp(parts["timestamp"] / 1000.0)
        parts['receive_at'] = ts.strftime('%Y-%m-%d %H:%M:%S')
        yield parts
    except Exception:
        pass


def application_stats_filter(parts, params):
    if not parts.get("action", "").startswith("activity_stream"):
        yield parts


def activity_stream_mobile_session_filter(parts, params):
    if "activity-stream-mobile-sessions" == parts.get("topic", ""):
        yield parts


def activity_stream_mobile_event_filter(parts, params):
    if "activity-stream-mobile-events" == parts.get("topic", ""):
        yield parts


def firefox_onboarding_session_filter(parts, params):
    if "firefox-onboarding-session" == parts.get("topic", ""):
        yield parts


def firefox_onboarding_event_filter(parts, params):
    if "firefox-onboarding-event" == parts.get("topic", ""):
        yield parts


def firefox_onboarding_session_filter_v2(parts, params):
    if "firefox-onboarding-session2" == parts.get("topic", ""):
        yield parts


def firefox_onboarding_event_filter_v2(parts, params):
    if "firefox-onboarding-event2" == parts.get("topic", ""):
        yield parts


def ping_centre_main_filter(parts, params):
    if "main" == parts.get("topic", ""):
        yield parts


def activity_stream_router_event_filter(parts, params):
    if "activity-stream-router" == parts.get("topic", ""):
        yield parts


# filters and processors for Activity Stream system addon
def assa_session_filter(parts, params):
    if "activity_stream_session" == parts.get("action", ""):
        yield parts


def assa_event_filter(parts, params):
    if "activity_stream_user_event" == parts.get("action", ""):
        yield parts


def assa_performance_filter(parts, params):
    if "activity_stream_performance_event" == parts.get("action", ""):
        yield parts


def assa_masga_filter(parts, params):
    if "activity_stream_undesired_event" == parts.get("action", ""):
        yield parts


def assa_impression_filter(parts, params):
    if "activity_stream_impression_stats" == parts.get("action", ""):
        yield parts


def timestamp_milli_to_micro(parts, params, columns=[]):
    for column in columns:
        parts[column] = int(parts[column] * 1000)
    yield parts


def timestamp_micro_to_milli(keys, value, params, columns=[]):
    key_parts = params.keysets["activity_stream_session_stats"]["key_parts"]
    for column in columns:
        index = key_parts.index(column)
        keys[index] = float(keys[index]) * 1e-3
    yield keys, value


def validate_uuid4(parts, params, fields=[]):
    from uuid import UUID

    try:
        for field in fields:
            if parts[field] == "n/a":
                continue
            UUID(parts[field], version=4)
        yield parts
    except:
        pass


def clean_assa_session(parts, params):
    import sys

    try:
        # check those required fields
        assert parts["client_id"]
        assert parts["addon_version"]
        assert parts["page"]
        assert parts["session_id"]
        # merge `perf` into `parts` if any
        parts.update(parts.pop("perf", {}))
        # merge `perf.topsites_icon_stats` into `parts` if any
        parts.update(parts.pop("topsites_icon_stats", {}))
        assert parts["load_trigger_type"]
        if parts["load_trigger_type"] not in ["unexpected", "first_window_opened",
                                              "session_restore", "menu_plus_or_keyboard",
                                              "url_bar", "refresh"]:
            parts["load_trigger_type"] = "invalid"

        # check those optional fields
        for f in ['release_channel', 'shield_id', 'region']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"

        # check those optional boolean fields, set it to False if missing
        for f in ['is_preloaded', 'is_prerendered']:
            parts[f] = bool(parts.get(f))

        # check those optional integer fields
        for f in ["session_duration", "user_prefs", "topsites_data_late_by_ms",
                  "highlights_data_late_by_ms", "screenshot_with_icon",
                  "screenshot", "tippytop", "rich_icon", "no_image", "topsites_pinned",
                  "profile_creation_date", "custom_screenshot", "topsites_search_shortcuts"]:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                # some addon versions might send floating point values by mistake, we do the conversion here
                parts[f] = int(round(parts[f]))
                if parts[f] >= 2 ** 31 or parts[f] < 0:
                    parts[f] = -1

        # check those floating point fields
        for f in ["load_trigger_ts", "visibility_event_rcvd_ts",
                  "topsites_first_painted_ts"]:
            if parts.get(f, None) is None:
                # TODO: increment the counters upon missing doubles
                parts[f] = -1.0
            else:
                # do NOT tolerate invalid values here
                assert -1.0 <= parts[f] < sys.float_info.max
        yield parts
    except Exception:
        pass


def clean_assa_event(parts, params):
    import json

    try:
        # check those required fields
        assert parts["client_id"]
        assert parts["addon_version"]
        assert parts["page"]
        assert parts["session_id"]
        assert parts["event"]
        # serialize the `value` object to JSON, use `{}` if it's missing
        value = parts.pop("value", {})
        assert isinstance(value, (dict, list))
        parts["value"] = json.dumps(value)

        for f in ['action_position', 'source', 'release_channel', 'shield_id']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"

        # check those optional integer fields
        for f in ["user_prefs", "profile_creation_date"]:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                # some addon versions might send floating point values by mistake, we do the conversion here
                parts[f] = int(round(parts[f]))
                if parts[f] >= 2 ** 31:
                    parts[f] = -1
        yield parts
    except Exception:
        pass


def clean_assa_performance(parts, params):
    try:
        # check those required fields
        assert parts["client_id"]
        assert parts["addon_version"]
        assert parts["event"]

        # check those optional integer fields
        for f in ["value", "user_prefs"]:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                # some addon versions might send floating point values by mistake, we do the conversion here
                parts[f] = int(round(parts[f]))
                if parts[f] >= 2 ** 31:
                    parts[f] = -1

        # check those optional string fields
        for f in ["page", "source", "event_id", "session_id", "release_channel", "shield_id"]:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_assa_masga(parts, params):
    try:
        # check those required fields
        assert parts["client_id"]
        # due to bug 1472038, we have to relax this condition, and treat addon_version as an optional field
        # TODO: re-enable this check once bug 1472038 gets fixed on client
        # assert parts["addon_version"]
        assert parts["event"]

        # check those optional integer fields
        for f in ["value", "user_prefs"]:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                # some addon versions might send floating point values by mistake, we do the conversion here
                parts[f] = int(round(parts[f]))
                if parts[f] >= 2 ** 31:
                    parts[f] = -1

        # check those optional string fields
        for f in ["page", "source", "session_id", "release_channel", "shield_id", "addon_version"]:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_assa_impression(parts, params):
    try:
        # check those required fields
        assert parts["client_id"]
        assert parts["addon_version"]
        assert parts["page"]

        if parts.get("source"):
            # Temporarily exclude all the impression pings from non-topstories sources, see Github issue #102
            # This is already fixed in AS, we still need this hot fix for the old versions of AS.
            assert parts["source"] == "TOP_STORIES"

        for f in ['source', 'release_channel', 'shield_id', 'region']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"

        # check those optional integer fields
        for f in ['user_prefs']:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                parts[f] = int(round(parts[f]))
                if parts[f] >= 2 ** 31:
                    parts[f] = -1

        # map impression_id, which has been added in Firefox 58, to client_id if provided
        if "impression_id" in parts:
            parts["client_id"] = parts["impression_id"]

        yield parts
    except Exception:
        pass


def clean_assa_router_event(parts, params):
    import json

    try:
        # check those required fields
        assert parts["impression_id"]
        assert parts["addon_version"]
        assert parts["event"]
        assert parts["action"]
        # treat client_id as impression_id if it's a valid uuid
        if parts.get("client_id", "n/a") != "n/a":
            parts["impression_id"] = parts["client_id"]
        # action is the actual source since source is now hardcoded in the current implementation
        parts["source"] = parts["action"]
        assert parts["message_id"]
        # If `value` is a dict, serialize it to JSON, use `{}` if it's missing, otherwise
        # just save the raw value
        value = parts.pop("value", {})
        if isinstance(value, (dict, list)):
            parts["value"] = json.dumps(value)
        else:
            parts["value"] = value if value is not None else 'n/a'

        for f in ['release_channel', 'shield_id']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"

        yield parts
    except Exception:
        pass


def watchdog_proxy_events_filter(parts, params):
    if "watchdog-proxy" == parts.get("topic", ""):
        yield parts


def clean_watchdog_proxy_event(parts, params):
    try:
        # check those required fields
        assert parts["event"]

        for f in ['consumer_name', 'watchdog_id', 'type', 'poller_id', 'photodna_tracking_id',
                  'worker_id']:
            # Populate the optional fields with default values if they are missing or with value "null"
            # This is necessary as Disco doesn't support "null"/"None" in the key part
            if parts.get(f, None) is None:
                parts[f] = "n/a"

        # check those optional integer fields
        for f in ['items_in_queue', 'items_in_progress', 'items_in_waiting', 'timing_sent',
                  'timing_received', 'timing_submitted']:
            if parts.get(f, None) is None:
                parts[f] = -1
            else:
                parts[f] = int(round(parts[f]))
                if parts[f] >= 2 ** 31:
                    parts[f] = -1

        # check those optional boolean fields, set it to False if missing
        for f in ['is_match', 'is_error']:
            parts[f] = bool(parts.get(f))

        yield parts
    except Exception:
        pass
