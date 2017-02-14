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
        parts['locale'] = parts['locale'][:12]
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


def parse_locale(parts, params):
    # skip illegal locales
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
    """If we have a 'click', 'block' or 'pin' action, just emit one record,
        otherwise it's an impression, emit all of the records"""
    tiles = parts.get('tiles')

    one = 1
    if parts.get('blacklisted', False):
        one = -1

    position = None
    vals = {'clicks': 0, 'impressions': 0, 'pinned': 0, 'blocked': 0,
            'sponsored': 0, 'sponsored_link': 0, 'newtabs': 0, 'enhanced': False}
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


def clean_activity_stream_session(parts, params):
    try:
        assert parts["client_id"]
        assert parts["addon_version"]
        assert parts["page"]
        assert parts["tab_id"]

        # check those required fields
        assert parts["load_reason"]
        assert parts["unload_reason"]
        assert 0 <= parts["session_duration"] < 2 ** 31

        # check those optional fields
        for f in ['max_scroll_depth', 'load_latency', 'highlights_size',
                  'total_history_size', 'total_bookmarks']:
            # populate the optional fields with default values if they are missing
            if f not in parts:
                parts[f] = -1
            else:
                assert parts[f] >= -1  # -1 is valid as it's the default for the integer type fields
        for f in ['experiment_id', 'session_id']:
            # populate the optional fields with default values if they are missing
            if f not in parts:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_activity_stream_mobile_sessiond(parts, params):
    try:
        assert parts["client_id"]
        assert parts["addon_version"]
        assert parts["page"]
        assert parts["tab_id"]

        # check those required fields
        assert parts["load_reason"]
        assert parts["unload_reason"]
        assert 0 <= parts["session_duration"] < 2 ** 32

        # check those optional fields
        for f in ['max_scroll_depth', 'load_latency', 'highlights_size',
                  'total_history_size', 'total_bookmarks']:
            # populate the optional fields with default values if they are missing
            if f not in parts:
                parts[f] = -1
            else:
                assert parts[f] >= -1  # -1 is valid as it's the default for the integer type fields
        for f in ['experiment_id', 'session_id']:
            # populate the optional fields with default values if they are missing
            if f not in parts:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_activity_stream_event(parts, params):
    try:
        assert parts["client_id"]
        assert parts["addon_version"]
        assert parts["page"]
        assert parts["tab_id"]

        # check those required fields
        assert parts["event"]

        # check those optional fields
        for f in ['action_position', 'source', 'experiment_id', 'session_id',
                  'url', 'recommender_type', 'highlight_type', 'provider', 'metadata_source']:
            # populate the optional fields with default values if they are missing
            if f not in parts:
                parts[f] = "n/a"
        # A hotfix for issue 1034 of activity stream version 1.1.1
        # The addon will send keys with null values for fields 'recommender_type' and 'highlight_type'
        # we need to replace those nulls as Inferno doesn't allow nulls in the key parts
        for f in ['recommender_type', 'highlight_type']:
            parts[f] = parts[f] or "n/a"
        yield parts
    except Exception:
        pass


def clean_activity_stream_performance(parts, params):
    try:
        assert parts["client_id"]
        assert parts["addon_version"]
        assert parts["tab_id"]

        # check those required fields
        assert parts["event"]
        assert parts['event_id']
        assert parts['source']
        # some addon versions might send floating point values by mistake, we do the conversion here
        parts["value"] = int(round(parts["value"]))
        assert 0 <= parts["value"] < 2 ** 31

        # check those optional fields
        for f in ['experiment_id', 'session_id', 'metadata_source']:
            # populate the optional fields with default values if they are missing
            if f not in parts:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_activity_stream_masga(parts, params):
    try:
        assert parts["client_id"]
        assert parts["addon_version"]
        assert parts["tab_id"]

        # check those required fields
        assert parts["event"]
        assert parts['source']
        # some addon versions might send floating point values by mistake, we do the conversion here
        parts["value"] = int(round(parts["value"]))
        # the client might send a unix timestamp sometimes, flag it as invalid by using a negetive number
        if parts["value"] >= 2 ** 31:
            parts["value"] = -1

        # check those optional fields
        for f in ['experiment_id', 'session_id']:
            # populate the optional fields with default values if they are missing
            if f not in parts:
                parts[f] = "n/a"
        yield parts
    except Exception:
        pass


def clean_shield_study_fields(parts, params):
    for f in ['tp_version']:
        # populate the optional fields with default values if they are missing
        if f not in parts:
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
    if "activity_stream_session" == parts.get("action", "") and "shield_variant" not in parts:
        yield parts


def activity_stream_session_filter(parts, params):
    if "activity_stream_session" == parts.get("action", "") and "shield_variant" not in parts:
        yield parts


def activity_stream_event_filter(parts, params):
    if "activity_stream_event" == parts.get("action", "") and "shield_variant" not in parts:
        yield parts


def activity_stream_performance_filter(parts, params):
    if "activity_stream_performance" == parts.get("action", "") and "shield_variant" not in parts:
        yield parts


def activity_stream_masga_filter(parts, params):
    if "activity_stream_masga_event" == parts.get("action", "") and "shield_variant" not in parts:
        yield parts


def ss_activity_stream_session_filter(parts, params):
    if "activity_stream_session" == parts.get("action", "") and "shield_variant" in parts:
        yield parts


def ss_activity_stream_event_filter(parts, params):
    if "activity_stream_event" == parts.get("action", "") and "shield_variant" in parts:
        yield parts


def ss_activity_stream_performance_filter(parts, params):
    if "activity_stream_performance" == parts.get("action", "") and "shield_variant" in parts:
        yield parts


def ss_activity_stream_masga_filter(parts, params):
    if "activity_stream_masga_event" == parts.get("action", "") and "shield_variant" in parts:
        yield parts
