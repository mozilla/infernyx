#!/usr/bin/env python
from optparse import OptionParser
from datetime import date, datetime, timedelta

from inferno.lib.settings import InfernoSettings
from inferno.lib.disco_ext import get_disco_handle


USAGE = """Usage:
        # delete all the tags that are more than 7 days old
        %prog -n 7

        # print all the tags that are more than 30 days old
        %prog -n 30 -d

        # delete with specific tags
        %prog -n 30 -t incoming:error -t incoming:info
        """


def main():
    parser = OptionParser(usage=USAGE)
    parser.add_option("-n", "--days",
                      dest="days", type="int", action="store", default=30,
                      help="specify how many days of data we want to keep")
    parser.add_option("-d", "--dryrun",
                      action="store_true", dest="dryrun", default=False,
                      help="print the to-be-deleted tags")
    parser.add_option("-t", "--tag", default=[],
                      action="append", dest="tags",
                      help="specify extra tags")
    options, _ = parser.parse_args()
    expire_data(options.days, options.dryrun, options.tags)


def expire_data(days, dryrun, extra_tags):
    settings = InfernoSettings()
    _, ddfs = get_disco_handle(settings["server"])
    tags = extract_tags_from_infernyx()
    tags += extra_tags
    to_delete = []
    date_lower = date.today() + timedelta(days=-days)
    try:
        all_tags = ddfs.list()
    except Exception as e:
        print "Can not fetch the tag list from ddfs: %s" % e
        return

    for tag in all_tags:
        try:
            prefix, ds = tag.rsplit(':', 1)
            tag_date = datetime.strptime(ds, "%Y-%m-%d").date()
        except:
            continue  # ignore the non-standard tag name
        if prefix in tags and tag_date < date_lower:
            to_delete.append(tag)

    to_delete.sort()  # delete tags with "incoming" first, then the "processed" ones
    if dryrun:
        if to_delete:
            print "Following tags will be deleted:\n"
            print "\n".join(to_delete)
        else:
            print "Nothing to be done"
    else:
        for tag in to_delete:
            try:
                print "Deleting tag: %s" % tag
                ddfs.delete(tag)
            except Exception as e:
                print "Failed to delete tag %s: %s" % (tag, e)


def extract_tags_from_infernyx():
    from infernyx.rules import RULES
    tags = set()
    for rule in RULES:
        for tag in rule.source_tags:
            tags.add(tag)
            if tag.startswith("incoming"):
                tags.add(tag.replace("incoming", "processed"))

    return tags


if __name__ == '__main__':
    main()
