# Script to merge changes from a csv file
import codecs
import os
import glob
import collections
import sys
import csv
import json
from urllib.parse import urlparse

# transitland.onestop_id	
# transitland.link	
# transitland.spec	
# mdb.source_id	
# transitfeeds.provider_id/feed_id	
# mdb.data_type	
# mdb.location.country_code	
# mdb.location.subdivision_name	
# mdb.location.municipality	
# mdb.provider	
# mdb.urls.direct_download	
# Outdated URL	
# Update URL in Transitland? 	
# Create New Agency Page? 	
# Trillium Past or Present Feed ?	
# Mark Unstable	Archive Page	
# mdb.urls.authentication_type	
# mdb.urls.authentication_info	
# mdb.urls.api_key_parameter_name	
# mdb.urls.license

CSV_FILE = sys.argv[1]
check_cols = ['feed_onestop_id']
check_rt_urls = []
check_rt_auth = []
check_rt_cols = []
# check_cols = ['us_ntd_id','rt_feed','realtime_trip_updates','realtime_vehicle_positions','realtime_alerts','type','info_url','param_name'] # ,'notes'
# check_rt_urls = ['realtime_trip_updates','realtime_vehicle_positions','realtime_alerts']
# check_rt_auth = ['type','info_url', 'param_name']
# check_rt_cols = check_rt_urls + check_rt_auth

def apply_change(ent, c):
    print("applying change:", c)
    if c[0] == 'set_tag':
        ent['tags'] = ent.get('tags') or {}
        ent['tags'][c[2]] = c[3]
    elif c[0] == 'set_url':
        ent['urls'] = ent.get('urls')or {}
        ent['urls'][c[2]] = c[3]
    elif c[0] == 'set_auth':
        ent['authorization'] = ent.get('authorization') or {}
        ent['authorization'][c[2]] = c[3]
    elif c[0] == 'add_associated_feed':
        ent['associated_feeds'] = ent.get('associated_feeds') or []
        ent['associated_feeds'].append({'feed_onestop_id':c[2]})
    elif c[0] == 'new_feed':
        pass
    elif c[0] == 'new_operator':
        pass
    else:
        raise Exception("unknown change:", c)

########################
########################
########################
########################

# Build index of feeds and operators
operators = {}
feeds = {}
filenames = set(os.path.basename(i) for i in glob.glob(os.path.join("*.dmfr.json")))
for fn in filenames:
    with open(fn) as f:
        data = json.load(f)
    fn = os.path.basename(fn)
    for op in data.get('operators', []):
        osid = op['onestop_id']
        if osid in operators:
            raise Exception.new("op already exists:", osid)
        op['file'] = fn
        operators[osid] = op
    for feed in data.get('feeds', []):
        osid = feed['id']
        if osid in feeds:
            raise Exception.new("feed already exists:", osid)
        feed['file'] = fn
        feeds[osid] = feed
        for op_update in feed.get('operators', []):
            op_osid = op_update['onestop_id']
            if op_osid in operators:
                print("updating op:", op_osid, op_update)
            op = operators.get(op_osid) or {'onestop_id': op_osid, 'associated_feeds':[], 'tags':{}}
            op['tags'] = op.get('tags', {})
            op['tags'].update(op_update.get('tags', {}))
            op['associated_feeds'] = op.get('associated_feeds') or []
            for a in op_update.get('associated_feeds', []):
                op['associated_feeds'].append(a)
            op['file'] = fn
            operators[op_osid] = op


# Process change CSV
changeset = []
with open(CSV_FILE) as f:
    reader = csv.DictReader(f)
    for row in reader:
        osid = row.get("operator_onestop_id")
        fsid = row.get("feed_onestop_id")

        if not osid and not fsid:
            continue

        # check if this row provides sufficient data to update a record
        check = list(filter(lambda x:x, map(row.get, check_cols)))
        if not check:
            print("\n=====", osid, fsid)
            print("no data to update")
            continue

        # check if we have RT data to update
        check_rt = list(filter(lambda x:x, map(row.get, check_rt_cols)))
        if not check_rt:
            print("\n=====", osid, fsid)
            print("no rt data to update... todo: check ntd, etc")
            continue
        
        print("\n=====", osid, fsid)

        op = operators.get(osid)
        feed = feeds.get(fsid)

        # check if rt feed exists
        feed = feeds.get(fsid)
        if fsid and not feed:
            feed = {'id': fsid, 'urls': {}, 'authorization': {}, 'spec': 'gtfs'}
            if op and op.get('file'):
                print("setting new feed", a, "file to operator file:", op.get('file'))
                feed['file'] = op.get('file')
            else:
                for key in check_rt_urls:
                    rturl = row.get(key)
                    if rturl:
                        print(rturl)
                        ap = urlparse(rturl)
                        feed['file'] = ap.hostname + '.dmfr.json'
                        break
            if not feed.get('file'):
                feed['file'] = 'unknown.dmfr.json'
            feeds[a] = feed
            changeset.append(('new_feed', a, feed['file']))

        # check if operator exists
        if osid and not op:
            # TODO: check associated_feeds
            op = {'onestop_id': osid, 'associated_feeds': []}
            # where should we put this?
            changeset.append(('new_operator', osid, feed['file']))
            operators[osid] = op
            print("new operator:", osid, op)

        # check feed association
        found_feed = False
        for f in op.get('associated_feeds', []):
            if f.get('feed_onestop_id') == feed['id']:
                found_feed = True
        if not found_feed:
            print('add associated feed:', osid, "->", rt_feed["id"])
            changeset.append(('add_associated_feed', osid, rt_feed["id"]))

        # check operator fields
        if row.get('us_ntd_id'):
            op['tags'] = op.get('tags') or {}
            a = op['tags'].get('us_ntd_id') or None
            b = row.get('us_ntd_id') or None
            if a != b and b:
                changeset.append(('set_tag', osid, 'us_ntd_id', b))
                print("operator us_ntd_id updated:", a, "->", b)

        # check feed fields
        feed_updated = False
        feed_urls = rt_feed.get('urls', {})
        for key in check_rt_urls:
            if feed_urls.get(key) != row.get(key) and row.get(key):
                changeset.append(('set_url', rt_feed['id'], key, row.get(key)))
                print("updated rt url:", key, feed_urls.get(key), "->", row.get(key))

        feed_auth = rt_feed.get('authorization', {})
        for key in check_rt_auth:
            a = feed_auth.get(key)
            b = row.get(key) or None
            if a != b:
                changeset.append(('set_auth', rt_feed['id'], key, b))
                print("updated rt auth:", key, a, "->", b)

# Print summary
changes_by_key = collections.defaultdict(list)
changes_by_file = collections.defaultdict(list)
new_files = set()
print("\nchangesets...")
for c in changeset:
    print(c)
    changes_by_key[c[1]].append(c)
    if c[0] == "new_feed" or c[0] == "new_operator":
        new_files.add(c[2])
print("new files:", new_files)

# Apply changesets
# changed = set()
# for fn in set(filenames) | new_files:
#     print("\nprocessing...", fn)
#     data = {}
#     updated = False
#     try:
#         with open(fn) as f:
#             data = json.load(f)
#     except:
#         pass
#     fn = os.path.basename(fn)
#     data['feeds'] = data.get('feeds') or []
#     data['operators'] = data.get('operators') or []
#     # new feeds
#     for c in [i for i in changeset if i[0] == 'new_feed' and i[2] == fn]:
#         print("NEW FEED:", c)
#         feed = {'id': c[1], 'spec':'gtfs-rt'}
#         apply_change(feed, c)
#         data['feeds'].append(feed)
#         updated = True
#     # new operators
#     for c in [i for i in changeset if i[0] == 'new_operator' and i[2] == fn]:
#         op = {'onestop_id': c[1], 'name': 'TODO'}
#         apply_change(op, c)
#         data['operators'].append(op)
#         updated = True

#     # update entities
#     for op in data.get('operators', []):
#         for c in changes_by_key[op['onestop_id']]:
#             apply_change(op, c)
#             updated = True
#     for feed in data.get('feeds', []):
#         for c in changes_by_key[feed['id']]:
#             apply_change(feed, c)
#             updated = True
#         for op in feed.get('operators', []):
#             for c in changes_by_key[op['onestop_id']]:
#                 apply_change(op, c)
#                 updated = True

#     if updated:
#         with codecs.open(fn, 'w', encoding='utf-8') as f:
#             json.dump(data, f, ensure_ascii=False, indent=2)        
    
