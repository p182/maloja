from . import optimized as dbo
from .utils import normalize
from .. import utilities
from ..cleanup import CleanerAgent, CollectorAgent
from ..globalconf import datadir

from doreah import settings, tsv
from doreah.logging import log


from sortedcontainers import SortedDict
from threading import Lock
import psutil
from collections import namedtuple
import lru
import datetime


Track = namedtuple("Track",["artists","title"])
Scrobble = namedtuple("Scrobble",["track","timestamp","album","duration","saved"])
# album is saved in the scrobble because it's not actually authorative information
# about the track, just info that was sent with this scrobble


dblock = Lock() #global database lock
lastsync = 0
cla = CleanerAgent()
coa = CollectorAgent()


# AUTHORATIVE INFORMATION
SCROBBLES = SortedDict()	# timestamp -> tuple
ARTISTS = {}	# normalized -> name
TRACKS = {}	# normalized -> tuple

ISSUES = {}


####
## Getting dict representations of database objects
####

def get_scrobble_dict(o):
	track = get_track_dict(TRACKS[o.track])
	return {"artists":track["artists"],"title":track["title"],"time":o.timestamp,"album":o.album,"duration":o.duration}

def get_artist_dict(o):
	return o
	#technically not a dict, but... you know

def get_track_dict(o):
	artists = [get_artist_dict(ARTISTS[a]) for a in o.artists]
	artists.sort()
	# we want the order of artists to be deterministic so when we update files
	# with new rules a diff can see what has actually been changed
	return {"artists":artists,"title":o.title}

####
## Getting database objects from dict representations
####

def get_artist_tuple(name):

	obj = name
	obj_normalized = normalize(name)

	if obj_normalized in ARTISTS:
		return ARTISTS[obj_normalized]

	else:
		ARTISTS[obj_normalized] = obj

		# with a new artist added, we might also get new artists that they are credited as
		cr = coa.getCredited(name)
		coa.updateIDs(ARTISTS)

		return ARTISTS[obj_normalized]

def get_track_tuple(artists,title):

	artistset = frozenset([get_artist_tuple(a) for a in artists])
	obj = Track(artists=artistset,title=title)
	obj_normalized = Track(artists=artistset,title=normalize(title))

	if obj_normalized in TRACKS:
		return TRACKS[obj_normalized]
	else:
		TRACKS[obj_normalized] = obj
		return TRACKS[obj_normalized]




def createScrobble(artists,title,time,album=None,duration=None,volatile=False):

	if len(artists) == 0 or title == "":
		return {}

	dblock.acquire()

	trackobj = get_track_tuple(artists,title)

	# idempotence
	if time in SCROBBLES and SCROBBLES[time].track == trackobj:
		dblock.release()

	else:
		# timestamp as unique identifier
		while (time in SCROBBLES):
			time += 1

		scrobbleobj = Scrobble(trackobj,time,album,duration,volatile)
		SCROBBLES[time] = scrobbleobj

		register_scrobbletime(time)
		invalidate_caches()
		dblock.release()

		proxy_scrobble_all(artists,title,time)

	return get_track_dict(trackobj)


# this will never be called from different threads, so no lock
def readScrobble(artists,title,time):
	while (time in SCROBBLES):
		time += 1
	track = get_track_tuple(artists,title)
	obj = Scrobble(track,time,None,None,True)
	SCROBBLES[time] = obj









def start_db():
	log("Starting database...")
	global lastsync
	lastsync = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
	build_db()
	log("Database reachable!")

def build_db():


	log("Building database...")

	global SCROBBLES, ARTISTS, TRACKS

	SCROBBLES = SortedDict()
	ARTISTS = {}
	TRACKS = {}


	# parse files
	db = tsv.parse_all(datadir("scrobbles"),"int","string","string",comments=False)
	#db = parseAllTSV("scrobbles","int","string","string",escape=False)
	for sc in db:
		artists = sc[1].split("␟")
		title = sc[2]
		time = sc[0]

		readScrobble(artists,title,time)


	# inform malojatime module about earliest scrobble
	if len(SCROBBLES) > 0: register_scrobbletime(SCROBBLES[0].timestamp)


	#start regular tasks
	utilities.update_medals()
	utilities.update_weekly()
	utilities.send_stats()


	global ISSUES
	ISSUES = check_issues()

	log("Database fully built!")



# Saves all cached entries to disk
def sync():

	# all entries by file collected
	# so we don't open the same file for every entry
	entries = {}

	for timestamp,scr in SCROBBLES.items():
		if not scr.saved:

			t = get_scrobble_dict(scr)

			date = datetime.date.fromtimestamp(t["time"])
			monthcode = str(date.year) + "_" + str(date.month)

			artists = "␟".join(t["artists"])
			album = t["album"] or "-"
			duration = t["duration"] or "-"

			assert timestamp == t["time"]

			entry = [str(timestamp),artists,t["title"],album,duration]


			entries.setdefault(monthcode,[]).append(entry)

			SCROBBLES[timestamp] = Scrobble(*scr[:-1],True)
			# save copy with last tuple entry set to true


	for e in entries:
		tsv.add_entries(datadir("scrobbles/" + e + ".tsv"),entries[e],comments=False)



	global lastsync
	lastsync = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
	#log("Database saved to disk.")

	# save cached images
	#saveCache()




import copy

if settings.get_settings("USE_DB_CACHE"):
	def db_query(**kwargs):
		return db_query_cached(**kwargs)
	def db_aggregate(**kwargs):
		return db_aggregate_cached(**kwargs)
else:
	def db_query(**kwargs):
		return db_query_full(**kwargs)
	def db_aggregate(**kwargs):
		return db_aggregate_full(**kwargs)


csz = settings.get_settings("DB_CACHE_ENTRIES")
cmp = settings.get_settings("DB_MAX_MEMORY")

# replace lru with memoization?

cache_query = lru.LRU(csz)
cache_query_perm = lru.LRU(csz)
cache_aggregate = lru.LRU(csz)
cache_aggregate_perm = lru.LRU(csz)

perm_caching = settings.get_settings("CACHE_DATABASE_PERM")
temp_caching = settings.get_settings("CACHE_DATABASE_SHORT")

cachestats = {
	"cache_query":{
		"hits_perm":0,
		"hits_tmp":0,
		"misses":0,
		"objperm":cache_query_perm,
		"objtmp":cache_query,
		"name":"Query Cache"
	},
	"cache_aggregate":{
		"hits_perm":0,
		"hits_tmp":0,
		"misses":0,
		"objperm":cache_aggregate_perm,
		"objtmp":cache_aggregate,
		"name":"Aggregate Cache"
	}
}

from doreah.regular import runhourly

@runhourly
def log_stats():
	logstr = "{name}: {hitsperm} Perm Hits, {hitstmp} Tmp Hits, {misses} Misses; Current Size: {sizeperm}/{sizetmp}"
	for s in (cachestats["cache_query"],cachestats["cache_aggregate"]):
		log(logstr.format(name=s["name"],hitsperm=s["hits_perm"],hitstmp=s["hits_tmp"],misses=s["misses"],
		sizeperm=len(s["objperm"]),sizetmp=len(s["objtmp"])),module="debug")

def db_query_cached(**kwargs):
	global cache_query, cache_query_perm
	key = utilities.serialize(kwargs)

	eligible_permanent_caching = (
		"timerange" in kwargs and
		not kwargs["timerange"].active() and
		perm_caching
	)
	eligible_temporary_caching = (
		not eligible_permanent_caching and
		temp_caching
	)

	# hit permanent cache for past timeranges
	if eligible_permanent_caching and key in cache_query_perm:
		cachestats["cache_query"]["hits_perm"] += 1
		return copy.copy(cache_query_perm.get(key))

	# hit short term cache
	elif eligible_temporary_caching and key in cache_query:
		cachestats["cache_query"]["hits_tmp"] += 1
		return copy.copy(cache_query.get(key))

	else:
		cachestats["cache_query"]["misses"] += 1
		result = db_query_full(**kwargs)
		if eligible_permanent_caching: cache_query_perm[key] = result
		elif eligible_temporary_caching: cache_query[key] = result

		if use_psutil:
			reduce_caches_if_low_ram()

		return result


def db_aggregate_cached(**kwargs):
	global cache_aggregate, cache_aggregate_perm
	key = utilities.serialize(kwargs)

	eligible_permanent_caching = (
		"timerange" in kwargs and
		not kwargs["timerange"].active() and
		perm_caching
	)
	eligible_temporary_caching = (
		not eligible_permanent_caching and
		temp_caching
	)

	# hit permanent cache for past timeranges
	if eligible_permanent_caching and key in cache_aggregate_perm:
		cachestats["cache_aggregate"]["hits_perm"] += 1
		return copy.copy(cache_aggregate_perm.get(key))

	# hit short term cache
	elif eligible_temporary_caching and key in cache_aggregate:
		cachestats["cache_aggregate"]["hits_tmp"] += 1
		return copy.copy(cache_aggregate.get(key))

	else:
		cachestats["cache_aggregate"]["misses"] += 1
		result = db_aggregate_full(**kwargs)
		if eligible_permanent_caching: cache_aggregate_perm[key] = result
		elif eligible_temporary_caching: cache_aggregate[key] = result

		if use_psutil:
			reduce_caches_if_low_ram()

		return result

def invalidate_caches():
	global cache_query, cache_aggregate
	cache_query.clear()
	cache_aggregate.clear()
	log("Database caches invalidated.")

def reduce_caches(to=0.75):
	global cache_query, cache_aggregate, cache_query_perm, cache_aggregate_perm
	for c in cache_query, cache_aggregate, cache_query_perm, cache_aggregate_perm:
		currentsize = len(c)
		if currentsize > 100:
			targetsize = max(int(currentsize * to),10)
			c.set_size(targetsize)
			c.set_size(csz)

def reduce_caches_if_low_ram():
	ramprct = psutil.virtual_memory().percent
	if ramprct > cmp:
		log("{prct}% RAM usage, reducing caches!".format(prct=ramprct),module="debug")
		ratio = (cmp / ramprct) ** 3
		reduce_caches(to=ratio)

####
## Database queries
####



# Queries the database
def db_query_full(artist=None,artists=None,title=None,track=None,since=None,to=None,within=None,timerange=None,associated=False,max_=None):

	(since, to) = time_stamps(since=since,to=to,within=within,range=timerange)

	# this is not meant as a search function. we *can* query the db with a string, but it only works if it matches exactly
	# if a title is specified, we assume that a specific track (with the exact artist combination) is requested
	# if not, duplicate artist arguments are ignored

	#artist = None

	if artist is not None and isinstance(artist,str):
		artist = ARTISTS.index(artist)

	# artists to numbers
	if artists is not None:
		artists = set([(ARTISTS.index(a) if isinstance(a,str) else a) for a in artists])

	# track to number
	if track is not None and isinstance(track,dict):
		trackartists = set([(ARTISTS.index(a) if isinstance(a,str) else a) for a in track["artists"]])
		track = TRACKS.index((frozenset(trackartists),track["title"]))
		artists = None

	#check if track is requested via title
	if title!=None and track==None:
		track = TRACKS.index((frozenset(artists),title))
		artists = None

	# if we're not looking for a track (either directly or per title artist arguments, which is converted to track above)
	# we only need one artist
	elif artist is None and track is None and artists is not None and len(artists) != 0:
		artist = artists.pop()


	# db query always reverse by default

	result = []

	i = 0
	for s in scrobbles_in_range(since,to,reverse=True):
		if i == max_: break
		if (track is None or s[0] == track) and (artist is None or artist in TRACKS[s[0]][0] or associated and artist in coa.getCreditedList(TRACKS[s[0]][0])):
			result.append(get_scrobble_dict(s))
			i += 1

	return result

	# pointless to check for artist when track is checked because every track has a fixed set of artists, but it's more elegant this way


# Queries that... well... aggregate
def db_aggregate_full(by=None,since=None,to=None,within=None,timerange=None,artist=None):


	(since, to) = time_stamps(since=since,to=to,within=within,range=timerange)

	if isinstance(artist, str):
		artist = ARTISTS.index(artist)

	if (by=="ARTIST"):
		#this is probably a really bad idea
		#for a in ARTISTS:
		#	num = len(db_query(artist=a,since=since,to=to))
		#

		# alright let's try for real
		charts = {}
		#for s in [scr for scr in SCROBBLES if since < scr[1] < to]:
		for s in scrobbles_in_range(since,to):
			artists = TRACKS[s[0]][0]
			for a in coa.getCreditedList(artists):
				# this either creates the new entry or increments the existing one
				charts[a] = charts.setdefault(a,0) + 1

		ls = [{"artist":get_artist_dict(ARTISTS[a]),"scrobbles":charts[a],"counting":[arti for arti in coa.getAllAssociated(ARTISTS[a]) if arti in ARTISTS]} for a in charts]
		ls.sort(key=lambda k:k["scrobbles"],reverse=True)
		# add ranks
		for rnk in range(len(ls)):
			if rnk == 0 or ls[rnk]["scrobbles"] < ls[rnk-1]["scrobbles"]:
				ls[rnk]["rank"] = rnk + 1
			else:
				ls[rnk]["rank"] = ls[rnk-1]["rank"]
		return ls

	elif (by=="TRACK"):
		charts = {}
		#for s in [scr for scr in SCROBBLES if since < scr[1] < to and (artist==None or (artist in TRACKS[scr[0]][0]))]:
		for s in [scr for scr in scrobbles_in_range(since,to) if (artist is None or (artist in TRACKS[scr[0]][0]))]:
			track = s[0]
			# this either creates the new entry or increments the existing one
			charts[track] = charts.setdefault(track,0) + 1

		ls = [{"track":get_track_dict(TRACKS[t]),"scrobbles":charts[t]} for t in charts]
		ls.sort(key=lambda k:k["scrobbles"],reverse=True)
		# add ranks
		for rnk in range(len(ls)):
			if rnk == 0 or ls[rnk]["scrobbles"] < ls[rnk-1]["scrobbles"]:
				ls[rnk]["rank"] = rnk + 1
			else:
				ls[rnk]["rank"] = ls[rnk-1]["rank"]
		return ls

	else:
		#return len([scr for scr in SCROBBLES if since < scr[1] < to])
		return len(list(scrobbles_in_range(since,to)))


# Search for strings
def db_search(query,type=None):
	if type=="ARTIST":
		results = []
		for a in ARTISTS:
			#if query.lower() in a.lower():
			if simplestr(query) in simplestr(a):
				results.append(a)

	if type=="TRACK":
		results = []
		for t in TRACKS:
			#if query.lower() in t[1].lower():
			if simplestr(query) in simplestr(t[1]):
				results.append(get_track_dict(t))

	return results















####
## Useful functions
####

# makes a string usable for searching (special characters are blanks, accents and stuff replaced with their real part)
def simplestr(input,ignorecapitalization=True):
	norm = unicodedata.normalize("NFKD",input)
	norm = [c for c in norm if not unicodedata.combining(c)]
	norm = [c if len(c.encode())==1 else " " for c in norm]
	clear = ''.join(c for c in norm)
	if ignorecapitalization: clear = clear.lower()
	return clear



#def getArtistId(nameorid):
#	if isinstance(nameorid,int):
#		return nameorid
#	else:
#		try:
#			return ARTISTS.index(nameorid)
#		except:
#			return -1


def insert(list_,item,key=lambda x:x):
	i = 0
	while len(list_) > i:
		if key(list_[i]) > key(item):
			list_.insert(i,item)
			return i
		i += 1

	list_.append(item)
	return i


def scrobbles_in_range(start,end,reverse=False):
	if reverse:
		for stamp in reversed(STAMPS):
			#print("Checking " + str(stamp))
			if stamp < start: return
			if stamp > end: continue
			yield SCROBBLESDICT[stamp]
	else:
		for stamp in STAMPS:
			#print("Checking " + str(stamp))
			if stamp < start: continue
			if stamp > end: return
			yield SCROBBLESDICT[stamp]
