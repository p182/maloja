from .database import db_query, db_aggregate


def get_scrobbles(**keys):
	r = db_query(**{k:keys[k] for k in keys if k in ["artist","artists","title","since","to","within","timerange","associated","track","max_"]})
	#if keys.get("max_") is not None:
	#	return r[:int(keys.get("max_"))]
	#else:
	#	return r
	return r


def info():
	totalscrobbles = get_scrobbles_num()
	artists = {}

	return {
		"name":settings.get_settings("NAME"),
		"artists":{
			chartentry["artist"]:round(chartentry["scrobbles"] * 100 / totalscrobbles,3)
			for chartentry in get_charts_artists() if chartentry["scrobbles"]/totalscrobbles >= 0
		},
		"known_servers":list(KNOWN_SERVERS)
	}



def get_scrobbles_num(**keys):
	r = db_query(**{k:keys[k] for k in keys if k in ["artist","track","artists","title","since","to","within","timerange","associated"]})
	return len(r)


#for multiple since values (must be ordered)
# DOESN'T SEEM TO ACTUALLY BE FASTER
# REEVALUATE

#def get_scrobbles_num_multiple(sinces=[],to=None,**keys):
#
#	sinces_stamps = [time_stamps(since,to,None)[0] for since in sinces]
#	#print(sinces)
#	#print(sinces_stamps)
#	minsince = sinces[-1]
#	r = db_query(**{k:keys[k] for k in keys if k in ["artist","track","artists","title","associated","to"]},since=minsince)
#
#	#print(r)
#
#	validtracks = [0 for s in sinces]
#
#	i = 0
#	si = 0
#	while True:
#		if si == len(sinces): break
#		if i == len(r): break
#		if r[i]["time"] >= sinces_stamps[si]:
#			validtracks[si] += 1
#		else:
#			si += 1
#			continue
#		i += 1
#
#
#	return validtracks



def get_tracks(artist=None):

	if artist is not None:
		artistid = ARTISTS.index(artist)
	else:
		artistid = None

	# Option 1
	return [get_track_dict(t) for t in TRACKS if (artistid in t.artists) or (artistid==None)]

	# Option 2 is a bit more elegant but much slower
	#tracklist = [get_track_dict(t) for t in TRACKS]
	#ls = [t for t in tracklist if (artist in t["artists"]) or (artist==None)]


def get_artists():
	return ARTISTS #well






def get_charts_artists(**keys):
	return db_aggregate(by="ARTIST",**{k:keys[k] for k in keys if k in ["since","to","within","timerange"]})







def get_charts_tracks(**keys):
	return db_aggregate(by="TRACK",**{k:keys[k] for k in keys if k in ["since","to","within","timerange","artist"]})









def get_pulse(**keys):

	rngs = ranges(**{k:keys[k] for k in keys if k in ["since","to","within","timerange","step","stepn","trail"]})
	results = []
	for rng in rngs:
		res = len(db_query(timerange=rng,**{k:keys[k] for k in keys if k in ["artists","artist","track","title","associated"]}))
		results.append({"range":rng,"scrobbles":res})

	return results





def get_performance(**keys):

	rngs = ranges(**{k:keys[k] for k in keys if k in ["since","to","within","timerange","step","stepn","trail"]})
	results = []

	for rng in rngs:
		if "track" in keys:
			charts = get_charts_tracks(timerange=rng)
			rank = None
			for c in charts:
				if c["track"] == keys["track"]:
					rank = c["rank"]
					break
		elif "artist" in keys:
			charts = get_charts_artists(timerange=rng)
			rank = None
			for c in charts:
				if c["artist"] == keys["artist"]:
					rank = c["rank"]
					break
		results.append({"range":rng,"rank":rank})

	return results








def get_top_artists(**keys):

	rngs = ranges(**{k:keys[k] for k in keys if k in ["since","to","within","timerange","step","stepn","trail"]})
	results = []

	for rng in rngs:
		try:
			res = db_aggregate(timerange=rng,by="ARTIST")[0]
			results.append({"range":rng,"artist":res["artist"],"counting":res["counting"],"scrobbles":res["scrobbles"]})
		except:
			results.append({"range":rng,"artist":None,"scrobbles":0})

	return results









def get_top_tracks(**keys):

	rngs = ranges(**{k:keys[k] for k in keys if k in ["since","to","within","timerange","step","stepn","trail"]})
	results = []

	for rng in rngs:
		try:
			res = db_aggregate(timerange=rng,by="TRACK")[0]
			results.append({"range":rng,"track":res["track"],"scrobbles":res["scrobbles"]})
		except:
			results.append({"range":rng,"track":None,"scrobbles":0})

	return results











def artistInfo(artist):

	charts = db_aggregate(by="ARTIST")
	scrobbles = len(db_query(artists=[artist]))
	#we cant take the scrobble number from the charts because that includes all countas scrobbles
	try:
		c = [e for e in charts if e["artist"] == artist][0]
		others = [a for a in coa.getAllAssociated(artist) if a in ARTISTS]
		position = c["rank"]
		performance = get_performance(artist=artist,step="week")
		return {
			"artist":artist,
			"scrobbles":scrobbles,
			"position":position,
			"associated":others,
			"medals":{"gold":[],"silver":[],"bronze":[],**MEDALS.get(artist,{})},
			"topweeks":WEEKLY_TOPARTISTS.get(artist,0)
		}
	except:
		# if the artist isnt in the charts, they are not being credited and we
		# need to show information about the credited one
		artist = coa.getCredited(artist)
		c = [e for e in charts if e["artist"] == artist][0]
		position = c["rank"]
		return {"replace":artist,"scrobbles":scrobbles,"position":position}






def trackInfo(track):
	charts = db_aggregate(by="TRACK")
	#scrobbles = len(db_query(artists=artists,title=title))	#chart entry of track always has right scrobble number, no countas rules here
	#c = [e for e in charts if set(e["track"]["artists"]) == set(artists) and e["track"]["title"] == title][0]
	c = [e for e in charts if e["track"] == track][0]
	scrobbles = c["scrobbles"]
	position = c["rank"]
	cert = None
	threshold_gold, threshold_platinum, threshold_diamond = settings.get_settings("SCROBBLES_GOLD","SCROBBLES_PLATINUM","SCROBBLES_DIAMOND")
	if scrobbles >= threshold_diamond: cert = "diamond"
	elif scrobbles >= threshold_platinum: cert = "platinum"
	elif scrobbles >= threshold_gold: cert = "gold"


	return {
		"track":track,
		"scrobbles":scrobbles,
		"position":position,
		"medals":{"gold":[],"silver":[],"bronze":[],**MEDALS_TRACKS.get((frozenset(track["artists"]),track["title"]),{})},
		"certification":cert,
		"topweeks":WEEKLY_TOPTRACKS.get(((frozenset(track["artists"]),track["title"])),0)
	}




def compare(remoteurl):
	import json
	compareurl = remoteurl + "/api/info"

	response = urllib.request.urlopen(compareurl)
	strangerinfo = json.loads(response.read())
	owninfo = info()

	#add_known_server(compareto)

	artists = {}

	for a in owninfo["artists"]:
		artists[a.lower()] = {"name":a,"self":int(owninfo["artists"][a]*1000),"other":0}

	for a in strangerinfo["artists"]:
		artists[a.lower()] = artists.setdefault(a.lower(),{"name":a,"self":0})
		artists[a.lower()]["other"] = int(strangerinfo["artists"][a]*1000)

	for a in artists:
		common = min(artists[a]["self"],artists[a]["other"])
		artists[a]["self"] -= common
		artists[a]["other"] -= common
		artists[a]["common"] = common

	best = sorted((artists[a]["name"] for a in artists),key=lambda x: artists[x.lower()]["common"],reverse=True)

	result = {
		"unique_self":sum(artists[a]["self"] for a in artists if artists[a]["common"] == 0),
		"more_self":sum(artists[a]["self"] for a in artists if artists[a]["common"] != 0),
		"common":sum(artists[a]["common"] for a in artists),
		"more_other":sum(artists[a]["other"] for a in artists if artists[a]["common"] != 0),
		"unique_other":sum(artists[a]["other"] for a in artists if artists[a]["common"] == 0)
	}

	total = sum(result[c] for c in result)

	for r in result:
		result[r] = (result[r],result[r]/total)



	return {
		"result":result,
		"info":{
			"ownname":owninfo["name"],
			"remotename":strangerinfo["name"]
		},
		"commonartist":best[0]
	}


def incoming_scrobble(artists,title,album=None,duration=None,time=None):
	artists = "/".join(artists)
	if time is None:
		time = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())

	log("Incoming scrobble (): ARTISTS: " + str(artists) + ", TRACK: " + title,module="debug")
	(artists,title) = cla.fullclean(artists,title)
	trackdict = createScrobble(artists,title,time,album,duration)

	sync()

	return {"status":"success","track":trackdict}









def issues():
	return ISSUES

def check_issues():
	combined = []
	duplicates = []
	newartists = []

	import itertools
	import difflib

	sortedartists = ARTISTS.copy()
	sortedartists.sort(key=len,reverse=True)
	reversesortedartists = sortedartists.copy()
	reversesortedartists.reverse()
	for a in reversesortedartists:

		nochange = cla.confirmedReal(a)

		st = a
		lis = []
		reachedmyself = False
		for ar in sortedartists:
			if (ar != a) and not reachedmyself:
				continue
			elif not reachedmyself:
				reachedmyself = True
				continue

			if (ar.lower() == a.lower()) or ("the " + ar.lower() == a.lower()) or ("a " + ar.lower() == a.lower()):
				duplicates.append((ar,a))
				break

			if (ar + " " in st) or (" " + ar in st):
				lis.append(ar)
				st = st.replace(ar,"").strip()
			elif (ar == st):
				lis.append(ar)
				st = ""
				if not nochange:
					combined.append((a,lis))
				break

			elif (ar in st) and len(ar)*2 > len(st):
				duplicates.append((a,ar))

		st = st.replace("&","").replace("and","").replace("with","").strip()
		if st != "" and st != a:
			if len(st) < 5 and len(lis) == 1:
				#check if we havent just randomly found the string in another word
				#if (" " + st + " ") in lis[0] or (lis[0].endswith(" " + st)) or (lis[0].startswith(st + " ")):
				duplicates.append((a,lis[0]))
			elif len(st) < 5 and len(lis) > 1 and not nochange:
				combined.append((a,lis))
			elif len(st) >= 5 and not nochange:
				#check if we havent just randomly found the string in another word
				if (" " + st + " ") in a or (a.endswith(" " + st)) or (a.startswith(st + " ")):
					newartists.append((st,a,lis))

	#for c in itertools.combinations(ARTISTS,3):
	#	l = list(c)
	#	print(l)
	#	l.sort(key=len,reverse=True)
	#	[full,a1,a2] = l
	#	if (a1 + " " + a2 in full) or (a2 + " " + a1 in full):
	#		combined.append((full,a1,a2))


	#for c in itertools.combinations(ARTISTS,2):
	#	if
	#
	#	if (c[0].lower == c[1].lower):
	#		duplicates.append((c[0],c[1]))


	#	elif (c[0] + " " in c[1]) or (" " + c[0] in c[1]) or (c[1] + " " in c[0]) or (" " + c[1] in c[0]):
	#		if (c[0] in c[1]):
	#			full, part = c[1],c[0]
	#			rest = c[1].replace(c[0],"").strip()
	#		else:
	#			full, part = c[0],c[1]
	#			rest = c[0].replace(c[1],"").strip()
	#		if rest in ARTISTS and full not in [c[0] for c in combined]:
	#			combined.append((full,part,rest))

	#	elif (c[0] in c[1]) or (c[1] in c[0]):
	#		duplicates.append((c[0],c[1]))


	return {"duplicates":duplicates,"combined":combined,"newartists":newartists}



def get_predefined_rulesets():
	validchars = "-_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	rulesets = []

	for f in os.listdir(datadir("rules/predefined")):
		if f.endswith(".tsv"):

			rawf = f.replace(".tsv","")
			valid = True
			for char in rawf:
				if char not in validchars:
					valid = False
					break # don't even show up invalid filenames

			if not valid: continue
			if not "_" in rawf: continue

			try:
				with open(datadir("rules/predefined",f)) as tsvfile:
					line1 = tsvfile.readline()
					line2 = tsvfile.readline()

					if "# NAME: " in line1:
						name = line1.replace("# NAME: ","")
					else: name = rawf.split("_")[1]
					if "# DESC: " in line2:
						desc = line2.replace("# DESC: ","")
					else: desc = ""

					author = rawf.split("_")[0]
			except:
				continue

			ruleset = {"file":rawf}
			rulesets.append(ruleset)
			if os.path.exists(datadir("rules",f)):
				ruleset["active"] = True
			else:
				ruleset["active"] = False

			ruleset["name"] = name
			ruleset["author"] = author
			ruleset["desc"] = desc

	return rulesets
