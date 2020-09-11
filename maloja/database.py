# server
from bottle import request, response, FormsDict

# rest of the project
from .cleanup import CleanerAgent, CollectorAgent
from . import utilities
from .malojatime import register_scrobbletime, time_stamps, ranges
from .malojauri import uri_to_internal, internal_to_uri, compose_querystring

from .thirdparty import proxy_scrobble_all

from .__pkginfo__ import version
from .globalconf import datadir

# doreah toolkit
from doreah.logging import log
from doreah import tsv
from doreah import settings
from doreah.caching import Cache, DeepCache
from doreah.auth import authenticated_api, authenticated_api_with_alternate
try:
	from doreah.persistence import DiskDict
except: pass
import doreah



# technical
import os
import datetime
import sys
import unicodedata
from collections import namedtuple
from threading import Lock
import yaml
import lru

# url handling
from importlib.machinery import SourceFileLoader
import urllib


# for performance testing
def generateStuff(num=0,pertrack=0,mult=0):
	import random
	for i in range(num):
		track = random.choice(TRACKS)
		t = get_track_dict(track)
		time = random.randint(STAMPS[0],STAMPS[-1])
		createScrobble(t["artists"],t["title"],time,volatile=True)

	for track in TRACKS:
		t = get_track_dict(track)
		for i in range(pertrack):
			time = random.randint(STAMPS[0],STAMPS[-1])
			createScrobble(t["artists"],t["title"],time,volatile=True)

	for scrobble in SCROBBLES:
		s = get_scrobble_dict(scrobble)
		for i in range(mult):
			createScrobble(s["artists"],s["title"],s["time"] - i*500,volatile=True)
