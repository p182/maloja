# This file is to store information in additional data structures
# for performance purposes, in other words things that violate SSOT. It
# should not be considered authorative

import unicodedata



### OPTIMIZATION
SCROBBLESDICT = {}	# timestamps to scrobble mapping
STAMPS = []		# sorted


TRACKS_NORMALIZED = []
ARTISTS_NORMALIZED = []
ARTISTS_NORMALIZED_SET = set()
TRACKS_NORMALIZED_SET = set()

MEDALS = {}	#literally only changes once per year, no need to calculate that on the fly
MEDALS_TRACKS = {}
WEEKLY_TOPTRACKS = {}
WEEKLY_TOPARTISTS = {}





# function to turn the name into a representation that can be easily compared, ignoring minor differences
