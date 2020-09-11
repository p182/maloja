from bottle import request
from doreah import tsv
from doreah.logging import log

from ..globalconf import datadir

clients = {}

# load API keys from disk
def load_api_keys():
	global clients
	tsv.create(datadir("clients/authenticated_machines.tsv"))
	for key,desc in tsv.parse(datadir("clients/authenticated_machines.tsv"),"string","string"):
		clients[key] = desc
	log("Authenticated Machines: " + ", ".join([clients[k] for k in clients]))

# check validity of specific api key
def check_api_key(k):
    return clients.get(k) or False

# get all valid keys
def all_api_keys():
	return [k for k in clients]


# check HTTP request for correct API key
def api_key_correct(request):
	args = request.params
	try:
		args.update(request.json)
	except:
		pass
	if "key" in args:
		apikey = args["key"]
		del args["key"]
	elif "apikey" in args:
		apikey = args["apikey"]
		del args["apikey"]
	else: return False

	return check_api_key(apikey)



load_api_keys()
