import unicodedata


remove_symbols = ["'","`","’"]
replace_with_space = [" - ",": "]
def normalize(name):
	for r in replace_with_space:
		name = name.replace(r," ")
	name = "".join(char for char in unicodedata.normalize('NFD',name.lower())
		if char not in remove_symbols and unicodedata.category(char) != 'Mn')
	return name
