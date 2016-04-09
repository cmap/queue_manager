
def check_for_apostraphes(items):
	contains_apostraphe = ["'" in x for x in items]
	if True in contains_apostraphe:
		return [(i, items[i]) for (i,x) in enumerate(contains_apostraphe) if x]
	else:
		return False


def build_in_clause(items):
	"""
	example of usage: cusor.execute("select * from pert_plate where pert_plate in (%s)" % in_clause")
	"""

	str_items = [str(x) for x in items]

	items_wik_apos = check_for_apostraphes(str_items)
	if items_wik_apos:
		raise Exception("some provided items contain apostraphes, these will fail in the sql IN clause - (index, item) - items_wik_apos:  {}".format(items_wik_apos))

	in_clause = "'" + "', '".join(str_items) + "'"
	return in_clause

