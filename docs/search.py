from smartcitizen_connector import search_by_query


o = search_by_query(endpoint = 'devices', search_items=[{'key': "owner_id", 'search_matcher':"eq", 'value': "8668"}])
print (o)

u = search_by_query(endpoint = 'users', search_items=[{'key': "username", 'search_matcher':"eq", 'value': "Joyce"}])
print (u)

p = search_by_query(endpoint = 'devices', search_items=[{'key': "postprocessing_id", 'search_matcher':"eq", 'value': "not_null"}])
print (p)

n = search_by_query(endpoint = 'devices', search_items=[{'key': "name", 'search_matcher':"cont", 'value': "air"}])
print (n)

m = search_by_query(endpoint = 'users', search_items=[{'key': "username", 'search_matcher':"cont", 'value': "osc"}])
print (m)

t = search_by_query(endpoint='devices', search_items=[{'key': "tags_name", 'search_matcher':"in", 'value': "TwinAIR"}])
print (t)
