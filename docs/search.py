from smartcitizen_connector import search_by_query

o = search_by_query(endpoint = 'devices', key="owner_id", search_matcher="eq", value="8668")
print (o)

u = search_by_query(endpoint = 'users', key="username", search_matcher="eq", value="Joyce")
print (u)

p = search_by_query(key="postprocessing_id", value="not_null")
print (p)

n = search_by_query(endpoint = 'devices', key="name", search_matcher="cont", value="air")
print (n)

m = search_by_query(endpoint = 'users', key="username", search_matcher="cont", value="osc")
print (m)

t = search_by_query(endpoint='devices', key='tags_name', search_matcher='in', value='TwinAIR')
print (t)
