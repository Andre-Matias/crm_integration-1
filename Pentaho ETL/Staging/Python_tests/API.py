import httplib2 as http
import json

try:
	from urlparse import urlparse
except ImportError:
	from urllib.parse import urlparse

headers = {
	'Accept': 'application/json',
	'Content-Type': 'application/json; charset=UTF-8'
}		

uri = 'httpsomething'
path = 'somepath'

target = urlparse(uri+path)
method = 'GET'
body = ''

h = http.Http()

#Authentication
if auth:
	h.add_credentials(auth.user,auth.password)

response, content = h.request(
	target.geturl(),
	method,
	body,
	headers)



#Parse json response

data = json.loads(content)




