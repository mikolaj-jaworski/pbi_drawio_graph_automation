import json
import requests
headers = {"Content-Type": "application/json"}
url = r'http://localhost:7071/api/orchestrators/Orchestrator' 
mydict = {"workspaces":['ws_name1', 'ws_name2']}
json_string = json.dumps(mydict)
r = requests.post(url, data=json_string, headers = headers)
print(r.text)