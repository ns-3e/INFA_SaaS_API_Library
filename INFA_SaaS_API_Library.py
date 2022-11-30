# Nick Smith 2022, all rights reserved
# Version 1.0

# Python Library for INFA SaaS API

import requests
import json

DEBUGMODE = True

class EnvironmentConnection():
    def __init__(self, username, password, pod="use4", login_url_prefix= "dm"):
        self.username = username
        self.password = password
        self.pod = pod
        self.login_url_prefix = login_url_prefix
        self.headers  = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        self.body = {'username': f'{username}', 'password': f'{password}'}
        self.ref_data_sets = [] #list of all reference data set objects
        self.base_url = f"https://{self.pod}-mdm.dm-us.informaticacloud.com"
        
        # Initiate the connection
        print("Initiating connection to environment- Authenticating...")
        self.auththenticate()

    def auththenticate(self):
        auth_request = requests.post(f'https://{self.login_url_prefix}-us.informaticacloud.com/identity-service/api/v1/Login', headers= self.headers, json= self.body)
        auth_response = json.loads(auth_request.text)
        self.headers['IDS-SESSION-ID'] = auth_response['sessionId']
    
    def get_xrefs(self, business_id, business_entity_id):
        url = f"{self.base_url}/business-entity/public/api/v1/entity-member/{business_entity_id}/{business_id}"
        get_xref_request = requests.get(url, headers= self.headers)
        get_xref_response = json.loads(get_xref_request.text)
        try:
            if DEBUGMODE: print("XREFS: ", get_xref_response['content'])
            return get_xref_response['content'] # returen list of dictionaries ex: [{'sourcePKey': '2762238433', 'sourceSystem': 'cmp'}]
        except:
            try:
                print("Error: ", get_xref_response['errorSummary'])
            except:
                print("Error: get_xref_response['errorSummary'] not found")
                return get_xref_response

    # Pass list of xref dicts to unmerge all records 
    def group_unmerge(self, xref_list, business_id, business_entity_id):
        url = f"{self.base_url}/business-entity/public/api/v1/entity-group/{business_entity_id}/{business_id}"
        unmerge_request = requests.post(url, headers = self.headers, json = xref_list)
        unmerge_response = json.loads(unmerge_request.text)
        pass

    # Pass business id, source system, and source pkey to unmerge a single xref from master record
    def single_unmerge(self, business_id, business_entity_id, source_system, source_pkey):
        url = f"{self.base_url}/business-entity/public/api/v1/entity-group/{business_entity_id}/{business_id}"
        body = "[{'sourcePKey': '"+source_pkey+"', 'sourceSystem': '"+source_system+"'}]"
        unmerge_request = requests.post(url, headers = self.headers, json = body)
        unmerge_response = json.loads(unmerge_request.text)
        pass
def main():
    pass

if __name__ == "__main__":
    main()
