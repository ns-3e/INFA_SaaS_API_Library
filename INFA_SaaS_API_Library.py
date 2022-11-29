# Nick Smith 2022, all rights reserved
# Version 1.0

# Python Library for INFA SaaS API

import requests
import json

class EnvironmentConnection():
  def __init__(self, username, password, pod="use4", login_url_prefix= "dm"):
    self.username = username
    self.password = password
    self.pod = pod
    self.login_url_prefix = login_url_prefix
    self.headers  = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    self.body = {'username': f'{username}', 'password': f'{password}'}
    self.ref_data_sets = [] #list of all reference data set objects
    
    # Initiate the connection
    print("Initiating connection to environment- Authenticating...")
    self.auththenticate()

  def auththenticate(self):
    auth_request = requests.post(f'https://{self.login_url_prefix}-us.informaticacloud.com/identity-service/api/v1/Login', headers= self.headers, json= self.body)
    auth_response = json.loads(auth_request.text)
    self.headers['IDS-SESSION-ID'] = auth_response['sessionId']
  


  def get_job_status(self, jobid):
    job_status_request = requests.get(f'https://{self.pod}-mdm.dm-us.informaticacloud.com/rdm-service/external/v1/jobs/{jobid}', headers=self.headers)
    job_status_response = json.loads(job_status_request.text)
    if 'state' in job_status_response:
        return job_status_response['state']
    else:
        print("get_job_status()- Error: ", job_status_response['starus'])
        return None
  

