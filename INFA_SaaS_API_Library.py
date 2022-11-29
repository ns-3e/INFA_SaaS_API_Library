# Nick Smith 2022, all rights reserved
# Version 1.0

# Python Library for INFA SaaS API


class EnvironmentConnection():
  def __init__(self, username, password, pod, login_url_prefix):
    self.username = username
    self.password = password
    self.pod = pod
    self.login_url_prefix = login_url_prefix
    self.headers  = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    self.body = {'username': f'{username}', 'password': f'{password}'}
    self.ref_data_sets = [] #list of all reference data set objects

    #Initiate the connection
    print("Initiating connection to environment- Authenticating...")
    self.auththenticate()
    self.get_all_ref_ids()
    for ref_set in self.ref_data_sets:
      self.get_refDataSets_json(ref_set)
      # if DEBUG_MODE:print("INITIALIZATION funct RefDataSet.get_refDataSet_json()- ref_dataset_json_response: ", ref_set.data_set_keys_values)


  def auththenticate(self):
    if DEBUG_MODE: print("Authenticating...")
    auth_request = requests.post(f'https://{self.login_url_prefix}-us.informaticacloud.com/identity-service/api/v1/Login', headers= self.headers, json= self.body)
    if DEBUG_MODE: print("Auth request response: ", auth_request)
    auth_response = json.loads(auth_request.text)
    if DEBUG_MODE: print("Auth response: ", auth_response)
    self.headers['IDS-SESSION-ID'] = auth_response['sessionId']
  
  def get_all_ref_ids(self):    
    # Retun all reference 360 IDs
    # Post https://use4-mdm.dm-us.informaticacloud.com/rdm-service/external/v1/model/export HTTP/1.1
    del self.headers['Content-Type']
    ref_ids_request = requests.get(f'https://{self.pod}-mdm.dm-us.informaticacloud.com/rdm-service/external/v1/rds', headers=self.headers)
    ref_ids_response = json.loads(ref_ids_request.text)
    if DEBUG_MODE:
      print("API ref_ids_response: ", ref_ids_response)
    for ref_data_set in ref_ids_response:
      data_set_object = RefDataSet(ref_data_set)
      self.ref_data_sets.append(data_set_object)
    return self.ref_data_sets

  def get_data_set_names(self):
    data_set_names = []
    for ref_data_set in self.ref_data_sets:
      data_set_names.append(ref_data_set.data_set_name)
    return data_set_names

  def get_refDataSets_json(self, ref_data_set_object):
    self.headers['Content-Type'] = 'application/json'
    ref_dataset_json_body = {
      "dateFormat" : "ISO",
      "containerType" : "codelist",
      "containerId" : f"{ref_data_set_object.data_set_default_list}"
    }
    ref_dataset_json_request = requests.post(f'https://{self.pod}-mdm.dm-us.informaticacloud.com/rdm-service/external/v1/export', headers=self.headers, json=ref_dataset_json_body)
    ref_dataset_json_response = json.loads(ref_dataset_json_request.text)
    # if DEBUG_MODE: print("RefDataSet.get_refDataSet_json()- API RefDataSet JSON Response: ", ref_dataset_json_response)
    ref_data_set_object.set_keys_values(ref_dataset_json_response['content'])
    # print("RefDataSet.get_refDataSet_json()- ref_dataset_json_response: ", ref_dataset_json_response)

  def get_job_status(self, jobid):
    if jobid == None:
      return None
    job_status_request = requests.get(f'https://{self.pod}-mdm.dm-us.informaticacloud.com/rdm-service/external/v1/jobs/{jobid}', headers=self.headers)
    job_status_response = json.loads(job_status_request.text)
    if DEBUG_MODE: print("API job_status_response: ", job_status_response)
    if 'state' in job_status_response:
      return job_status_response['state']
    else:
      if DEBUG_MODE: print("----Job couldn't be found----")
      return None
  

  def update_ref_data_set(self, source_ref_data_set_list, target_container_id): #ref_data_set_list is a list of ref_data_set objects
    # TODO Update if exists / Create if not
    # find API endpoint for updates
    update_headers = self.headers
    update_headers['Content-Type'] = 'application/json'
    del update_headers['Content-Type']
    update_url = f'https://{self.pod}-mdm.dm-us.informaticacloud.com/rdm-service/external/v1/import'
    files = {'file': open(f"{directory}/CodeLists/{source_ref_data_set_list.data_set_name}.csv", 'r')}
    if DEBUG_MODE: print("files: ", files)
    container_id = ' "containerId"'+":"+f'"{target_container_id}"'
    body = {"importSettings": '{"delimiter":"COMMA", "textQualifier":"DOUBLE_QUOTE", "startingRow": null, "codepage":"UTF8", "dateFormat":"ISO", "containerType":"CODELIST",'+ container_id +', "repeatHeaders":true, "mappings":{ "name":"Name", "code":"Code"}}'}
    if DEBUG_MODE: print("Update Request Body: ", body)
    update_request = requests.post(update_url, headers=update_headers, files=files, data=body)
    update_response = json.loads(update_request.text)
    if DEBUG_MODE: print("RefDataSet.update_ref_data_set()- API Update Response: ", update_response)
    if "errorCode" in update_response:
      print(f"Error updating {source_ref_data_set_list.data_set_name} - {update_response['errorCode']}")
    else:
      print("Reference Data Set Updated: ", source_ref_data_set_list.data_set_name)
    return source_ref_data_set_list.data_set_name, update_response['jobId']
