import os
import csv
import json
import requests
import getpass
import sys
import re
from string import Template

version = 20241111

'''
pip install requests

You can also set your defaults in:
~/.informatica_cdgc/credentials.json
Example:
{
    "default_pod": "dm-us",
    "default_user": "shayes_infa",
    "default_pwd": "1234"        
}

'''


default_pod="dm-us"        
default_user="shayes_infa"
default_pwd="1234"


prompt_for_login_info = True
pause_before_loading = True
show_raw_errors = False

default_config_file = './config.csv'
extracts_folder = './data'

searches = [
    {
        'search_name': 'All Resources',
        'save_filename': 'resources.json',
        'elastic_search': {
                "from": 0,
                "size": 1000,
                "query": {
                    "term": {
                    "core.classType": "core.Resource"
                    }
                },
                "sort": [
                    {
                    "com.infa.ccgf.models.governance.scannedTime": {
                        "order": "desc"
                    }
                    }
                ]
        }
    },
    {
        'search_name': 'Datasets and Elements in a Resource',
        'save_filename': 'assets.json',
        'elastic_search': {
            "size": 1000,  
            "query": {
                "bool": {
                "must": [
                    {
                    "term": {
                        "core.origin": "${core_origin}"
                    }
                    }
                ],
                "filter": [
                    {
                    "terms": {
                        "type": ["core.DataElement", "core.DataSet"]
                    }
                    }
                ]
                }
            }
        }
    },
    {
        'search_name': 'Glossary Relationships',
        'save_filename': 'glossary_relationships.json',
        'elastic_search': {
            "from": 0,
            "size": 10000,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"elementType": "RELATIONSHIP" }},
                        {"term": {"type":"com.infa.ccgf.models.governance.IClassTechnicalGlossaryBase" }}
                        
                    ]
                }
            },
            "sort": [
                {
                    "com.infa.ccgf.models.governance.scannedTime": {
                        "order": "desc"
                    }
                }
            ]
        }  
    },
   {
        'search_name': 'Business Terms',
        'save_filename': 'glossary_terms.json',
        'elastic_search': {
            "from": 0,
            "size": 1000,
            "query": {
                "term": {
                "core.classType": "com.infa.ccgf.models.governance.BusinessTerm"
                }
            },
            "sort": [
                {
                "com.infa.ccgf.models.governance.scannedTime": {
                    "order": "desc"
                }
                }
            ]
        } 
    },
    {
        'search_name': 'Business Datasets',
        'save_filename': 'business_datasets.json',
        'elastic_search': {
            "from": 0,
            "size": 1000,
            "query": {
                "term": {
                "core.classType": "com.infa.ccgf.models.governance.DataSet"
                }
            },
            "sort": [
                {
                "com.infa.ccgf.models.governance.scannedTime": {
                    "order": "desc"
                }
                }
            ]
        } 
    }          
]

publish_data = [
   {
        'action': 'Associate Dataset to Element',
        'payload': {
            "items":[
                {
                    "elementType":"RELATIONSHIP",
                    "fromIdentity":"${core_identify_of_dataset}",
                    "toIdentity":"${core_identify_of_element}",
                    "operation":"INSERT",
                    "type":"com.infa.ccgf.models.governance.asscDataSetDataElement",
                    "identityType":"INTERNAL",
                    "attributes":{}
                }
            ]
            }
    },
   {
        'action': 'Delete Association Dataset to Element',
        'payload': {
            "items":[
                {
                    "elementType":"RELATIONSHIP",
                    "fromIdentity":"${core_identify_of_dataset}",
                    "toIdentity":"${core_identify_of_element}",
                    "operation":"DELETE",
                    "type":"com.infa.ccgf.models.governance.asscDataSetDataElement",
                    "identityType":"INTERNAL",
                    "attributes":{}
                }
            ]
            }
    },
    {
        'action': 'Dataset Lineage',
        'payload': {
            "items":[
                {
                    "elementType":"RELATIONSHIP",
                    "fromIdentity":"${core_identify_of_source_dataset}",
                    "toIdentity":"${core_identify_of_target_dataset}",
                    "operation":"INSERT",
                    "type":"com.infa.ccgf.models.governance.asscBusinessDatasetDataFlow",
                    "identityType":"INTERNAL",
                    "attributes":{}
                }
            ]
            }
    },
    {
        'action': 'Delete Dataset Lineage',
        'payload': {
            "items":[
                {
                    "elementType":"RELATIONSHIP",
                    "fromIdentity":"${core_identify_of_source_dataset}",
                    "toIdentity":"${core_identify_of_target_dataset}",
                    "operation":"DELETE",
                    "type":"com.infa.ccgf.models.governance.asscBusinessDatasetDataFlow",
                    "identityType":"INTERNAL",
                    "attributes":{}
                }
            ]
            }
    }                    

]

def load_credentials_from_home():
    global default_user, default_pwd, default_pod
    
    # Define the path to the credentials file in the user's home directory
    credentials_path = os.path.join(os.path.expanduser("~"), ".informatica_cdgc", "credentials.json")
    
    # Check if the file exists
    if os.path.exists(credentials_path):
        with open(credentials_path, 'r') as file:
            try:
                # Load the JSON data
                credentials = json.load(file)
                
                # Set each credential individually if it exists in the file
                if 'default_user' in credentials:
                    default_user = credentials['default_user']
                if 'default_pwd' in credentials:
                    default_pwd = credentials['default_pwd']
                if 'default_pod' in credentials:
                    default_pod = credentials['default_pod']
                
            except json.JSONDecodeError:
                pass

def process_json_error(text):
    result_text = text
    if not show_raw_errors:
        try:
            resultJson = json.loads(text)
            result_text = resultJson['message']
        except Exception as e:
            pass
    return result_text

def getCredentials():
    global pod
    global iics_user
    global iics_pwd
    global iics_url
    global cdgc_url

    if any(var not in globals() for var in ['pod', 'iics_user', 'iics_pwd', 'iics_url', 'cdgc_url']):
        if prompt_for_login_info == True:
            pod = input(f"Enter pod (default: {default_pod}): ") or default_pod
            iics_user = input(f"Enter username (default : {default_user}): ") or default_user
            iics_pwd=getpass.getpass("Enter password: ") or default_pwd   
        else:
            if len(default_pod) > 1:
                pod = default_pod
            else:
                pod = input(f"Enter pod (default: {default_pod}): ") or default_pod
            if len(default_user) > 1:
                iics_user = default_user
            else:
                iics_user = input(f"Enter username (default : {default_user}): ") or default_user
            if len(default_pwd) > 1:
                iics_pwd = default_pwd
            else:
                iics_pwd=getpass.getpass("Enter password: ") or default_pwd   
        iics_url = "https://"+pod+".informaticacloud.com"
        cdgc_url = "https://cdgc-api."+pod+".informaticacloud.com"

def login():
    global sessionID
    global orgID
    global headers
    global headers_bearer
    global jwt_token
    global api_url   
    # retrieve the sessionID & orgID & headers
    ## Test to see if I'm already logged in
    if 'jwt_token' not in globals() or len(headers_bearer) < 2:
        loginURL = iics_url+"/saas/public/core/v3/login"
        loginData = {'username': iics_user, 'password': iics_pwd}
        response = requests.post(loginURL, headers={'content-type':'application/json'}, data=json.dumps(loginData))
        try:        
            data = json.loads(response.text)   
            sessionID = data['userInfo']['sessionId']
            orgID = data['userInfo']['orgId']
            api_url = data['products'][0]['baseApiUrl']
            headers = {'Accept':'application/json', 'INFA-SESSION-ID':sessionID,'IDS-SESSION-ID':sessionID, 'icSessionId':sessionID}
        except:
            print("ERROR: logging in: ",loginURL," : ",response.text)
            quit()

        # retrieve the Bearer token
        URL = iics_url+"/identity-service/api/v1/jwt/Token?client_id=cdlg_app&nonce=g3t69BWB49BHHNn&access_code="  
        response = requests.post(URL, headers=headers, data=json.dumps(loginData))
        try:        
            data = json.loads(response.text)
            jwt_token = data['jwt_token']
            headers_bearer = {'content-type':'application/json', 'Accept':'application/json', 'INFA-SESSION-ID':sessionID,'IDS-SESSION-ID':sessionID, 'icSessionId':sessionID, 'Authorization':'Bearer '+jwt_token}        
        except:
            print("ERROR: Getting Token in: ",URL," : ",response.text)
            quit()

def create_query_with_token(json_template, **tokens):
    template = Template(json_template)
    # Substitute placeholders with actual values
    return json.loads(template.safe_substitute(tokens))


def process_search(search_name, **tokens):
    getCredentials()
    login()        

    for i in searches:
        if i['search_name'] == search_name:
            this_search_name = i['search_name']
            this_save_filename = i['save_filename']
            this_elastic_search_raw = i['elastic_search']
            query_json_template = json.dumps(this_elastic_search_raw)
            template = Template(query_json_template)
            this_elastic_search = json.loads(template.safe_substitute(tokens))

            this_full_filename_path = os.path.join(extracts_folder, this_save_filename)
            getCredentials()
            login()
            this_header = headers_bearer
            this_header['X-INFA-SEARCH-LANGUAGE'] = 'elasticsearch'
            
            Result = requests.post(cdgc_url+"/ccgf-searchv2/api/v1/search", headers=this_header, data=json.dumps(this_elastic_search))
            detailResultJson = json.loads(Result.text)

            os.makedirs(extracts_folder, exist_ok=True)
            if os.path.exists(this_full_filename_path) and os.path.getsize(this_full_filename_path) > 0:
                # Read existing data
                with open(this_full_filename_path, 'r') as file:
                    try:
                        data = json.load(file)  # Load existing JSON array
                    except json.JSONDecodeError:
                        data = []  # Start a new array if the file is not valid JSON
            else:
                data = []  # Start a new array if the file doesn't exist or is empty

            # Append new data to the list
            data.append(detailResultJson)

            # Write the updated array back to the file
            with open(this_full_filename_path, 'w') as file:
                json.dump(data, file, indent=4)

def process_publish(action, **tokens):
    def find_message_codes(data_str):
        # Attempt to parse the input string as JSON
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            # If parsing fails, return the original string
            return data_str
        
        # If JSON parsing is successful, proceed to find "messageCode"
        def recursive_find_message_codes(data):
            message_codes = []
            
            # If data is a dictionary, check for "messageCode" and recurse into values
            if isinstance(data, dict):
                for key, value in data.items():
                    if key == "messageCode" and value:
                        message_codes.append(value)
                    elif isinstance(value, (dict, list)):
                        message_codes.extend(recursive_find_message_codes(value))
            
            # If data is a list, iterate and recurse
            elif isinstance(data, list):
                for item in data:
                    message_codes.extend(recursive_find_message_codes(item))
            return message_codes

        # Get all message codes and join them with a space
        all_message_codes = recursive_find_message_codes(data)
        concatenated_message_codes = " ".join(all_message_codes)
        if len(all_message_codes) == 0:
            return data_str
        else:
            return concatenated_message_codes


    getCredentials()
    login()        

    for i in publish_data:
        if i['action'] == action:
            this_action = i['action']
            this_payload_raw = i['payload']
            query_json_template = json.dumps(this_payload_raw)
            template = Template(query_json_template)
            this_payload = json.loads(template.safe_substitute(tokens))

            getCredentials()
            login()
            this_header = headers_bearer
            this_header['X-INFA-SEARCH-LANGUAGE'] = 'elasticsearch'
            this_header['X-INFA-PRODUCT-ID'] = 'CDGC'


            
            Result = requests.post(cdgc_url+"/ccgf-contentv2/api/v1/publish", headers=this_header, data=json.dumps(this_payload))
            if Result.status_code != 200 and Result.status_code != 207:
                result_text = process_json_error(Result.text)
                return False, result_text
            else:
                ## Return success, but what's the actual message?
                message_code = find_message_codes(Result.text)
                if "SUCCESS" in message_code:
                    return True, message_code
                else:
                    if show_raw_errors:
                        return False, Result.text
                    else:
                        return False, message_code
    return False, "Action not found"


               


def search_data(filename, criteria):
    this_full_filename_path = os.path.join(extracts_folder, filename)
    """
    Searches through the JSON file and returns documents that match the given criteria,
    with support for exact matches, regex-based matches, and array membership checks.

    Args:
        filename (str): Path to the JSON file.
        criteria (dict): Dictionary of criteria where each key is the attribute to match.
                         Each value can be a string (for exact matches), a compiled regex pattern,
                         or a dictionary with an 'in' key for array membership checks.

    Returns:
        list: A list of matching documents.
    """
    with open(this_full_filename_path, 'r') as file:
        data = json.load(file)
    
    results = []
    
    # Iterate over each item in the JSON file (not just the first element)
    for item in data:
        for document in item["hits"]["hits"]:
            source = document["sourceAsMap"]
            
            # Check if document matches all criteria
            match = True
            for key, value in criteria.items():
                field_value = source.get(key)
                
                # Check if the criterion is a regex pattern
                if isinstance(value, re.Pattern):
                    if field_value is None or not value.match(field_value):
                        match = False
                        break
                
                # Check for an exact match
                elif isinstance(value, dict) and 'in' in value:
                    # Array membership check: value['in'] should be in the array field_value
                    if field_value is None or not isinstance(field_value, list) or value['in'] not in field_value:
                        match = False
                        break
                
                elif field_value != value:
                    match = False
                    break
            
            # If all criteria matched, add the document to results
            if match:
                results.append(source)
    
    return results

def cleanup_data():
    os.makedirs(extracts_folder, exist_ok=True)
    for filename in os.listdir(extracts_folder):
        file_path = os.path.join(extracts_folder, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)  # Delete the file
        except Exception as e:
            print(f"ERROR: Error deleting file {file_path}: {e}")




def read_config_and_begin(to_delete=False):
    print(f"INFO: Cleaning up data")
    cleanup_data()
    print(f"INFO: Loading preliminary data")
    process_search('All Resources') 
    process_search('Glossary Relationships') 
    process_search('Business Terms') 
    process_search('Business Datasets') 
    publish_items = []
    loaded_resources = []   
    with open(default_config_file, mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            business_dataset_name = row.get('Business Dataset Name')
            resource_path = row.get('Resource')
            technical_dataset_regex = row.get('Technical Dataset')
            technical_element_regex = row.get('Technical Element')
            require_business_name = row.get('Require Business Name')
            require_glossary_association = row.get('Require Associated Glossary')
            lineage_source_dataset = row.get('Source Data Set Lineage')
            lineage_target_dataset = row.get('Target Data Set Lineage')
            insert_or_delete = row.get('Action', 'INSERT')
            if business_dataset_name and len(business_dataset_name) > 2:
                resource_name = resource_path.split("/")[0]
                business_dataset_id = "NEW"
                business_dataset_id_search = search_data('business_datasets.json', {"core.name": business_dataset_name})
                if len(business_dataset_id_search) > 0:
                    business_dataset_id = business_dataset_id_search[0]['core.identity']
                result = search_data('resources.json', {"core.resourceName": resource_name})
                for r in result:
                    if resource_name not in loaded_resources:
                        print(f"INFO: Loading data for \"{resource_name}\"")
                        process_search('Datasets and Elements in a Resource', core_origin=r['core.origin'])
                        loaded_resources.append(resource_name)
                    updated_path = re.sub(f'^{re.escape(resource_name)}', r['core.origin'] + '://' + r['core.origin'], resource_path)   
                    technical_datasets = search_data('assets.json', {
                        "core.name": re.compile("^"+technical_dataset_regex+"$"), 
                        "core.location": re.compile(r"^"+updated_path+".*"), 
                        "type": {"in": "core.DataSet"}
                    })
                    for d in technical_datasets:
                        dataset_path = d['core.location'] +"/"
                        pretty_dataset_path = d['core.location'].replace(d['core.origin']+"://"+d['core.origin'], resource_name)
                        print(f"INFO: Looking at appropriate elements in: {pretty_dataset_path}")
                        technical_elements = search_data('assets.json', {
                            "core.name": re.compile("^"+technical_element_regex+"$"), 
                            "core.location": re.compile(r"^"+dataset_path+".*"), 
                            ## "core.location": re.compile(r".*"+d['core.name']+"/"+technical_element_regex+"$"), 
                            "type": {"in": "core.DataElement"}
                        })
                        for e in technical_elements:
                            business_term_names = []
                            glossary_relationships = search_data('glossary_relationships.json', {
                                "core.sourceIdentity": e['core.identity'] 
                            })
                            
                            for r in glossary_relationships:
                                business_terms =  search_data('glossary_terms.json', {
                                    "core.identity": r['core.targetIdentity'] 
                                }) 
                                
                                for bt in business_terms:
                                    business_term_names.append(bt['core.name'])
                            business_name = e.get('core.inferredBusinessName', '')
                            if ( 
                                (
                                ( not require_business_name.lower().startswith(("y", "t", "1")) ) or
                                ( require_business_name.lower().startswith(("y", "t", "1")) and len(business_name) > 1 ) 
                                ) and (
                                ( not require_glossary_association.lower().startswith(("y", "t", "1")) ) or
                                ( require_glossary_association.lower().startswith(("y", "t", "1")) and len(business_term_names) > 0 ) 
                                )
                            ):
                                if insert_or_delete == 'DELETE':
                                    print(f"INFO:  Queuing DELETE Element from \"{business_dataset_name}\": {e['core.name']} | Business Name: {business_name} | Terms: {business_term_names}")
                                    item = {
                                        'message': f"Deleting Element in \"{business_dataset_name}\": {e['core.name']}",
                                        'action': 'Delete Association Dataset to Element',
                                        'core_identify_of_dataset': business_dataset_id,
                                        'core_identify_of_element': e['core.identity']
                                    }
                                    publish_items.append(item)
                                else:
                                    print(f"INFO:  Queuing Insert Element in \"{business_dataset_name}\": {e['core.name']} | Business Name: {business_name} | Terms: {business_term_names}")
                                    item = {
                                        'message': f"Publishing Element in \"{business_dataset_name}\": {e['core.name']}",
                                        'action': 'Associate Dataset to Element',
                                        'core_identify_of_dataset': business_dataset_id,
                                        'core_identify_of_element': e['core.identity']
                                    }
                                    publish_items.append(item)

            if (
                lineage_source_dataset 
                and lineage_target_dataset 
                and len(lineage_source_dataset) > 1
                and len(lineage_target_dataset) > 1
            ):
                source_dataset_id_search = search_data('business_datasets.json', {"core.name": lineage_source_dataset})
                if len(source_dataset_id_search) > 0:
                    source_dataset_id = source_dataset_id_search[0]['core.identity']
                target_dataset_id_search = search_data('business_datasets.json', {"core.name": lineage_target_dataset})
                if len(target_dataset_id_search) > 0:
                    target_dataset_id = target_dataset_id_search[0]['core.identity'] 
                
                if insert_or_delete == 'DELETE':
                    print(f"INFO: Queuing DELETE Lineage from \"{lineage_source_dataset}\" to \"{lineage_target_dataset}\"")
                    item = {
                        'message': f"Deleting Lineage from \"{lineage_source_dataset}\" to \"{lineage_target_dataset}\"",
                        'action': 'Delete Dataset Lineage',
                        'core_identify_of_source_dataset': source_dataset_id,
                        'core_identify_of_target_dataset': target_dataset_id
                    }
                else:
                    print(f"INFO: Queuing Lineage from \"{lineage_source_dataset}\" to \"{lineage_target_dataset}\"")
                    item = {
                        'message': f"Publishing Lineage from \"{lineage_source_dataset}\" to \"{lineage_target_dataset}\"",
                        'action': 'Dataset Lineage',
                        'core_identify_of_source_dataset': source_dataset_id,
                        'core_identify_of_target_dataset': target_dataset_id
                    }

                publish_items.append(item)                                    

    if pause_before_loading:
        input(f"Press Any Key to load ...")
    for i in publish_items:
        print(f"INFO: {i['message']}")
        if i['action'] in ['Dataset Lineage', 'Delete Dataset Lineage']:
            publish_result, message = process_publish(i['action'], core_identify_of_source_dataset=i['core_identify_of_source_dataset'], core_identify_of_target_dataset=i['core_identify_of_target_dataset'])
        else:
            publish_result, message = process_publish(i['action'], core_identify_of_dataset=i['core_identify_of_dataset'], core_identify_of_element=i['core_identify_of_element'])
        if publish_result:
            print(f"INFO: Publish successful")
        else:
            print(f"ERROR: Failed to publish: {message}")        

    

                

            

if len(sys.argv) > 1:
    default_config_file = sys.argv[1]

load_credentials_from_home()
read_config_and_begin()















