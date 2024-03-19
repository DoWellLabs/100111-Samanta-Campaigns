import json
from concurrent.futures import ThreadPoolExecutor
import requests
import pickle
import base64
import time

def gsheet_connection(field, command):
    url = "http://uxlivinglab.pythonanywhere.com/"
    payload = {
        "cluster": "undefined",
        "database": "1vbrNoCJju1pdMmR5ShT4tqbc5N25bQpT5MriJwlDsxk",
        "collection" : "sheet_one",
        "document": "sheet_one",
        "team_member_ID": "1241001",
        "function_ID": "ABCDE",
        "command": command,
        "field": field,
        "update_field": {},
        "platform": "bangalore"
    }

    response = requests.post(url, json=payload)
    r = json.loads(response.json())
    return r

# start_time = time.perf_counter()

# gsheet_connection({}, "fetch")

# end_time = time.perf_counter()
# response_time = end_time - start_time
# print(f"Request took {response_time:.2f} seconds to process")


    
"""
    For 'insert' send field as list.

    success will append the row at the end of the sheet and response ex, 
    {"isSuccess": true}

"""
# r = gsheet_connection([2,'item 4', 'item 5', 'item 6'], 'insert')

# print(r)
 

"""
    command: 'find'
    
    field: {'column_index': 'A', 'column_value': 'a_cell_value_in_that_column'}
    
    success will return a list of the matching row values ex, {"isSuccess": true, "data": [ 'index 1', 'value 1', 23 ]}

"""


# find_data = {'column_index': 'B', 'column_value': 'item 10'}

# r = gsheet_connection(find_data, 'find')

# print(r)


"""
    command: "fetch"
    field: {}
    success will return all the rows of the sheet as list of list ex, {"isSuccess": true, "data": [["1", "item 1", "item 2", "item 3"]]}
    
"""

# r = gsheet_connection({}, 'fetch')

# print(r)

class ClientError(Exception):
    """Exception raised for errors in the Client."""


class GSheetClient:
    """Dowell Google Sheet Client"""
    
    def fetch(self, object_class = None) :
        """
        Fetches all objects of the given class from the GSheet.

        :param object_class: The class of the objects to be fetched. If None, then fetches all objects.
        """
        t1 = time.perf_counter()
        response = gsheet_connection({}, 'fetch')
        if not response["isSuccess"]:
            raise ValueError("Could not connect to GSheet API.")
        objs = []
        resp_data = response["data"]
        def parse_obj(row):
            if row and (object_class is None or row[1] == object_class.__name__):
                try:
                    object_data = row[2]
                    object_bytes = base64.urlsafe_b64decode(object_data.encode())
                    obj = pickle.loads(object_bytes)
                    if isinstance(obj, object_class):
                        objs.append(obj)
                except:
                    pass
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(parse_obj, resp_data)
        t2 = time.perf_counter()
        print(f"Time for fetch: {t2-t1}")
        return objs
    
    @property
    def last_index(self):
        """
        Returns the index of the last row of data in the GSheet.
        """
        response = gsheet_connection({}, 'fetch')
        if not response["isSuccess"]:
            raise ClientError("Could not connect to GSheet API.")
        resp_data = response["data"]
        resp_data = [row for row in resp_data if row]
        return len(resp_data)
    
    @property
    def next_index(self):
        """
        Returns the index of the next row of data to be inserted in the GSheet.
        """
        return self.last_index + 1
        

    def insert(self, object_: object):
        """
        Saves the given object to the GSheet.

        :param object_: The object to be saved.
        """
        data_index = self.next_index
        obj_bytes = pickle.dumps(object_)
        data = [object_.__class__.__name__, base64.urlsafe_b64encode(obj_bytes).decode()]
        response = gsheet_connection([data_index, *data], 'insert')
        if not response["isSuccess"]:
            raise ClientError(f"Could not save {object_.__class__.__name__} to GSheet.")
        return None
    

