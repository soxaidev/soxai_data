import httpx
import pandas as pd
import json

class DataLoader():
    def __init__(self, token):
        """
        Initializes an instance of the class.

        Parameters:
        token (str): The token used for authentication, please go to https://soxai-web-api-tiufu2wgva-df.a.run.app/ and login to generate one token.

        Attributes:
        url (str): The base URL of the API.
        headers (dict): The headers used for the API requests.
        org_id (str): The ID of the organization. Will be none for normal users.

        Usage:

        ##Plot the daily info data
        ```python
        from soxai_data import DataLoader

        sx_data = DataLoader(token=<Your-soxai-api-token>)
        df = sx_data.getDailyData()
        df.plot()
        ```
        """
        self.url = 'https://soxai-firebase.df.r.appspot.com/api/'
        self.headers = {
            'Content-Type': 'text/plain',
            'soxai-api-key': token
        }
        self.org_id = None

    def getMyInfo(self):
        """
        Get the account information.

        Returns:
        dict: my personal information.
        """
        url = self.url + 'myOrg'
        response = httpx.get(url, headers=self.headers)
        data = response.json()
        # return response.json()
        try:
            if 'isOrgUser' in data.keys() and data['isOrgUser']:
                self.org_id = data['myOrg']['orgId']
        except:
            pass
        return data
            
    
    def getMyOrgUsers(self, org_id=None):
        """
        Retrieves the users associated with the specified organization.

        Args:
            org_id (str, optional): The ID of the organization. If not provided, the method will use the default organization ID.

        Returns:
            pd.DataFrame: The DataFrame containing the users associated with the specified organization.

        Raises:
            None

        """
        if self.org_id is None:
            _ = self.getMyInfo()
            if self.org_id is None:
                return None
        org_id = self.org_id if org_id is None else org_id
        url = self.url + 'orgs/' + self.org_id + '/orgUsers'
        response = httpx.get(url, headers=self.headers)
        try:
            data = response.json()
            return pd.DataFrame(data)
        except:
            return None

    def add_uid_filter_to_flux_query(self, flux_query: str, uids: list) -> str:
        uid_filter = ' or '.join([f'r["uid"] == "{uid}"' for uid in uids])
        filter_statement = f'|> filter(fn: (r) => {uid_filter})\n'

        # Find Insertion Position
        range_index = flux_query.find('|> range(')
        if range_index != -1:
            # Insert the filter statement on the line below the range statement
            insert_index = flux_query.index('\n', range_index) + 1
            modified_flux_query = flux_query[:insert_index] + filter_statement + flux_query[insert_index:]
            return modified_flux_query

        # If the range statement is not found, the original query statement is returned directly
        raise Exception('Cannot find range statement in flux query')

    def getDailyData(self, start_date=None, end_date=None, convert_to_local_time=True, uid_list:list = [], timeout=60.0):
        """
        Retrieves daily data from the SOXAI database within the specified date range.

        Args:
            start_date (str, optional): The start date of the data range. Defaults to '-7d'.
            end_date (str, optional): The end date of the data range. Defaults to 'now()'.
            convert_to_local_time (booleanm, optional): The flag to change to local time.
            uid_list (list): The uid to specify in the condition.
            timeout (float, optional): The timeout in seconds. (Up to 120.0)

        Returns:
            pandas.DataFrame: A DataFrame containing the retrieved data.

        Raises:
            Exception: If there is an error in querying the data.

        """
        url = self.url + 'queryData'
        if start_date is None:
            start_date = '-7d'
        else:
            start_date = int(pd.Timestamp(start_date).timestamp())
        if end_date is None:
            end_date = 'now()'
        else:
            end_date = int(pd.Timestamp(end_date).timestamp())

        query = """from(bucket: "SOXAI")
                    |> range(start: {}, stop: {} )
                    |> filter(fn: (r) => r["_measurement"] == "SX_Daily_Prod") 
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                """.format(start_date, end_date)
        
        if len(uid_list) > 0:
            query = self.add_uid_filter_to_flux_query(query, uid_list)

        try:
            response = httpx.post(url, headers=self.headers, data=query, timeout=httpx.Timeout(timeout))
            data = response.json()
            df =  pd.DataFrame(data)
            if convert_to_local_time:
                df = self.post_process_data(df)
            return df
        except Exception as e:
            print("Error in querying the data", e)
            return None
        
    def getDetailData(self, start_date=None, end_date=None, convert_to_local_time=True, uid_list:list = [], timeout=60.0):
        """
        Retrieves daily detail data from the SOXAI database.

        Args:
            start_date (str or None): The start date of the data range in the format 'YYYY-MM-DD'. If None, the default is '-1d' (one day ago).
            end_date (str or None): The end date of the data range in the format 'YYYY-MM-DD'. If None, the default is 'now()' (current date and time).
            convert_to_local_time (booleanm, optional): The flag to change to local time.
            uid_list (list): The uid to specify in the condition.
            timeout (float, optional): The timeout in seconds. (Up to 120.0)

        Returns:
            pandas.DataFrame or None: The retrieved data as a pandas DataFrame, or None if an error occurred during the data retrieval.

        """
        url = self.url + 'queryData'
        if start_date is None:
            start_date = '-1d'
        else:
            start_date = int(pd.Timestamp(start_date).timestamp())
        if end_date is None:
            end_date = 'now()'
        else:
            end_date = int(pd.Timestamp(end_date).timestamp())

        query = """from(bucket: "SOXAI")
                    |> range(start: {}, stop: {} )
                    |> filter(fn: (r) => r["_measurement"] == "SX_Detail_Prod") 
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                """.format(start_date, end_date)

        if len(uid_list) > 0:
            query = self.add_uid_filter_to_flux_query(query, uid_list)

        try:
            response = httpx.post(url, headers=self.headers, data=query, timeout=httpx.Timeout(timeout))
            data = response.json()
            df =  pd.DataFrame(data)
            if convert_to_local_time:
                df = self.post_process_data(df)
            return df
        except Exception as e:
            print("Error in querying the data", e)
            return None
        
    def post_process_data(self,df):
        """
        Post process the data to make it more readable.

        Args:
            df (pd.DataFrame): The DataFrame to be post-processed.

        Returns:
            pd.DataFrame: The post-processed DataFrame.

        """
        df['local_time'] = pd.to_datetime(df['_time']) + pd.to_timedelta(df['utc_offset_mins'], unit='minutes')
        # drop ['_start', '_stop', '_time'] columns
        df.drop(['_start', '_stop', '_time', '_measurement'], axis=1, inplace=True)
        df = df.set_index('local_time')
        return df
    

    def getRawData(self, uid, start_date=None, end_date=None, timeout=5.0):
        """
        Retrieves raw data from the SOXAI database within the specified date range.

        Args:
            uid (str): The uid to specify in the condition
            start_date (str, optional): The start date of the data range. Defaults to '-7d'.
            end_date (str, optional): The end date of the data range. Defaults to 'now()'.
            convert_to_local_time (booleanm, optional): The flag to change to local time
            timeout (float, optional): The timeout in seconds (Up to 60.0)

        Returns:
            pandas.DataFrame: A DataFrame containing the retrieved data.

        Raises:
            Exception: If there is an error in querying the data.

        """
        if start_date is None:
            start_date = '-7d'
        else:
            start_date = int(pd.Timestamp(start_date).timestamp())
        if end_date is None:
            end_date = 'now()'
        else:
            end_date = int(pd.Timestamp(end_date).timestamp())

        url = self.url + f'RawData/{uid}'
        query = f"?page=0&start_time={start_date}&stop_time={end_date}&format=json"

        try:
            response = httpx.get(url + query, headers=self.headers, timeout=httpx.Timeout(timeout))
            data = json.loads(response.json())
            df =  pd.DataFrame(data)
            return df
        except Exception as e:
            print("Error in querying the data", e)
            return None