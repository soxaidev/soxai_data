import httpx
import pandas as pd

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

    def getDailyData(self, start_date=None, end_date=None, convert_to_local_time=True):
        """
        Retrieves daily data from the SOXAI database within the specified date range.

        Args:
            start_date (str, optional): The start date of the data range. Defaults to '-7d'.
            end_date (str, optional): The end date of the data range. Defaults to 'now()'.

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
        print('query posted to url' + query)
        try:
            response = httpx.post(url, headers=self.headers, data=query)
            data = response.json()
            df =  pd.DataFrame(data)
            if convert_to_local_time:
                df = self.post_process_data(df)
            return df
          
        except Exception as e:
            print("Error in querying the data", e)
            return None
        
    def getDetailData(self, start_date=None, end_date=None, convert_to_local_time=True):
        """
        Retrieves daily detail data from the SOXAI database.

        Args:
            start_date (str or None): The start date of the data range in the format 'YYYY-MM-DD'. If None, the default is '-1d' (one day ago).
            end_date (str or None): The end date of the data range in the format 'YYYY-MM-DD'. If None, the default is 'now()' (current date and time).

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
        print('query posted to url' + query)
        try:
            response = httpx.post(url, headers=self.headers, data=query)
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
    

    def getDailyDataByUid(self, uid, start_time, stop_time):
		"""
        Retrieves daily data from the SOXAI database within the specified date range by uid.

        Args:
			uid (str): The uid to specify in the condition
            start_date (str, optional): The start date of the data range. Defaults to '-7d'.
            end_date (str, optional): The end date of the data range. Defaults to 'now()'.

        Returns:
            pandas.DataFrame: A DataFrame containing the retrieved data.

        Raises:
            Exception: If there is an error in querying the data.

        """
        result_list = []
        if not uid:
            return None
        url = self.url + f'DailyInfoData/{uid}'
        if start_time is None:
            start_time = '-30d'
        else:
            start_time = int(pd.Timestamp(start_time).timestamp())
        if stop_time is None:
            stop_time = 'now()'
        else:
            stop_time = int(pd.Timestamp(stop_time).timestamp())

        query = f"?page=0&start_time={start_time}&stop_time={stop_time}&format=json"

        try:
            timeout = httpx.Timeout(70.0)
            response = httpx.get(url + query, headers=self.headers, timeout=timeout)
            data = json.loads(response.json())
            df =  pd.DataFrame(data)
            return df
        except Exception as e:
            print("Error in querying the data", e)
            return None

