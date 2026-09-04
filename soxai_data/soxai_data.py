"""
Client for the SOXAI web api.

DataLoader is the entry point: it authenticates with an api token and exposes the
account, organization, daily and raw data endpoints as pandas DataFrames.
"""

import datetime
import json
from typing import Optional, Union

import httpx
import pandas as pd

# a date argument accepted by the data fetching methods. datetime.datetime and
# pandas.Timestamp are subclasses of datetime.date, so they are covered as well
DateLike = Union[str, datetime.date]

# the longest range the v2 endpoints accept. a longer one is answered with 400, so the
# methods below refuse it before spending a request per uid on a known answer
MAX_RANGE_DAYS = 366


def _to_aware_timestamp(value: DateLike, arg_name: str) -> pd.Timestamp:
    """
    Read a date argument of a data fetching method as a timezone aware Timestamp.

    This is the timezone rule shared by every date argument of the data fetching
    methods: a value that carries no timezone information is interpreted as utc,
    and a value that carries timezone information keeps the given offset.

    args:
        - value : a date or datetime, as a string, datetime.date, datetime.datetime
          or pandas.Timestamp
        - arg_name : the name of the argument, used in the error message
    returns:
        - the timezone aware Timestamp of value
    raises:
        - ValueError : if value cannot be read as a date or datetime
    """
    error_message = f"Incorrect {arg_name} value({value}), should be a date or datetime"
    if isinstance(value, (int, float)):
        # a bare number would be read as an offset in nanoseconds from the epoch,
        # which is never what the caller means here
        raise ValueError(error_message)
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as e:
        raise ValueError(error_message) from e
    if pd.isna(timestamp):
        # None and an empty string are parsed as NaT instead of raising
        raise ValueError(error_message)
    if timestamp.tzinfo is None:
        # no timezone information given, so interpret it as utc
        return timestamp.tz_localize('UTC')
    # timezone information given, so follow that offset
    return timestamp


class DataLoader:
    """Read SOXAI data through the web api with an api token."""

    def __init__(self, token):
        """
        Initialize an instance of the class.

        Usage:

        ##Plot the daily info data
        ```python
        from soxai_data import DataLoader

        sx_data = DataLoader(token=<Your-soxai-api-token>)
        df = sx_data.getDailyInfoV2(uid_list=['<uid>'])
        df.plot()
        ```

        args:
            - token : the token used for authentication, please go to
              https://soxai-web-api-tiufu2wgva-df.a.run.app/ and login to generate one token
        """
        self.url = 'https://web-api.soxai.site/api/'
        self.headers = {
            'Content-Type': 'text/plain',
            'soxai-api-key': token
        }
        self.org_id = None

    def getMyInfo(self):
        """
        Get the account information.

        returns:
            - dict of my personal information
        raises:
            - httpx.HTTPStatusError : if the api returns an error status
        """
        url = self.url + 'myOrg'
        response = httpx.get(url, headers=self.headers)
        response.raise_for_status()
        data = response.json()
        try:
            if 'isOrgUser' in data.keys() and data['isOrgUser']:
                self.org_id = data['myOrg']['orgId']
        except Exception:
            pass
        return data

    def getMyOrgUsers(self, org_id=None):
        """
        Retrieve the users associated with the specified organization.

        args:
            - org_id : the id of the organization. If not provided, the id of my own
              organization is looked up with getMyInfo
        returns:
            - DataFrame containing the users of the organization, or None if the
              organization could not be resolved or the response was not readable
        raises:
            - httpx.HTTPStatusError : if the api returns an error status
        """
        if org_id is None:
            if self.org_id is None:
                _ = self.getMyInfo()
            org_id = self.org_id
            if org_id is None:
                return None
        url = self.url + f'orgs/{org_id}/orgUsers'
        response = httpx.get(url, headers=self.headers)
        response.raise_for_status()
        try:
            data = response.json()
            return pd.DataFrame(data)
        except Exception:
            return None

    def _post_process_data(self, df):
        """
        Post process the data to make it more readable.

        The local_time index holds the wall clock time of the user's timezone. It is
        deliberately timezone naive: utc_offset_mins can differ per row, so a single
        timezone cannot describe the whole column, and keeping the UTC label after
        adding the offset would make the values look like UTC timestamps.

        args:
            - df : the DataFrame to be post-processed
        returns:
            - a post-processed copy of df, indexed by the local_time wall clock
        """
        df = df.copy()
        # utc=True applies the same timezone rule as the date arguments: a _time value
        # without timezone information is read as utc, and an offset is followed
        local_time = (
            pd.to_datetime(df['_time'], utc=True)
            + pd.to_timedelta(df['utc_offset_mins'], unit='minutes')
        )
        df['local_time'] = local_time.dt.tz_localize(None)
        # drop the columns that are no longer needed, ignoring the ones the api did not return
        df = df.drop(columns=['_start', '_stop', '_time', '_measurement'], errors='ignore')
        return df.set_index('local_time')

    def getRawData(
        self,
        uid,
        start_date: Optional[DateLike] = None,
        end_date: Optional[DateLike] = None,
        timeout: float = 5.0,
    ):
        """
        Retrieve raw data from the SOXAI database within the specified date range.

        args:
            - uid : the uid to specify in the condition
            - start_date : the start of the data range. Without timezone information it
              is read as utc, and with timezone information (e.g. '2026-01-20T00:00:00+09:00')
              that offset is followed. Defaults to 7 days before the current utc time
            - end_date : the end of the data range, read the same way as start_date.
              Defaults to the current utc time
            - timeout : the timeout in seconds (Up to 60.0)
        returns:
            - DataFrame containing the retrieved data, or None if the request failed
        raises:
            - ValueError : if a date argument cannot be read as a date or datetime
        """
        # this endpoint takes unix seconds, so each argument is converted with the shared
        # timezone rule: no timezone information means utc, a given offset is followed
        if start_date is None:
            # no argument to follow, so the default is based on the current utc time
            start_time = int((pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=7)).timestamp())
        else:
            start_time = int(_to_aware_timestamp(start_date, 'start_date').timestamp())
        if end_date is None:
            end_time = int(pd.Timestamp.now(tz='UTC').timestamp())
        else:
            end_time = int(_to_aware_timestamp(end_date, 'end_date').timestamp())

        url = self.url + f'RawData/{uid}'
        query = f"?page=0&start_time={start_time}&stop_time={end_time}&format=json"

        try:
            response = httpx.get(url + query, headers=self.headers, timeout=httpx.Timeout(timeout))
            response.raise_for_status()
            data = response.json()
            if isinstance(data, str):
                # this endpoint returns the payload as a json encoded string
                data = json.loads(data)
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            print("Error in querying the data", e)
            return None

    def getDailyInfoV2(
        self,
        start_date: Optional[DateLike] = None,
        end_date: Optional[DateLike] = None,
        uid_list: Optional[list] = None,
        *,
        convert_to_local_time: bool = False,
        timeout: float = 60.0,
    ):
        """
        Retrieve daily info data from the SOXAI v2 api within the specified date range.

        args:
            - start_date : the start date of the data range, e.g. '2026-01-20'. Without
              timezone information it is read as utc, and with timezone information
              (e.g. '2026-01-20T00:00:00+09:00') the day is the one seen at that offset.
              Defaults to 7 days before the current utc date
            - end_date : the end date of the data range, read the same way as start_date.
              Defaults to the current utc date
            - uid_list : the uids to fetch data for
            - convert_to_local_time : keyword only, whether to convert the timestamps to
              local time
            - timeout : keyword only, the timeout in seconds (Up to 120.0). It is keyword
              only so that a caller that used to pass it as the fourth positional argument
              gets a TypeError instead of silently setting convert_to_local_time
        returns:
            - DataFrame containing the retrieved data, or None if every uid answered
              without data
        raises:
            - ValueError : if a date argument cannot be read as a date or datetime, if
              start_date is after end_date, or if the range covers more than
              MAX_RANGE_DAYS days
            - TypeError : if convert_to_local_time or timeout is passed positionally
            - httpx.HTTPStatusError : if no uid answered with data and the api answered
              with an error status
        """
        if uid_list is None:
            uid_list = []

        url = self.url + 'v2/DailyInfoData/'

        # this endpoint works in day units, so each argument is reduced to a calendar day
        # with the shared timezone rule: without timezone information the value is read as
        # utc, and with timezone information the day is the one seen at that offset
        if start_date is None:
            # no argument to follow, so the default is based on the current utc date
            start_day = (pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
        else:
            start_day = _to_aware_timestamp(start_date, 'start_date').strftime("%Y-%m-%d")

        if end_date is None:
            end_day = pd.Timestamp.now(tz='UTC').strftime("%Y-%m-%d")
        else:
            end_day = _to_aware_timestamp(end_date, 'end_date').strftime("%Y-%m-%d")

        # date range check on the days that are actually sent
        if start_day > end_day:
            raise ValueError(f"start_date({start_day}) must not be after end_date({end_day})")

        # a longer range is rejected by the api, so it is a caller error rather than an
        # empty result. raising keeps the request that is certain to fail off the api
        span_days = (pd.Timestamp(end_day) - pd.Timestamp(start_day)).days
        if span_days > MAX_RANGE_DAYS:
            raise ValueError(
                f"the range of start_date({start_day}) and end_date({end_day}) covers "
                f"{span_days} days, which must not exceed {MAX_RANGE_DAYS}"
            )

        params = {
            "start_day": start_day,
            "end_day": end_day,
            "format": "json",
        }
        return self._fetch_v2_data(url, params, uid_list, convert_to_local_time, timeout)

    def getDailyDataV2(
        self,
        start_datetime: DateLike,
        end_datetime: DateLike,
        uid_list: Optional[list] = None,
        *,
        convert_to_local_time: bool = False,
        timeout: float = 60.0,
    ):
        """
        Retrieve daily detail data from the SOXAI v2 api within the specified datetime range.

        args:
            - start_datetime : the start of the datetime range, e.g.
              '2026-01-20T00:00:00+09:00'. Without timezone information it is read as utc,
              and with timezone information that offset is followed. The request always
              carries an explicit offset
            - end_datetime : the end of the datetime range, read the same way as
              start_datetime
            - uid_list : the uids to fetch data for
            - convert_to_local_time : keyword only, whether to convert the timestamps to
              local time
            - timeout : keyword only, the timeout in seconds (Up to 120.0). It is keyword
              only so that a caller that used to pass it as the fourth positional argument
              gets a TypeError instead of silently setting convert_to_local_time
        returns:
            - DataFrame containing the retrieved data, or None if every uid answered
              without data
        raises:
            - ValueError : if a datetime argument cannot be read as a date or datetime, if
              start_datetime is not before end_datetime, or if the range covers more than
              MAX_RANGE_DAYS days
            - TypeError : if convert_to_local_time or timeout is passed positionally
            - httpx.HTTPStatusError : if no uid answered with data and the api answered
              with an error status
        """
        if uid_list is None:
            uid_list = []

        url = self.url + 'v2/DailyDetailData/'

        # this endpoint works with instants, so each argument is read with the shared
        # timezone rule: without timezone information the value is read as utc, and with
        # timezone information that offset is followed
        start_timestamp = _to_aware_timestamp(start_datetime, 'start_datetime')
        end_timestamp = _to_aware_timestamp(end_datetime, 'end_datetime')

        # datetime range check (aware timestamps compare correctly across offsets)
        if start_timestamp >= end_timestamp:
            raise ValueError(
                f"start_datetime({start_datetime}) must be before end_datetime({end_datetime})"
            )

        # a longer range is rejected by the api, so it is a caller error rather than an
        # empty result. raising keeps the request that is certain to fail off the api
        span_days = (end_timestamp - start_timestamp).days
        if span_days > MAX_RANGE_DAYS:
            raise ValueError(
                f"the range of start_datetime({start_datetime}) and "
                f"end_datetime({end_datetime}) covers {span_days} days, which must not "
                f"exceed {MAX_RANGE_DAYS}"
            )

        # send an explicit offset so that the api never has to guess the timezone
        params = {
            "start_day": start_timestamp.isoformat(timespec='seconds'),
            "end_day": end_timestamp.isoformat(timespec='seconds'),
            "format": "json",
        }
        return self._fetch_v2_data(url, params, uid_list, convert_to_local_time, timeout)

    def _fetch_v2_data(self, url, params, uid_list, convert_to_local_time, timeout):
        """
        Fetch v2 api data for each uid and combine the results into one DataFrame.

        A uid that fails is reported and skipped, so the data of the uids that did
        succeed is still returned. When no uid answered with data and at least one
        request failed, that failure is raised instead: there is no data to isolate it
        from, and returning None would make a failed request look like a range that
        simply holds nothing.

        args:
            - url : the endpoint url, ending with a slash
            - params : the query parameters shared by every request
            - uid_list : the uids to fetch data for
            - convert_to_local_time : whether to convert the timestamps to local time
            - timeout : the timeout in seconds
        returns:
            - DataFrame containing the combined data, or None if every uid answered
              without data
        raises:
            - Exception : the first failure, when no uid answered with data and at least
              one request failed. An error status arrives as httpx.HTTPStatusError
        """
        fetched_data_list = []
        failed_uid_list = []
        # the first failure, kept so that a call that fetched nothing can report why
        first_error = None
        for uid in uid_list:
            try:
                response = httpx.get(
                    url + uid,
                    headers=self.headers,
                    params=params,
                    timeout=httpx.Timeout(timeout),
                )
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                print(f"Error in querying the data for {uid}", e)
                failed_uid_list.append(uid)
                if first_error is None:
                    first_error = e
                continue
            if not isinstance(data, list):
                # an error response is a dict, and extending with it would add its keys as rows
                print(f"Unexpected response for {uid} : {data}")
                failed_uid_list.append(uid)
                if first_error is None:
                    first_error = ValueError(f"Unexpected response for {uid} : {data}")
                continue
            fetched_data_list.extend(data)

        if failed_uid_list:
            print(
                f"Failed to fetch data for {len(failed_uid_list)} of {len(uid_list)} "
                f"uids : {failed_uid_list}"
            )

        # data length check
        if len(fetched_data_list) == 0:
            if first_error is not None:
                # nothing was fetched and at least one request failed, so report that
                # failure instead of letting it pass for an empty date range
                raise first_error
            print("No data fetched for the given uid list and date range.")
            return None

        df = pd.DataFrame(fetched_data_list)

        if convert_to_local_time:
            df = self._post_process_data(df)

        return df
