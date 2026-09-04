"""
Batch helper that averages SOXAI daily info data over fixed length periods.

The entry point is AverageDataExecutor: it reads a csv of uids, fetches the daily info
data of each uid through DataLoader, averages every numeric field over fixed length
periods (30 days by default) and writes the result to a csv. It can run once or be
driven by a daily scheduler, in which case the uids it could not finish are carried
over to the next run.
"""

import datetime
import time
from typing import Dict, List, Optional

import httpx
import pandas as pd
import schedule

from .soxai_data import DataLoader, MAX_RANGE_DAYS


class CsvFile:
    """Read and write the csv files used by AverageDataExecutor."""

    def read_csv_df(self, input_file: str) -> pd.DataFrame:
        """
        Read a csv file into a DataFrame.

        args:
            - input_file : path of the csv file to read
        returns:
            - the DataFrame holding the contents of the file
        """
        df = pd.read_csv(input_file)
        return df

    def write_df_csv(self, df: pd.DataFrame, output_file: str) -> None:
        """
        Write a DataFrame to a csv file without the index.

        args:
            - df : the DataFrame to write
            - output_file : path of the csv file to write
        """
        df.to_csv(output_file, index=False)

    def write_csv_sort_index(self, df: pd.DataFrame, output_file: str) -> None:
        """
        Write a DataFrame to a csv file, sorted by uid and start_date.

        args:
            - df : the DataFrame to write, holding uid and start_date columns
            - output_file : path of the csv file to write
        """
        df_sorted = df.sort_values(['uid', 'start_date'])
        df_sorted.to_csv(output_file, mode='w', index=False)


class SoxaiWebApi:
    """Thin wrapper around DataLoader that only raises when a retry cannot help."""

    def __init__(self, api_key: str) -> None:
        """
        Create a DataLoader for the given api key.

        args:
            - api_key : the SOXAI api token
        """
        self.api_key = api_key
        self.sx_data = self.initialize_dataloader()

    def initialize_dataloader(self) -> DataLoader:
        """
        Build the DataLoader used for every request.

        returns:
            - the DataLoader authenticated with self.api_key
        """
        sx_data = DataLoader(token=self.api_key)
        return sx_data

    def get_daily_data_by_uid(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        convert_to_local_time: bool = False,
        uid_list: Optional[List[str]] = None,
        timeout: float = 60.0,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch daily info data for the given uids.

        args:
            - start_date : start of the range, e.g. '2022-03-01'. A value without
              timezone information is read as utc
            - end_date : end of the range, read the same way as start_date
            - convert_to_local_time : whether to convert the timestamps to local time
            - uid_list : the uids to fetch the data for
            - timeout : the timeout in seconds
        returns:
            - the fetched DataFrame, or None if the api answered without data or failed in
              a way a later run may get past
        raises:
            - httpx.HTTPStatusError : if the api answered with a 4xx status. The same
              request keeps getting that answer, so the caller must not retry it
        """
        if uid_list is None:
            uid_list = []
        try:
            # convert_to_local_time and timeout are keyword only on getDailyInfoV2
            df = self.sx_data.getDailyInfoV2(
                start_date=start_date,
                end_date=end_date,
                uid_list=uid_list,
                convert_to_local_time=convert_to_local_time,
                timeout=timeout,
            )
            return df
        except httpx.HTTPStatusError as e:
            if e.response.status_code < 500:
                # a bad request, a bad token, a missing permission or an unknown uid, all
                # of which answer the same way however often they are asked
                print(f'the web api rejected the request  {e}')
                raise
            # the api failed on its own side, which a later run may get past
            print(f'failed to get data from the web api  {e}')
            return None
        except Exception as e:
            # a timeout, a connection error or an unexpected payload, which a later run
            # may get past as well
            print(f'failed to get data from the web api  {e}')
            return None


class DataProcessing:
    """Split daily info data into fixed length periods and average each of them."""

    def sort_df_by_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert the _time column to utc aware datetimes and sort by it.

        Converting before sorting keeps the order correct when the api returns timestamps
        with different offsets, and forcing utc keeps the later comparisons against
        current_date from mixing naive and aware datetimes. pandas infers a single format
        for the whole column, so a response that mixes string formats raises ValueError
        here instead of being parsed row by row.

        args:
            - df : DataFrame holding a _time column
        returns:
            - a copy of df sorted by the converted _time column
        """
        df_converted = df.copy()
        df_converted['_time'] = pd.to_datetime(df_converted['_time'], utc=True)
        return df_converted.sort_values('_time')

    def get_average_data(
        self,
        df: pd.DataFrame,
        start_end_date_dict: Dict[str, pd.Timestamp],
    ) -> pd.DataFrame:
        """
        Average every numeric field of one period into a single row.

        args:
            - df : the rows of one period, holding a uid column
            - start_end_date_dict : the start_date and end_date of the period
        returns:
            - a one row DataFrame of uid, start_date, end_date and the field averages
        """
        # take the uid of the period together with the period boundaries
        df_start_date = pd.DataFrame({'start_date': [start_end_date_dict['start_date']]})
        df_end_date = pd.DataFrame({'end_date': [start_end_date_dict['end_date']]})
        df_uid = df.head(1)['uid'].reset_index(drop=True)
        # drop the columns that are not averaged, ignoring the ones the api did not return
        df_fields = df.drop(
            columns=[
                '_start', '_stop', '_measurement', '_time',
                'month', 'uid', 'workday', 'year', 'year_week',
            ],
            errors='ignore',
        )
        # coerce the object columns to numbers: fields such as sleep_start_time_true hold
        # a timestamp string and others are always None, and both become NaN here, which
        # mean() skips (they are not counted as 0)
        for column in df_fields.select_dtypes(include='object').columns:
            df_fields[column] = pd.to_numeric(df_fields[column], errors='coerce')
        # drop columns that hold no numeric value at all (e.g. free text fields)
        df_fields = df_fields.dropna(axis=1, how='all')
        # average every remaining field over the period
        df_average = pd.DataFrame(df_fields.mean(numeric_only=True)).transpose()
        df_average = df_average.reset_index(drop=True)
        # join the uid, the period boundaries and the field averages into one row
        df_result = pd.concat([df_uid, df_start_date, df_end_date, df_average], axis=1)
        return df_result

    def get_date_after_including_the_date(
        self,
        start_date: pd.Timestamp,
        plus_cnt: int,
    ) -> pd.Timestamp:
        """
        Move a date forward by plus_cnt days, counting start_date as the first day.

        args:
            - start_date : the first day of the range
            - plus_cnt : the number of days of the range, including start_date
        returns:
            - the last day of the range
        """
        end_date = start_date + datetime.timedelta(days=plus_cnt - 1)
        return end_date

    def make_list_period_date(
        self,
        df: pd.DataFrame,
        current_date: datetime.datetime,
        period_cnt: int = 30,
    ) -> List[Dict[str, pd.Timestamp]]:
        """
        Build the list of consecutive periods from the first row of df up to current_date.

        The periods are back to back: the next one starts on the day after the previous
        one ends. The last period is the one that reaches or passes current_date, so it
        may extend beyond it.

        args:
            - df : DataFrame sorted by the utc aware _time column
            - current_date : the date the periods are generated up to. A naive value is
              read as utc
            - period_cnt : the number of days of each period
        returns:
            - the list of periods, each as a start_date and end_date dict
        raises:
            - ValueError : if period_cnt is below 1
        """
        if period_cnt < 1:
            # anything below 1 never moves first_date forward, so the loop would never end
            raise ValueError(f'period_cnt must be 1 or greater, got {period_cnt}')
        # align current_date with the utc aware _time column
        current_date = pd.Timestamp(current_date)
        if current_date.tzinfo is None:
            current_date = current_date.tz_localize('UTC')
        # the first period starts on the day of the oldest row
        first_date = df['_time'].iloc[0]
        period_date_list = []
        while True:
            end_date = self.get_date_after_including_the_date(first_date, period_cnt)
            date_period = {
                'start_date': first_date,
                'end_date': end_date,
            }
            period_date_list.append(date_period)
            if end_date < current_date:
                first_date = self.get_date_after_including_the_date(end_date, 2)
            else:
                break
        return period_date_list

    def get_period_date_df(
        self,
        df: pd.DataFrame,
        date_period_list: List[Dict[str, pd.Timestamp]],
    ) -> List[pd.DataFrame]:
        """
        Split df into one DataFrame per period.

        args:
            - df : DataFrame holding a utc aware _time column
            - date_period_list : the periods to split df by
        returns:
            - one DataFrame per period, in the same order as date_period_list. A period
              without data yields an empty DataFrame
        """
        df_period_list = []
        for date_period in date_period_list:
            # take the rows that fall inside the period, both boundaries included
            df_period = df[
                (df['_time'] >= date_period['start_date'])
                & (df['_time'] <= date_period['end_date'])
            ]
            df_period_list.append(df_period)
        return df_period_list

    def get_average_datas(
        self,
        df: pd.DataFrame,
        current_date: datetime.datetime,
        period_cnt: int,
    ) -> pd.DataFrame:
        """
        Average the data of one uid over consecutive periods.

        args:
            - df : the daily info data of one uid, holding a _time column
            - current_date : the date the periods are generated up to
            - period_cnt : the number of days of each period
        returns:
            - one row per period that holds data, or an empty DataFrame if none does
        raises:
            - ValueError : if period_cnt is below 1
        """
        # order the rows by date
        df_sorted = self.sort_df_by_time(df)
        # list the periods from the oldest row up to current_date
        date_period_list = self.make_list_period_date(df_sorted, current_date, period_cnt)
        # split the rows into those periods
        df_period_list = self.get_period_date_df(df_sorted, date_period_list)
        # collect the average of each period and concat once at the end
        df_processed_list = []
        for date_period, df_group in zip(date_period_list, df_period_list):
            # skip empty periods so that later periods are still processed
            if len(df_group) < 1:
                continue
            df_processed_list.append(self.get_average_data(df_group, date_period))
        if not df_processed_list:
            return pd.DataFrame()
        return pd.concat(df_processed_list).reset_index(drop=True)


class AverageDataExecutor:
    """Run the averaging over a csv of uids, once or on a daily schedule."""

    # a run that produced no data at all is repeated on the following days up to this
    # many times. retrying covers a transient api failure, while the limit keeps a
    # permanent one from keeping execute_scheduler alive forever
    MAX_FRUITLESS_RUNS = 3

    def __init__(
        self,
        api_key: str,
        period_cnt: int,
        input_file: str,
        output_file_path: str,
    ) -> None:
        """
        Set up an executor.

        args:
            - api_key : the SOXAI api token
            - period_cnt : the number of days of each averaging period
            - input_file : path of the csv holding the uids, in a 'UID list' column
            - output_file_path : directory the result csv files are written to
        """
        self.api_key = api_key
        self.period_cnt = period_cnt
        self.input_file = input_file
        self.output_file_path = output_file_path
        self.task_executed = False
        # how many runs in a row produced no data, reset as soon as one produces some
        self.fruitless_run_cnt = 0

    def within_time_range(
        self,
        start_time: Optional[datetime.time],
        end_time: Optional[datetime.time],
    ) -> bool:
        """
        Tell whether the current local time is inside the given time window.

        args:
            - start_time : window start, or None for no restriction
            - end_time : window end, or None for no restriction
        returns:
            - True if there is no restriction, or the current time is in the window
        """
        if start_time is None or end_time is None:
            return True
        current_time = datetime.datetime.now().time()
        if start_time <= end_time:
            return start_time <= current_time <= end_time
        # the window crosses midnight (e.g. 22:00 - 02:00)
        return current_time >= start_time or current_time <= end_time

    def get_time(self, time_str: Optional[str]) -> Optional[datetime.time]:
        """
        Convert an "hh:mm" string into a datetime.time.

        args:
            - time_str : time string in "hh:mm" format, or None for no restriction
        returns:
            - the parsed datetime.time, or None when time_str is None
        raises:
            - ValueError : if time_str is not a valid "hh:mm" string
        """
        if time_str is None:
            return None
        try:
            hour_str, minute_str = time_str.split(':')
            return datetime.time(int(hour_str), int(minute_str), 0)
        except (ValueError, AttributeError) as e:
            # returning None here would silently disable the time window
            raise ValueError(f'Incorrect time format({time_str}), should be hh:mm') from e

    def execute(
        self,
        process_start_time: Optional[str] = None,
        process_end_time: Optional[str] = None,
    ) -> None:
        """
        Average the data of every uid of the input csv and write the results.

        The averages cover the newest MAX_RANGE_DAYS days ending on the day the run
        starts, which is the longest range the api answers in one request.

        Three csv files are written to output_file_path, all prefixed with the utc start
        time of the run:

        - <prefix>_user_uid.csv : the averages, sorted by uid and start_date
        - <prefix>_not_processed_uid.csv : the uids left over, written only if any
        - <prefix>_failed_uid.csv : the uids that failed, written only if any

        When uids are left over, input_file is repointed at the not processed csv so that
        the next run continues with them instead of starting over. task_executed, which is
        what stops execute_scheduler, is set once every uid has been processed, and also
        when repeating the run cannot be expected to help:

        - the run saw every uid of its input, produced no data and hit nothing that a
          later run could get past, so a repeat would do the same work for the same
          result. A uid the api rejects with a 4xx counts as answered for good here
        - MAX_FRUITLESS_RUNS runs in a row produced no data while hitting something a
          later run might have got past, a 5xx or a closing time window

        The leftover uids stay in the not processed and failed files either way.

        args:
            - process_start_time : start of the processing window in "hh:mm" format, or
              None for no restriction
            - process_end_time : end of the processing window in "hh:mm" format, or None
              for no restriction
        raises:
            - ValueError : if a time string is not in "hh:mm" format
        """
        # read the processing window given by the caller
        start_time = self.get_time(process_start_time)
        end_time = self.get_time(process_end_time)

        # stop right away when the run starts outside of the window
        if not self.within_time_range(start_time, end_time):
            print('not within_time_range')
            return

        # measure the start time
        start_datetime = datetime.datetime.now(datetime.timezone.utc)

        # output paths of this run, all sharing the start time as their prefix. the
        # prefix carries milliseconds so that two runs started inside the same second
        # cannot write over each other's results
        input_file = self.input_file
        run_id = start_datetime.strftime('%Y%m%d%H%M%S%f')[:-3]
        file_prefix = f'{self.output_file_path}/{run_id}'
        output_file_path = f'{file_prefix}_user_uid.csv'
        not_processed_uid_path = f'{file_prefix}_not_processed_uid.csv'
        failed_processed_uid_path = f'{file_prefix}_failed_uid.csv'
        # the day range data is fetched for. the api rejects a range longer than
        # MAX_RANGE_DAYS, and splitting it would cost one request per window per uid, so
        # a run covers the newest MAX_RANGE_DAYS days. both ends are taken from
        # start_datetime, so a run that crosses midnight cannot ask for one day too many.
        # getDailyInfoV2 reads a date without timezone information as utc
        end_date = start_datetime.strftime('%Y-%m-%d')
        start_date = (
            start_datetime - datetime.timedelta(days=MAX_RANGE_DAYS)
        ).strftime('%Y-%m-%d')

        # the averages of every processed uid
        df_result_list = []
        # the uids whose data could not be fetched or processed
        failed_processed_uid_list = []

        csv_file = CsvFile()
        soxai_web_api = SoxaiWebApi(self.api_key)
        data_processing = DataProcessing()

        # read the uids to process
        df_uid_list = csv_file.read_csv_df(input_file)

        # a fruitless run is only worth repeating when it did not see the whole input,
        # or when something went wrong that a later run may get past
        reached_every_uid = True
        transient_failure = False

        for uid in df_uid_list['UID list']:
            if not self.within_time_range(start_time, end_time):
                # out of the window, so leave the remaining uids to the next run
                print('ended tasks for today')
                reached_every_uid = False
                break
            print(f'processing uid {uid}')
            try:
                df = soxai_web_api.get_daily_data_by_uid(
                    start_date=start_date,
                    end_date=end_date,
                    convert_to_local_time=False,
                    uid_list=[uid],
                    timeout=60.0,
                )
            except httpx.HTTPStatusError as e:
                # the api rejected this uid for good, so retrying it cannot help. the
                # uids after it are unaffected and still worth fetching
                failed_processed_uid_list.append(uid)
                print(f'the web api rejected uid {uid} : {e}')
                continue
            if df is None:
                # the request failed in a way a later run may get past, so keep the uid
                # for the next run and let this run earn a retry
                failed_processed_uid_list.append(uid)
                transient_failure = True
                print('failed_get_from_web_api')
                continue
            if len(df) < 1:
                continue
            try:
                df_result = data_processing.get_average_datas(df, start_datetime, self.period_cnt)
            except Exception as e:
                # isolate the failure per uid so that one bad uid does not drop the rest
                failed_processed_uid_list.append(uid)
                print(f'Error in process data ({uid}) : {e}')
                continue
            df_result_list.append(df_result)

        if df_result_list:
            df_all_results = pd.concat(df_result_list, axis=0)
            # write the averages sorted by uid and start_date
            csv_file.write_csv_sort_index(df_all_results, output_file_path)
            df_not_processed_uid = df_uid_list[~df_uid_list['UID list'].isin(df_all_results['uid'])]
        else:
            df_not_processed_uid = df_uid_list

        # spend a retry on a run that produced nothing, and give the budget back as soon
        # as a run produces something, so that only a stuck situation exhausts it
        if df_result_list:
            self.fruitless_run_cnt = 0
        else:
            self.fruitless_run_cnt += 1

        if len(df_not_processed_uid) == 0:
            # every uid has been processed, so the scheduler can stop
            self.task_executed = True
        else:
            csv_file.write_df_csv(df_not_processed_uid, not_processed_uid_path)
            # resume from the remaining uids on the next run instead of starting over
            self.input_file = not_processed_uid_path
            # the uids are left in the not processed and failed files to be looked at in
            # both of the cases below, which is what stops execute_scheduler
            if reached_every_uid and not df_result_list and not transient_failure:
                # every uid of the input was seen, none of them produced data and nothing
                # went wrong that a later run could get past, so a repeat would do exactly
                # the same work for exactly the same result
                print(
                    'nothing left that another run could fetch, giving up on '
                    f'{len(df_not_processed_uid)} uids'
                )
                self.task_executed = True
            elif self.fruitless_run_cnt >= self.MAX_FRUITLESS_RUNS:
                # the runs that produced nothing did hit something a later run might have
                # got past, a 5xx or a closing time window, but not often enough to keep
                # trying for good
                print(
                    f'{self.fruitless_run_cnt} runs produced no data, giving up on '
                    f'{len(df_not_processed_uid)} uids'
                )
                self.task_executed = True

        if len(failed_processed_uid_list) > 0:
            df_failed_uid = pd.DataFrame(failed_processed_uid_list, columns=['UID list'])
            csv_file.write_df_csv(df_failed_uid, failed_processed_uid_path)

        # measure the end time
        end_datetime = datetime.datetime.now(datetime.timezone.utc)

        print(f'start_datetime : {start_datetime}')
        print(f'end_datetime   : {end_datetime}')
        print(f'gap_time : {end_datetime - start_datetime}')

    def execute_scheduler(self, schedule_start_time: str, schedule_end_time: str) -> None:
        """
        Run execute once a day until every uid of the input csv has been processed.

        A run that goes past schedule_end_time stops and hands its remaining uids to the
        run of the next day, so the loop can span several days. It also ends as soon as a
        run cannot be usefully repeated, and after MAX_FRUITLESS_RUNS runs in a row that
        produced no data, so that uids the api rejects, uids that hold no data and uids a
        5xx keeps failing do not keep the loop alive forever.

        args:
            - schedule_start_time : time of day the run starts, in "hh:mm" format
            - schedule_end_time : time of day the run stops, in "hh:mm" format
        """
        print(f'this program will start at {schedule_start_time} and end at {schedule_end_time}')

        # register the daily task
        schedule.every().day.at(schedule_start_time).do(
            self.execute,
            process_start_time=schedule_start_time,
            process_end_time=schedule_end_time,
        )
        # set once every uid has been processed
        self.task_executed = False
        # the retry budget belongs to this scheduler run
        self.fruitless_run_cnt = 0

        while not self.task_executed:
            # wait for the scheduled time
            schedule.run_pending()
            time.sleep(1)

        print('All done')
