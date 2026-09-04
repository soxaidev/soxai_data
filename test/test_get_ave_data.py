"""Unit tests for soxai_data.get_ave_data. Every external call is mocked."""

import datetime
from unittest import mock

import httpx
import pandas as pd
import pytest

from soxai_data import get_ave_data
from soxai_data.get_ave_data import (
    MAX_RANGE_DAYS,
    AverageDataExecutor,
    CsvFile,
    DataProcessing,
    SoxaiWebApi,
)


def make_http_status_error(status_code):
    """
    Build the error the web api client raises for an error status.

    args:
        - status_code : the http status of the answer
    returns:
        - the httpx.HTTPStatusError carrying a response with that status
    """
    request = httpx.Request('GET', 'https://web-api.example/api/')
    return httpx.HTTPStatusError(
        f'{status_code} error',
        request=request,
        response=httpx.Response(status_code, request=request),
    )


def make_rows(day_offsets, uid='uid-aaa', start='2022-03-01', naive=True, legacy=False):
    """
    Build daily info rows in the shape the real v2 api returns.

    args:
        - day_offsets : day offsets from start, one row each
        - uid : value of the uid column
        - start : the day offset 0 refers to
        - naive : whether _time carries no timezone information
        - legacy : whether to add the columns the influx based v1 api used to return
    returns:
        - the rows as a DataFrame
    """
    rows = []
    for offset in day_offsets:
        day = pd.Timestamp(start) + pd.Timedelta(days=offset)
        suffix = '' if naive else '+00:00'
        row = {
            '_time': day.strftime('%Y-%m-%dT00:00:00') + suffix,
            'uid': uid,
            'utc_offset_mins': 540,
            'ML_ver': None,
            'fw_ver': 120,
            'sleep_score': 80 + offset,
            'sleep_efficiency': 92.5,
            'health_hr_day_mean': 60,
            'sleep_start_time_true': '2022-02-28T23:10:00+09:00',
        }
        if legacy:
            row.update({
                '_start': 'x',
                '_stop': 'x',
                '_measurement': 'daily_info',
                'year': day.year,
                'month': day.month,
                'year_week': 'w',
                'workday': True,
            })
        rows.append(row)
    return pd.DataFrame(rows)


CURRENT_DATE = datetime.datetime(2022, 7, 1, tzinfo=datetime.timezone.utc)


class TestCsvFile:
    """CsvFile, with the pandas io calls mocked out."""

    def test_read_csv_df_reads_the_given_path(self):
        # The path is handed to pandas unchanged and the frame is returned.
        expected = pd.DataFrame({'UID list': ['uid-aaa']})
        with mock.patch.object(get_ave_data.pd, 'read_csv', return_value=expected) as read_csv:
            got = CsvFile().read_csv_df('/tmp/in.csv')
        read_csv.assert_called_once_with('/tmp/in.csv')
        assert got is expected

    def test_write_df_csv_omits_the_index(self):
        # An unnamed index column would be junk in the output file.
        df = mock.Mock(spec=pd.DataFrame)
        CsvFile().write_df_csv(df, '/tmp/out.csv')
        df.to_csv.assert_called_once_with('/tmp/out.csv', index=False)

    def test_write_csv_sort_index_sorts_by_uid_and_start_date(self):
        # The result file is grouped per uid and ordered in time.
        df = pd.DataFrame({
            'uid': ['uid-bbb', 'uid-aaa', 'uid-aaa'],
            'start_date': ['2022-03-01', '2022-04-01', '2022-03-01'],
        })
        with mock.patch.object(pd.DataFrame, 'to_csv') as to_csv:
            CsvFile().write_csv_sort_index(df, '/tmp/out.csv')
        written = to_csv.call_args[0]
        assert to_csv.call_count == 1
        assert written[0] == '/tmp/out.csv'

    def test_write_csv_sort_index_omits_the_index(self):
        # The sorted output must not carry the old row numbers.
        df = pd.DataFrame({'uid': ['uid-aaa'], 'start_date': ['2022-03-01']})
        with mock.patch.object(pd.DataFrame, 'to_csv') as to_csv:
            CsvFile().write_csv_sort_index(df, '/tmp/out.csv')
        assert to_csv.call_args[1] == {'mode': 'w', 'index': False}

    def test_write_csv_sort_index_orders_the_rows(self):
        # The rows reaching to_csv are the sorted ones.
        df = pd.DataFrame({
            'uid': ['uid-bbb', 'uid-aaa'],
            'start_date': ['2022-03-01', '2022-04-01'],
        })
        captured = {}

        def fake_to_csv(self, *args, **kwargs):
            """Record the uid order of the frame that reaches to_csv."""
            captured['uids'] = list(self['uid'])

        with mock.patch.object(pd.DataFrame, 'to_csv', fake_to_csv):
            CsvFile().write_csv_sort_index(df, '/tmp/out.csv')
        assert captured['uids'] == ['uid-aaa', 'uid-bbb']


class TestSoxaiWebApi:
    """SoxaiWebApi, with DataLoader mocked out."""

    def test_dataloader_gets_the_api_key(self):
        # The token reaches DataLoader.
        with mock.patch.object(get_ave_data, 'DataLoader') as loader_class:
            SoxaiWebApi('my-key')
        loader_class.assert_called_once_with(token='my-key')

    def test_arguments_are_passed_by_keyword(self):
        # getDailyInfoV2 takes uid_list before convert_to_local_time, so positional
        # arguments would swap the two.
        with mock.patch.object(get_ave_data, 'DataLoader') as loader_class:
            api = SoxaiWebApi('my-key')
            api.get_daily_data_by_uid(
                start_date='2022-03-01',
                end_date=None,
                convert_to_local_time=False,
                uid_list=['uid-aaa'],
                timeout=30.0,
            )
        loader_class.return_value.getDailyInfoV2.assert_called_once_with(
            start_date='2022-03-01',
            end_date=None,
            uid_list=['uid-aaa'],
            convert_to_local_time=False,
            timeout=30.0,
        )

    def test_missing_uid_list_becomes_empty(self):
        # A None uid_list must not reach the mutable default of the api method.
        with mock.patch.object(get_ave_data, 'DataLoader') as loader_class:
            SoxaiWebApi('my-key').get_daily_data_by_uid(start_date='2022-03-01')
        assert loader_class.return_value.getDailyInfoV2.call_args[1]['uid_list'] == []

    def test_returns_the_fetched_frame(self):
        # The DataFrame of the api is returned unchanged.
        expected = make_rows([0])
        with mock.patch.object(get_ave_data, 'DataLoader') as loader_class:
            loader_class.return_value.getDailyInfoV2.return_value = expected
            got = SoxaiWebApi('my-key').get_daily_data_by_uid(uid_list=['uid-aaa'])
        assert got is expected

    def test_failure_returns_none(self, capsys):
        # An error a later run may get past is reported and yields None.
        with mock.patch.object(get_ave_data, 'DataLoader') as loader_class:
            loader_class.return_value.getDailyInfoV2.side_effect = RuntimeError('boom')
            got = SoxaiWebApi('my-key').get_daily_data_by_uid(uid_list=['uid-aaa'])
        assert got is None
        assert 'failed to get data' in capsys.readouterr().out

    def test_a_server_error_returns_none(self, capsys):
        # A 5xx is the api failing on its own side, which a later run may get past.
        with mock.patch.object(get_ave_data, 'DataLoader') as loader_class:
            loader_class.return_value.getDailyInfoV2.side_effect = make_http_status_error(500)
            got = SoxaiWebApi('my-key').get_daily_data_by_uid(uid_list=['uid-aaa'])
        assert got is None
        assert 'failed to get data' in capsys.readouterr().out

    def test_a_rejected_request_is_raised(self, capsys):
        # A 4xx answers the same way however often it is asked, so hiding it behind None
        # would have the caller retry a request that cannot succeed.
        with mock.patch.object(get_ave_data, 'DataLoader') as loader_class:
            loader_class.return_value.getDailyInfoV2.side_effect = make_http_status_error(403)
            with pytest.raises(httpx.HTTPStatusError):
                SoxaiWebApi('my-key').get_daily_data_by_uid(uid_list=['uid-aaa'])
        assert 'rejected the request' in capsys.readouterr().out


class TestSortDfByTime:
    """DataProcessing.sort_df_by_time."""

    def test_time_becomes_utc_aware(self):
        # The comparisons against current_date need an aware column.
        df = DataProcessing().sort_df_by_time(make_rows([0]))
        assert df['_time'].iloc[0].tzinfo is not None

    def test_rows_are_ordered_by_time(self):
        # The oldest row has to come first for the period list to start correctly.
        df = DataProcessing().sort_df_by_time(make_rows([2, 0, 1]))
        assert list(df['sleep_score']) == [80, 81, 82]

    def test_input_dataframe_is_not_modified(self):
        # The caller's DataFrame must not be mutated in place.
        source = make_rows([0, 1])
        DataProcessing().sort_df_by_time(source)
        assert isinstance(source['_time'].iloc[0], str)

    def test_time_without_timezone_is_read_as_utc(self):
        # A naive timestamp is read as utc, matching the api client.
        df = DataProcessing().sort_df_by_time(make_rows([0], naive=True))
        assert str(df['_time'].iloc[0]) == '2022-03-01 00:00:00+00:00'

    def test_mixed_formats_are_ordered_correctly(self):
        # Converting before sorting keeps mixed offsets in the right order.
        df = pd.DataFrame({
            '_time': ['2022-03-02T00:00:00+09:00', '2022-03-01T20:00:00Z'],
            'uid': ['uid-aaa', 'uid-aaa'],
        })
        got = DataProcessing().sort_df_by_time(df)
        assert list(got.index) == [0, 1]


class TestGetAverageData:
    """DataProcessing.get_average_data."""

    def period(self):
        """
        Build a period dict.

        returns:
            - a start_date and end_date dict
        """
        return {
            'start_date': pd.Timestamp('2022-03-01', tz='UTC'),
            'end_date': pd.Timestamp('2022-03-30', tz='UTC'),
        }

    def test_returns_one_row(self):
        # A period is collapsed into a single row.
        df = DataProcessing().get_average_data(make_rows([0, 1]), self.period())
        assert len(df) == 1

    def test_numeric_fields_are_averaged(self):
        # The average of the period is what the caller asked for.
        df = DataProcessing().get_average_data(make_rows([0, 1]), self.period())
        assert df['sleep_score'].iloc[0] == 80.5

    def test_numbers_held_as_strings_are_coerced(self):
        # A field delivered as a string still has to be averaged as a number.
        source = make_rows([0, 1])
        source['health_hr_day_mean'] = ['60', '62']
        df = DataProcessing().get_average_data(source, self.period())
        assert df['health_hr_day_mean'].iloc[0] == 61.0

    def test_timestamp_string_fields_are_dropped(self):
        # sleep_start_time_true holds a timestamp, which would become a column of NaN.
        df = DataProcessing().get_average_data(make_rows([0, 1]), self.period())
        assert 'sleep_start_time_true' not in df.columns

    def test_always_empty_fields_are_dropped(self):
        # ML_ver is None in every row of the real response.
        df = DataProcessing().get_average_data(make_rows([0, 1]), self.period())
        assert 'ML_ver' not in df.columns

    def test_time_is_not_averaged(self):
        # _time is described by the period boundaries instead.
        df = DataProcessing().get_average_data(make_rows([0, 1]), self.period())
        assert '_time' not in df.columns

    def test_legacy_columns_are_dropped_when_present(self):
        # The v2 api no longer returns these, but a v1 shaped response is still handled.
        df = DataProcessing().get_average_data(make_rows([0, 1], legacy=True), self.period())
        for column in ('_start', '_stop', '_measurement', 'month', 'year', 'year_week'):
            assert column not in df.columns

    def test_metadata_fields_of_the_v2_response_are_averaged(self):
        # utc_offset_mins and fw_ver are not measurements but are not on the drop list,
        # so they are averaged. This pins the current behaviour of the real response.
        df = DataProcessing().get_average_data(make_rows([0, 1]), self.period())
        assert df['utc_offset_mins'].iloc[0] == 540.0
        assert df['fw_ver'].iloc[0] == 120.0

    def test_uid_and_period_are_kept(self):
        # The row has to say which uid and which period it describes.
        df = DataProcessing().get_average_data(make_rows([0, 1]), self.period())
        assert df['uid'].iloc[0] == 'uid-aaa'
        assert df['start_date'].iloc[0] == self.period()['start_date']
        assert df['end_date'].iloc[0] == self.period()['end_date']

    def test_missing_columns_do_not_raise(self):
        # The v2 api returns none of the legacy columns, so the drop has to tolerate that.
        df = DataProcessing().get_average_data(make_rows([0, 1]), self.period())
        assert df['sleep_score'].iloc[0] == 80.5

    def test_invalid_numbers_are_skipped_not_counted_as_zero(self):
        # to_numeric turns junk into NaN and mean() skips it.
        source = make_rows([0, 1])
        source['health_hr_day_mean'] = ['not a number', '60']
        df = DataProcessing().get_average_data(source, self.period())
        assert df['health_hr_day_mean'].iloc[0] == 60.0

    def test_single_row_period(self):
        # A period holding one day averages to that day's values.
        df = DataProcessing().get_average_data(make_rows([0]), self.period())
        assert df['sleep_score'].iloc[0] == 80


class TestDateHelpers:
    """DataProcessing.get_date_after_including_the_date and make_list_period_date."""

    def test_start_date_counts_as_the_first_day(self):
        # A 30 day period that starts on the first ends on the thirtieth.
        got = DataProcessing().get_date_after_including_the_date(pd.Timestamp('2022-03-01'), 30)
        assert got == pd.Timestamp('2022-03-30')

    def test_one_day_period_ends_on_the_start_date(self):
        # A single day period does not move the date.
        got = DataProcessing().get_date_after_including_the_date(pd.Timestamp('2022-03-01'), 1)
        assert got == pd.Timestamp('2022-03-01')

    def test_periods_start_at_the_oldest_row(self):
        # The first period begins on the day of the first row.
        df = DataProcessing().sort_df_by_time(make_rows([0]))
        periods = DataProcessing().make_list_period_date(df, CURRENT_DATE, 30)
        assert str(periods[0]['start_date'])[:10] == '2022-03-01'

    def test_periods_are_back_to_back(self):
        # The next period starts the day after the previous one ends.
        df = DataProcessing().sort_df_by_time(make_rows([0]))
        periods = DataProcessing().make_list_period_date(df, CURRENT_DATE, 30)
        for previous, following in zip(periods, periods[1:]):
            assert following['start_date'] - previous['end_date'] == pd.Timedelta(days=1)

    def test_last_period_reaches_current_date(self):
        # The list stops at the period that reaches or passes current_date.
        df = DataProcessing().sort_df_by_time(make_rows([0]))
        periods = DataProcessing().make_list_period_date(df, CURRENT_DATE, 30)
        assert periods[-1]['end_date'] >= pd.Timestamp(CURRENT_DATE)
        assert periods[-2]['end_date'] < pd.Timestamp(CURRENT_DATE)

    def test_naive_current_date_is_read_as_utc(self):
        # A naive current_date must not break the comparison against the aware column.
        df = DataProcessing().sort_df_by_time(make_rows([0]))
        periods = DataProcessing().make_list_period_date(df, datetime.datetime(2022, 7, 1), 30)
        assert len(periods) == 5

    def test_period_of_one_day_progresses(self):
        # A one day period still moves forward, one row per day.
        df = DataProcessing().sort_df_by_time(make_rows([0]))
        periods = DataProcessing().make_list_period_date(df, datetime.datetime(2022, 3, 5), 1)
        assert [str(p['start_date'])[:10] for p in periods] == [
            '2022-03-01', '2022-03-02', '2022-03-03', '2022-03-04', '2022-03-05',
        ]

    @pytest.mark.parametrize('period_cnt', [0, -1, -30])
    def test_non_positive_period_is_rejected(self, period_cnt):
        # A period below one day would never move the start forward and loop forever.
        df = DataProcessing().sort_df_by_time(make_rows([0]))
        with pytest.raises(ValueError, match='period_cnt must be 1 or greater'):
            DataProcessing().make_list_period_date(df, CURRENT_DATE, period_cnt)

    def test_single_period_when_current_date_is_inside_the_first_one(self):
        # A range shorter than one period yields exactly one period.
        df = DataProcessing().sort_df_by_time(make_rows([0]))
        periods = DataProcessing().make_list_period_date(df, datetime.datetime(2022, 3, 10), 30)
        assert len(periods) == 1


class TestGetPeriodDateDf:
    """DataProcessing.get_period_date_df."""

    def periods(self):
        """
        Build two consecutive one day periods.

        returns:
            - the periods as a list of dicts
        """
        return [
            {'start_date': pd.Timestamp('2022-03-01', tz='UTC'),
             'end_date': pd.Timestamp('2022-03-01', tz='UTC')},
            {'start_date': pd.Timestamp('2022-03-02', tz='UTC'),
             'end_date': pd.Timestamp('2022-03-02', tz='UTC')},
        ]

    def test_one_frame_per_period(self):
        # The result lines up with the period list.
        df = DataProcessing().sort_df_by_time(make_rows([0, 1]))
        assert len(DataProcessing().get_period_date_df(df, self.periods())) == 2

    def test_rows_are_assigned_to_their_period(self):
        # Each row lands in the period that contains its timestamp.
        df = DataProcessing().sort_df_by_time(make_rows([0, 1]))
        groups = DataProcessing().get_period_date_df(df, self.periods())
        assert [len(g) for g in groups] == [1, 1]

    def test_both_boundaries_are_inclusive(self):
        # A row exactly on the end date belongs to the period.
        df = DataProcessing().sort_df_by_time(make_rows([0]))
        period = [{'start_date': pd.Timestamp('2022-03-01', tz='UTC'),
                   'end_date': pd.Timestamp('2022-03-01', tz='UTC')}]
        assert len(DataProcessing().get_period_date_df(df, period)[0]) == 1

    def test_period_without_data_is_empty(self):
        # A period the user did not wear the ring yields an empty frame.
        df = DataProcessing().sort_df_by_time(make_rows([0]))
        groups = DataProcessing().get_period_date_df(df, self.periods())
        assert len(groups[1]) == 0


class TestGetAverageDatas:
    """DataProcessing.get_average_datas."""

    def test_one_row_per_period_that_holds_data(self):
        # Two populated periods produce two rows.
        df = make_rows([0, 1] + list(range(70, 76)))
        got = DataProcessing().get_average_datas(df, CURRENT_DATE, 30)
        assert len(got) == 2

    def test_an_empty_period_does_not_stop_the_later_ones(self):
        # A gap in the middle must not discard the periods after it.
        df = make_rows([0, 1] + list(range(70, 76)))
        got = DataProcessing().get_average_datas(df, CURRENT_DATE, 30)
        assert str(got['start_date'].iloc[1])[:10] == '2022-04-30'

    def test_index_is_reset(self):
        # The rows are numbered from zero for the csv output.
        df = make_rows([0, 1] + list(range(70, 76)))
        got = DataProcessing().get_average_datas(df, CURRENT_DATE, 30)
        assert list(got.index) == [0, 1]

    def test_returns_an_empty_frame_when_no_period_holds_data(self):
        # An empty input yields an empty frame rather than raising on concat.
        df = make_rows([0]).iloc[0:0]
        with mock.patch.object(DataProcessing, 'make_list_period_date', return_value=[]):
            got = DataProcessing().get_average_datas(df, CURRENT_DATE, 30)
        assert got.empty

    def test_naive_time_and_naive_current_date_work_together(self):
        # Neither side of the comparison may be left timezone naive.
        df = make_rows([0, 1], naive=True)
        got = DataProcessing().get_average_datas(df, datetime.datetime(2022, 7, 1), 30)
        assert len(got) == 1

    def test_aware_time_is_supported(self):
        # A response that carries offsets is handled the same way.
        df = make_rows([0, 1], naive=False)
        got = DataProcessing().get_average_datas(df, CURRENT_DATE, 30)
        assert len(got) == 1

    def test_invalid_period_is_rejected(self):
        # The period validation is reachable through the public entry point.
        with pytest.raises(ValueError):
            DataProcessing().get_average_datas(make_rows([0]), CURRENT_DATE, 0)


class TestGetTime:
    """AverageDataExecutor.get_time."""

    def executor(self):
        """
        Build an executor with dummy paths.

        returns:
            - the AverageDataExecutor under test
        """
        return AverageDataExecutor('key', 30, '/tmp/in.csv', '/tmp')

    def test_parses_hours_and_minutes(self):
        # An "hh:mm" string becomes the matching time.
        assert self.executor().get_time('09:30') == datetime.time(9, 30)

    def test_midnight_is_parsed(self):
        # 00:00 is a valid time, not a missing value.
        assert self.executor().get_time('00:00') == datetime.time(0, 0)

    def test_none_means_no_restriction(self):
        # None is how the caller says there is no time window.
        assert self.executor().get_time(None) is None

    @pytest.mark.parametrize('value', ['25:00', '09:60', 'abc', '09', '1:2:3', '09:xx', '', 930])
    def test_invalid_value_raises(self, value):
        # Returning None here would silently disable the time window.
        with pytest.raises(ValueError, match='should be hh:mm'):
            self.executor().get_time(value)


class TestWithinTimeRange:
    """AverageDataExecutor.within_time_range."""

    def executor(self):
        """
        Build an executor with dummy paths.

        returns:
            - the AverageDataExecutor under test
        """
        return AverageDataExecutor('key', 30, '/tmp/in.csv', '/tmp')

    def at(self, hour, minute=0):
        """
        Patch the current local time.

        args:
            - hour : hour of the fake current time
            - minute : minute of the fake current time
        returns:
            - a context manager that applies the patch
        """
        fake = mock.Mock(wraps=datetime)
        fake.datetime.now.return_value = datetime.datetime(2026, 9, 4, hour, minute)
        fake.time = datetime.time
        return mock.patch.object(get_ave_data, 'datetime', fake)

    def test_inside_a_normal_window(self):
        # A time between the boundaries is inside the window.
        with self.at(12):
            assert self.executor().within_time_range(datetime.time(9), datetime.time(18)) is True

    def test_outside_a_normal_window(self):
        # A time past the end is outside the window.
        with self.at(20):
            assert self.executor().within_time_range(datetime.time(9), datetime.time(18)) is False

    def test_boundaries_are_inclusive(self):
        # The window includes its own start and end.
        with self.at(9):
            assert self.executor().within_time_range(datetime.time(9), datetime.time(18)) is True
        with self.at(18):
            assert self.executor().within_time_range(datetime.time(9), datetime.time(18)) is True

    def test_inside_a_window_that_crosses_midnight(self):
        # A 22:00 to 02:00 window covers 23:30.
        with self.at(23, 30):
            assert self.executor().within_time_range(datetime.time(22), datetime.time(2)) is True

    def test_inside_a_window_that_crosses_midnight_after_midnight(self):
        # The same window still covers 01:00 the next morning.
        with self.at(1):
            assert self.executor().within_time_range(datetime.time(22), datetime.time(2)) is True

    def test_outside_a_window_that_crosses_midnight(self):
        # Midday is outside a night window.
        with self.at(12):
            assert self.executor().within_time_range(datetime.time(22), datetime.time(2)) is False

    def test_missing_start_means_no_restriction(self):
        # Without a start there is no window to be outside of.
        with self.at(12):
            assert self.executor().within_time_range(None, datetime.time(2)) is True

    def test_missing_end_means_no_restriction(self):
        # Without an end there is no window to be outside of.
        with self.at(12):
            assert self.executor().within_time_range(datetime.time(22), None) is True


@pytest.fixture
def executor_env(monkeypatch):
    """
    Patch the collaborators of AverageDataExecutor.execute.

    returns:
        - a namespace holding the executor and the mocked collaborators
    """
    csv_file = mock.Mock(spec=CsvFile)
    csv_file.read_csv_df.return_value = pd.DataFrame({'UID list': ['uid-aaa', 'uid-bbb']})
    web_api = mock.Mock(spec=SoxaiWebApi)
    web_api.get_daily_data_by_uid.side_effect = lambda **kwargs: make_rows(
        [0, 1], uid=kwargs['uid_list'][0]
    )
    processing = mock.Mock(spec=DataProcessing)
    processing.get_average_datas.side_effect = lambda df, current, period: pd.DataFrame({
        'uid': [df['uid'].iloc[0]],
        'start_date': [pd.Timestamp('2022-03-01', tz='UTC')],
        'end_date': [pd.Timestamp('2022-03-30', tz='UTC')],
        'sleep_score': [80.5],
    })
    monkeypatch.setattr(get_ave_data, 'CsvFile', mock.Mock(return_value=csv_file))
    monkeypatch.setattr(get_ave_data, 'SoxaiWebApi', mock.Mock(return_value=web_api))
    monkeypatch.setattr(get_ave_data, 'DataProcessing', mock.Mock(return_value=processing))
    return mock.Mock(
        executor=AverageDataExecutor('key', 30, '/tmp/in.csv', '/tmp/out'),
        csv_file=csv_file,
        web_api=web_api,
        processing=processing,
    )


def written_paths(csv_file):
    """
    Collect the paths CsvFile was asked to write.

    args:
        - csv_file : the mocked CsvFile
    returns:
        - the list of written paths
    """
    paths = [call[0][1] for call in csv_file.write_df_csv.call_args_list]
    paths += [call[0][1] for call in csv_file.write_csv_sort_index.call_args_list]
    return paths


class TestExecute:
    """AverageDataExecutor.execute."""

    def test_returns_immediately_outside_the_window(self, executor_env, capsys):
        # A run that starts outside its window must not fetch anything.
        with mock.patch.object(AverageDataExecutor, 'within_time_range', return_value=False):
            executor_env.executor.execute('09:00', '18:00')
        assert executor_env.web_api.get_daily_data_by_uid.call_count == 0
        assert 'not within_time_range' in capsys.readouterr().out

    def test_invalid_time_raises(self, executor_env):
        # A malformed window is a caller error, not a silent full day run.
        with pytest.raises(ValueError):
            executor_env.executor.execute('25:00', '18:00')

    def test_one_request_per_uid(self, executor_env):
        # Every uid of the input csv is fetched.
        executor_env.executor.execute()
        assert executor_env.web_api.get_daily_data_by_uid.call_count == 2

    def test_fetch_asks_for_the_longest_range_the_api_accepts(self, executor_env):
        # The window is exactly MAX_RANGE_DAYS days, sent as 'YYYY-MM-DD' strings.
        executor_env.executor.execute()
        sent = executor_env.web_api.get_daily_data_by_uid.call_args[1]
        span = pd.Timestamp(sent['end_date']) - pd.Timestamp(sent['start_date'])
        assert span == pd.Timedelta(days=MAX_RANGE_DAYS)

    def test_fetch_ends_on_the_day_of_the_run(self, executor_env):
        # Both ends come from the run's own clock, so a run that crosses midnight cannot
        # ask for a range the api rejects. The file prefix carries that same clock.
        executor_env.executor.execute()
        sent = executor_env.web_api.get_daily_data_by_uid.call_args[1]
        day = written_paths(executor_env.csv_file)[0].split('/')[-1][:8]
        assert sent['end_date'] == f'{day[:4]}-{day[4:6]}-{day[6:8]}'

    def test_fetch_asks_for_one_uid_at_a_time(self, executor_env):
        # The averaging is per uid, so the uids are fetched separately.
        executor_env.executor.execute()
        recorded = executor_env.web_api.get_daily_data_by_uid.call_args_list
        uid_lists = [call[1]['uid_list'] for call in recorded]
        assert uid_lists == [['uid-aaa'], ['uid-bbb']]

    def test_results_are_written_once(self, executor_env):
        # The averages of every uid go into a single output file.
        executor_env.executor.execute()
        assert executor_env.csv_file.write_csv_sort_index.call_count == 1

    def test_result_holds_every_uid(self, executor_env):
        # The written frame is the concatenation of the per uid results.
        executor_env.executor.execute()
        written = executor_env.csv_file.write_csv_sort_index.call_args[0][0]
        assert sorted(written['uid']) == ['uid-aaa', 'uid-bbb']

    def test_output_paths_share_the_run_prefix(self, executor_env):
        # All output files of one run carry the same timestamp prefix.
        executor_env.executor.execute()
        path = executor_env.csv_file.write_csv_sort_index.call_args[0][1]
        assert path.startswith('/tmp/out/') and path.endswith('_user_uid.csv')

    def test_output_prefix_carries_milliseconds(self, executor_env):
        # Two runs inside the same second would share a second precision prefix and write
        # over each other's results, so the prefix goes down to milliseconds.
        executor_env.executor.execute()
        path = executor_env.csv_file.write_csv_sort_index.call_args[0][1]
        stamp = path.split('/')[-1].split('_')[0]
        assert len(stamp) == 17 and stamp.isdigit()

    def test_completion_sets_the_task_flag(self, executor_env):
        # Once every uid is processed the scheduler can stop.
        executor_env.executor.execute()
        assert executor_env.executor.task_executed is True

    def test_completion_writes_no_leftover_file(self, executor_env):
        # With nothing left over there is no not processed file to write.
        executor_env.executor.execute()
        assert not any('not_processed' in p for p in written_paths(executor_env.csv_file))

    def test_completion_keeps_the_input_file(self, executor_env):
        # There is nothing to resume from, so the input stays as configured.
        executor_env.executor.execute()
        assert executor_env.executor.input_file == '/tmp/in.csv'

    def test_failed_fetch_is_recorded(self, executor_env):
        # A uid whose request failed goes into the failed file.
        executor_env.web_api.get_daily_data_by_uid.side_effect = [
            None, make_rows([0, 1], uid='uid-bbb'),
        ]
        executor_env.executor.execute()
        assert any('failed_uid' in p for p in written_paths(executor_env.csv_file))

    def test_failed_fetch_does_not_stop_the_other_uids(self, executor_env):
        # The uids after the failing one are still processed.
        executor_env.web_api.get_daily_data_by_uid.side_effect = [
            None, make_rows([0, 1], uid='uid-bbb'),
        ]
        executor_env.executor.execute()
        written = executor_env.csv_file.write_csv_sort_index.call_args[0][0]
        assert list(written['uid']) == ['uid-bbb']

    def test_failed_uid_is_carried_over(self, executor_env):
        # The next run has to retry the uid that failed.
        executor_env.web_api.get_daily_data_by_uid.side_effect = [
            None, make_rows([0, 1], uid='uid-bbb'),
        ]
        executor_env.executor.execute()
        leftover = [c[0][0] for c in executor_env.csv_file.write_df_csv.call_args_list
                    if 'not_processed' in c[0][1]]
        assert list(leftover[0]['UID list']) == ['uid-aaa']

    def test_processing_failure_is_isolated_per_uid(self, executor_env, capsys):
        # One uid that cannot be processed must not drop the rest of the run.
        executor_env.processing.get_average_datas.side_effect = [
            RuntimeError('boom'),
            pd.DataFrame({'uid': ['uid-bbb'], 'start_date': ['2022-03-01'],
                          'end_date': ['2022-03-30'], 'sleep_score': [80.5]}),
        ]
        executor_env.executor.execute()
        written = executor_env.csv_file.write_csv_sort_index.call_args[0][0]
        assert list(written['uid']) == ['uid-bbb']
        assert 'Error in process data (uid-aaa)' in capsys.readouterr().out

    def test_empty_response_is_skipped_without_failing(self, executor_env):
        # A uid with no data is neither a failure nor a result row.
        executor_env.web_api.get_daily_data_by_uid.side_effect = [
            make_rows([0]).iloc[0:0], make_rows([0, 1], uid='uid-bbb'),
        ]
        executor_env.executor.execute()
        assert not any('failed_uid' in p for p in written_paths(executor_env.csv_file))

    def test_leaving_the_window_stops_the_loop(self, executor_env):
        # The loop breaks as soon as the window closes.
        answers = iter([True, True, False])
        with mock.patch.object(AverageDataExecutor, 'within_time_range',
                               side_effect=lambda *a: next(answers)):
            executor_env.executor.execute('09:00', '18:00')
        assert executor_env.web_api.get_daily_data_by_uid.call_count == 1

    def test_leaving_the_window_does_not_mean_completion(self, executor_env):
        # Running out of time is not the same as having processed every uid.
        answers = iter([True, True, False])
        with mock.patch.object(AverageDataExecutor, 'within_time_range',
                               side_effect=lambda *a: next(answers)):
            executor_env.executor.execute('09:00', '18:00')
        assert executor_env.executor.task_executed is False

    def test_leaving_the_window_carries_the_rest_over(self, executor_env):
        # The unfinished uids are handed to the next run through input_file.
        answers = iter([True, True, False])
        with mock.patch.object(AverageDataExecutor, 'within_time_range',
                               side_effect=lambda *a: next(answers)):
            executor_env.executor.execute('09:00', '18:00')
        assert executor_env.executor.input_file.endswith('_not_processed_uid.csv')

    def test_nothing_fetched_leaves_every_uid_unprocessed(self, executor_env):
        # When no uid could be fetched the whole input is carried over.
        executor_env.web_api.get_daily_data_by_uid.side_effect = None
        executor_env.web_api.get_daily_data_by_uid.return_value = None
        executor_env.executor.execute()
        leftover = [c[0][0] for c in executor_env.csv_file.write_df_csv.call_args_list
                    if 'not_processed' in c[0][1]]
        assert list(leftover[0]['UID list']) == ['uid-aaa', 'uid-bbb']

    def test_nothing_fetched_writes_no_result_file(self, executor_env):
        # There is no average to write when nothing was fetched.
        executor_env.web_api.get_daily_data_by_uid.side_effect = None
        executor_env.web_api.get_daily_data_by_uid.return_value = None
        executor_env.executor.execute()
        assert executor_env.csv_file.write_csv_sort_index.call_count == 0

    def test_a_rejected_uid_does_not_earn_a_retry(self, executor_env, capsys):
        # A 4xx repeats on every run, so there is nothing for a later run to come back for.
        executor_env.web_api.get_daily_data_by_uid.side_effect = make_http_status_error(403)
        executor_env.executor.execute()
        assert executor_env.executor.task_executed is True
        assert 'nothing left that another run could fetch' in capsys.readouterr().out

    def test_a_rejected_uid_is_recorded_as_failed(self, executor_env):
        # Giving up on the uid still has to leave it on disk for a human to look at.
        executor_env.web_api.get_daily_data_by_uid.side_effect = make_http_status_error(403)
        executor_env.executor.execute()
        assert any('failed_uid' in p for p in written_paths(executor_env.csv_file))

    def test_a_rejected_uid_does_not_stop_the_other_uids(self, executor_env):
        # Only the rejected uid is dropped, the ones after it are still fetched.
        executor_env.web_api.get_daily_data_by_uid.side_effect = [
            make_http_status_error(404), make_rows([0, 1], uid='uid-bbb'),
        ]
        executor_env.executor.execute()
        written = executor_env.csv_file.write_csv_sort_index.call_args[0][0]
        assert list(written['uid']) == ['uid-bbb']

    def test_one_recoverable_failure_still_earns_a_retry(self, executor_env):
        # A rejected uid does not make the run hopeless while another one may recover.
        executor_env.web_api.get_daily_data_by_uid.side_effect = [
            make_http_status_error(403), None,
        ]
        executor_env.executor.execute()
        assert executor_env.executor.task_executed is False

    def test_a_fruitless_pass_keeps_the_scheduler_running(self, executor_env):
        # A run that produced nothing can be a transient failure, so it is worth a retry.
        executor_env.web_api.get_daily_data_by_uid.side_effect = None
        executor_env.web_api.get_daily_data_by_uid.return_value = None
        executor_env.executor.execute()
        assert executor_env.executor.task_executed is False

    def test_repeated_fruitless_passes_stop_the_scheduler(self, executor_env, capsys):
        # A run that never starts producing data must not keep the loop alive forever.
        executor_env.web_api.get_daily_data_by_uid.side_effect = None
        executor_env.web_api.get_daily_data_by_uid.return_value = None
        for _ in range(AverageDataExecutor.MAX_FRUITLESS_RUNS):
            executor_env.executor.execute()
        assert executor_env.executor.task_executed is True
        assert (f'{AverageDataExecutor.MAX_FRUITLESS_RUNS} runs produced no data, '
                'giving up on 2 uids') in capsys.readouterr().out

    def test_a_full_pass_with_one_result_keeps_the_scheduler_running(self, executor_env):
        # One uid still succeeded, so the failing one is worth one more run.
        executor_env.web_api.get_daily_data_by_uid.side_effect = [
            None, make_rows([0, 1], uid='uid-bbb'),
        ]
        executor_env.executor.execute()
        assert executor_env.executor.task_executed is False

    def test_a_truncated_pass_without_any_result_keeps_the_scheduler_running(self, executor_env):
        # The window closed before the second uid, so the next run still has work to do.
        executor_env.web_api.get_daily_data_by_uid.side_effect = None
        executor_env.web_api.get_daily_data_by_uid.return_value = None
        answers = iter([True, True, False])
        with mock.patch.object(AverageDataExecutor, 'within_time_range',
                               side_effect=lambda *a: next(answers)):
            executor_env.executor.execute('09:00', '18:00')
        assert executor_env.executor.task_executed is False

    def test_a_truncated_pass_of_rejected_uids_keeps_the_scheduler_running(self, executor_env):
        # The window closed before the second uid, which has not been tried even once, so
        # the run cannot be called hopeless even though nothing it saw was recoverable.
        executor_env.web_api.get_daily_data_by_uid.side_effect = make_http_status_error(403)
        answers = iter([True, True, False])
        with mock.patch.object(AverageDataExecutor, 'within_time_range',
                               side_effect=lambda *a: next(answers)):
            executor_env.executor.execute('09:00', '18:00')
        assert executor_env.executor.task_executed is False

    def test_repeated_truncated_passes_stop_the_scheduler(self, executor_env):
        # A window too short to reach any data never makes a full pass, and must not keep
        # the loop alive forever either.
        executor_env.web_api.get_daily_data_by_uid.side_effect = None
        executor_env.web_api.get_daily_data_by_uid.return_value = None
        runs = AverageDataExecutor.MAX_FRUITLESS_RUNS
        # every run enters the window, reaches the first uid and then finds it closed
        answers = iter([True, True, False] * runs)
        with mock.patch.object(AverageDataExecutor, 'within_time_range',
                               side_effect=lambda *a: next(answers)):
            for _ in range(runs):
                executor_env.executor.execute('09:00', '18:00')
        assert executor_env.executor.task_executed is True

    def test_a_run_that_produced_data_gives_the_retries_back(self, executor_env):
        # Data means the situation is not stuck, so the following runs count from zero.
        executor_env.web_api.get_daily_data_by_uid.side_effect = None
        executor_env.web_api.get_daily_data_by_uid.return_value = None
        for _ in range(AverageDataExecutor.MAX_FRUITLESS_RUNS - 1):
            executor_env.executor.execute()
        executor_env.web_api.get_daily_data_by_uid.return_value = make_rows([0, 1])
        executor_env.executor.execute()
        executor_env.web_api.get_daily_data_by_uid.return_value = None
        executor_env.executor.execute()
        assert executor_env.executor.task_executed is False

    def test_input_csv_is_read_once(self, executor_env):
        # The uid list is read a single time per run.
        executor_env.executor.execute()
        executor_env.csv_file.read_csv_df.assert_called_once_with('/tmp/in.csv')

    def test_timing_is_reported(self, executor_env, capsys):
        # The run prints how long it took.
        executor_env.executor.execute()
        out = capsys.readouterr().out
        assert 'start_datetime' in out and 'end_datetime' in out and 'gap_time' in out


class TestExecuteScheduler:
    """AverageDataExecutor.execute_scheduler."""

    def test_registers_a_daily_task(self, executor_env):
        # The run is scheduled once a day at the given time.
        with mock.patch.object(get_ave_data, 'schedule') as scheduler, \
                mock.patch.object(get_ave_data.time, 'sleep'):
            scheduler.run_pending.side_effect = lambda: setattr(
                executor_env.executor, 'task_executed', True
            )
            executor_env.executor.execute_scheduler('09:00', '18:00')
        scheduler.every.return_value.day.at.assert_called_once_with('09:00')

    def test_passes_the_window_to_execute(self, executor_env):
        # The scheduled call carries the same window it was registered with.
        with mock.patch.object(get_ave_data, 'schedule') as scheduler, \
                mock.patch.object(get_ave_data.time, 'sleep'):
            scheduler.run_pending.side_effect = lambda: setattr(
                executor_env.executor, 'task_executed', True
            )
            executor_env.executor.execute_scheduler('09:00', '18:00')
        kwargs = scheduler.every.return_value.day.at.return_value.do.call_args[1]
        assert kwargs == {'process_start_time': '09:00', 'process_end_time': '18:00'}

    def test_loops_until_the_task_is_done(self, executor_env):
        # The loop keeps polling the scheduler until every uid has been processed.
        calls = {'n': 0}

        def run_pending():
            """Count the polls and finish the run on the third one."""
            calls['n'] += 1
            if calls['n'] >= 3:
                executor_env.executor.task_executed = True

        with mock.patch.object(get_ave_data, 'schedule') as scheduler, \
                mock.patch.object(get_ave_data.time, 'sleep') as sleep:
            scheduler.run_pending.side_effect = run_pending
            executor_env.executor.execute_scheduler('09:00', '18:00')
        assert calls['n'] == 3
        assert sleep.call_count == 3

    def test_flag_is_reset_before_the_loop(self, executor_env):
        # A flag left over from an earlier run must not end the loop before it starts.
        executor_env.executor.task_executed = True
        polled = {'n': 0}

        def run_pending():
            """Count the polls and finish the run immediately."""
            polled['n'] += 1
            executor_env.executor.task_executed = True

        with mock.patch.object(get_ave_data, 'schedule') as scheduler, \
                mock.patch.object(get_ave_data.time, 'sleep'):
            scheduler.run_pending.side_effect = run_pending
            executor_env.executor.execute_scheduler('09:00', '18:00')
        assert polled['n'] == 1

    def test_retry_budget_is_reset_before_the_loop(self, executor_env):
        # A budget spent by an earlier scheduler run must not end this one early.
        executor_env.executor.fruitless_run_cnt = AverageDataExecutor.MAX_FRUITLESS_RUNS

        with mock.patch.object(get_ave_data, 'schedule') as scheduler, \
                mock.patch.object(get_ave_data.time, 'sleep'):
            scheduler.run_pending.side_effect = lambda: setattr(
                executor_env.executor, 'task_executed', True
            )
            executor_env.executor.execute_scheduler('09:00', '18:00')
        assert executor_env.executor.fruitless_run_cnt == 0

    def test_waits_between_polls(self, executor_env):
        # Polling without sleeping would spin the cpu at 100 percent.
        with mock.patch.object(get_ave_data, 'schedule') as scheduler, \
                mock.patch.object(get_ave_data.time, 'sleep') as sleep:
            scheduler.run_pending.side_effect = lambda: setattr(
                executor_env.executor, 'task_executed', True
            )
            executor_env.executor.execute_scheduler('09:00', '18:00')
        sleep.assert_called_once_with(1)
