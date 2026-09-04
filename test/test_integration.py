"""
Integration tests that call the real SOXAI web api.

They are skipped unless SOXAI_API_TOKEN and SOXAI_UID are readable from the
environment or from a .env file, and also skipped when those credentials do not
authenticate, so that a normal `pytest` run never depends on the network. Run them
with `pytest -m integration` once the credentials are in place.
"""

import httpx
import pandas as pd
import pytest

from soxai_data import DataLoader
from soxai_data.get_ave_data import AverageDataExecutor, DataProcessing

from conftest import (
    DAILY_INFO_COLUMNS,
    INTEGRATION_END_DATE,
    INTEGRATION_END_DATETIME,
    INTEGRATION_START_DATE,
    INTEGRATION_START_DATETIME,
    LEGACY_DAILY_INFO_COLUMNS,
)

pytestmark = pytest.mark.integration

# columns that carry no measurement, so anything else is a field to average
NON_FIELD_COLUMNS = set(DAILY_INFO_COLUMNS)


@pytest.fixture(scope='session')
def live_loader(api_token):
    """
    Build a DataLoader against the real api, skipping when the token does not work.

    returns:
        - a DataLoader whose token has been proven to authenticate
    """
    loader = DataLoader(token=api_token)
    try:
        loader.getMyInfo()
    except httpx.HTTPStatusError as e:
        pytest.skip(f'SOXAI_API_TOKEN does not authenticate ({e.response.status_code}), '
                    'skipping the integration tests')
    except Exception as e:
        # anything else means the api cannot be reached from here, which is not a
        # failure of the library
        pytest.skip(f'the api is unreachable ({type(e).__name__}), skipping the integration tests')
    return loader


@pytest.fixture(scope='session')
def daily_info(live_loader, api_uid):
    """
    Fetch the daily info data of the range that is known to hold data.

    returns:
        - the fetched DataFrame
    """
    df = live_loader.getDailyInfoV2(
        start_date=INTEGRATION_START_DATE,
        end_date=INTEGRATION_END_DATE,
        uid_list=[api_uid],
    )
    if df is None:
        pytest.skip('the api returned no daily info data for the configured uid and range')
    return df


@pytest.fixture(scope='session')
def daily_detail(live_loader, api_uid):
    """
    Fetch the daily detail data of the range that is known to hold data.

    returns:
        - the fetched DataFrame
    """
    df = live_loader.getDailyDataV2(
        INTEGRATION_START_DATETIME, INTEGRATION_END_DATETIME, uid_list=[api_uid]
    )
    if df is None:
        pytest.skip('the api returned no daily detail data for the configured range')
    return df


class TestAccount:
    """The account endpoints against the real api."""

    def test_get_my_info_returns_a_mapping(self, live_loader):
        # The account information is a json object.
        assert isinstance(live_loader.getMyInfo(), dict)

    def test_get_my_info_reports_whether_i_am_an_org_user(self, live_loader):
        # The library reads isOrgUser to decide whether an organization id exists.
        assert 'isOrgUser' in live_loader.getMyInfo()

    def test_get_my_info_identifies_the_account(self, live_loader):
        # The account information names the user behind the token.
        info = live_loader.getMyInfo()
        assert 'uid' in info and 'email' in info

    def test_org_users_are_returned_for_an_org_user(self, live_loader):
        # A normal user has no organization, which is reported as None.
        info = live_loader.getMyInfo()
        result = live_loader.getMyOrgUsers()
        if info.get('isOrgUser'):
            assert isinstance(result, pd.DataFrame)
        else:
            assert result is None

    def test_an_invalid_token_raises(self, live_loader):
        # An unusable token has to surface instead of being swallowed. live_loader is
        # requested so that this test is skipped together with the rest of the module.
        with pytest.raises(httpx.HTTPStatusError):
            DataLoader(token='definitely-not-a-valid-token').getMyInfo()


class TestDailyInfoV2:
    """getDailyInfoV2 against the real api."""

    def test_the_known_range_holds_data(self, daily_info):
        # The configured range is the one the developer knows has data.
        assert len(daily_info) > 0

    def test_the_response_carries_the_columns_the_library_relies_on(self, daily_info):
        # The unit test fixtures are only faithful while these columns exist.
        missing = NON_FIELD_COLUMNS - set(daily_info.columns)
        assert not missing, f'the api no longer returns {sorted(missing)}'

    def test_the_response_carries_measurement_fields(self, daily_info):
        # Without any field there would be nothing to average.
        assert set(daily_info.columns) - NON_FIELD_COLUMNS

    def test_the_response_carries_the_documented_metric_groups(self, daily_info):
        # The batch averages the activity, health and sleep groups of the ring.
        prefixes = {c.split('_')[0] for c in daily_info.columns}
        assert {'activity', 'health', 'sleep'} <= prefixes

    def test_the_response_no_longer_carries_the_v1_columns(self, daily_info):
        # The v2 api dropped the influx bookkeeping columns, which is why the library
        # has to drop them defensively rather than unconditionally.
        assert not set(LEGACY_DAILY_INFO_COLUMNS) & set(daily_info.columns)

    def test_the_time_column_carries_an_explicit_offset(self, daily_info):
        # An explicit offset is what lets the library convert without guessing.
        assert pd.to_datetime(daily_info['_time']).dt.tz is not None

    def test_the_offset_of_the_user_is_reported(self, daily_info):
        # _post_process_data shifts _time by this column.
        assert pd.api.types.is_integer_dtype(daily_info['utc_offset_mins'])

    def test_every_row_belongs_to_the_requested_uid(self, daily_info, api_uid):
        # The endpoint is scoped to one uid.
        assert set(daily_info['uid']) == {api_uid}

    def test_rows_fall_inside_the_requested_range(self, daily_info):
        # The api must not return days outside the range that was asked for.
        times = pd.to_datetime(daily_info['_time'], utc=True)
        assert times.min() >= pd.Timestamp(INTEGRATION_START_DATE, tz='UTC')
        assert times.max() <= pd.Timestamp(INTEGRATION_END_DATE, tz='UTC') + pd.Timedelta(days=1)

    def test_convert_to_local_time_indexes_by_local_time(self, live_loader, api_uid):
        # The flag has to reach _post_process_data on a real response too.
        df = live_loader.getDailyInfoV2(
            start_date=INTEGRATION_START_DATE,
            end_date=INTEGRATION_END_DATE,
            uid_list=[api_uid],
            convert_to_local_time=True,
        )
        assert df.index.name == 'local_time'
        assert df.index[0].tzinfo is None

    def test_a_datetime_with_an_offset_is_accepted(self, live_loader, api_uid):
        # The day is taken at the given offset, which the api accepts as a plain date.
        df = live_loader.getDailyInfoV2(
            start_date=INTEGRATION_START_DATETIME,
            end_date=INTEGRATION_END_DATETIME,
            uid_list=[api_uid],
        )
        assert df is not None and len(df) > 0

    def test_an_unknown_uid_raises(self, live_loader):
        # A uid that does not exist is answered with 404, or with 403 when the caller is
        # a PartnerAdmin or an OrgAdmin, never with an empty result. This call carries no
        # other uid, so there is no data to isolate the failure from and it is raised.
        with pytest.raises(httpx.HTTPStatusError):
            live_loader.getDailyInfoV2(
                start_date=INTEGRATION_START_DATE,
                end_date=INTEGRATION_END_DATE,
                uid_list=['this-uid-does-not-exist'],
            )

    def test_a_failing_uid_does_not_discard_the_others(self, live_loader, api_uid):
        # One unknown uid must not throw away the data of the working one.
        df = live_loader.getDailyInfoV2(
            start_date=INTEGRATION_START_DATE,
            end_date=INTEGRATION_END_DATE,
            uid_list=['this-uid-does-not-exist', api_uid],
        )
        assert df is not None and set(df['uid']) == {api_uid}

    def test_an_inverted_range_raises_before_the_request(self, live_loader, api_uid):
        # The range check runs locally, so no request is made.
        with pytest.raises(ValueError, match='must not be after'):
            live_loader.getDailyInfoV2(
                start_date=INTEGRATION_END_DATE,
                end_date=INTEGRATION_START_DATE,
                uid_list=[api_uid],
            )


class TestDailyDataV2:
    """getDailyDataV2 against the real api."""

    def test_the_known_datetime_range_holds_data(self, daily_detail):
        # The configured datetime range is the one the developer knows has data.
        assert len(daily_detail) > 0

    def test_the_response_carries_the_structural_columns(self, daily_detail):
        # The detail endpoint answers in the same shape as the daily info endpoint.
        assert set(DAILY_INFO_COLUMNS) <= set(daily_detail.columns)

    def test_the_detail_data_is_finer_than_the_daily_data(self, daily_detail, daily_info):
        # The point of this endpoint is more than one row per day.
        assert len(daily_detail) > len(daily_info)

    def test_every_row_belongs_to_the_requested_uid(self, daily_detail, api_uid):
        # The endpoint is scoped to one uid.
        assert set(daily_detail['uid']) == {api_uid}

    def test_a_datetime_without_an_offset_is_accepted(self, live_loader, api_uid):
        # A naive datetime is read as utc, which the api accepts as an explicit offset.
        df = live_loader.getDailyDataV2(
            '2026-09-02T15:00:00', '2026-09-04T15:00:00', uid_list=[api_uid]
        )
        assert df is None or len(df) > 0

    def test_an_inverted_range_raises_before_the_request(self, live_loader, api_uid):
        # The range check runs locally, so no request is made.
        with pytest.raises(ValueError, match='must be before'):
            live_loader.getDailyDataV2(
                INTEGRATION_END_DATETIME, INTEGRATION_START_DATETIME, uid_list=[api_uid]
            )


class TestRawData:
    """getRawData against the real api."""

    def test_returns_the_raw_samples(self, live_loader, api_uid):
        # The raw endpoint answers with one row per sample.
        result = live_loader.getRawData(
            api_uid, start_date=INTEGRATION_START_DATETIME, end_date=INTEGRATION_END_DATETIME
        )
        if result is None:
            pytest.skip('the api returned no raw data for the configured range')
        assert len(result) > 0
        assert '_time' in result.columns

    def test_an_invalid_date_raises_before_the_request(self, live_loader, api_uid):
        # A bad date is a caller error, not an empty result.
        with pytest.raises(ValueError):
            live_loader.getRawData(api_uid, start_date='not a date')


class TestDataProcessingOnRealData:
    """DataProcessing against a real api response."""

    def test_real_data_averages_into_periods(self, daily_info):
        # The averaging has to work on the columns the api actually returns.
        result = DataProcessing().get_average_datas(
            daily_info, pd.Timestamp(INTEGRATION_END_DATE, tz='UTC'), 30
        )
        assert len(result) >= 1

    def test_the_result_holds_the_uid_and_the_period(self, daily_info, api_uid):
        # Every output row says which uid and which period it describes.
        result = DataProcessing().get_average_datas(
            daily_info, pd.Timestamp(INTEGRATION_END_DATE, tz='UTC'), 30
        )
        assert result['uid'].iloc[0] == api_uid
        assert 'start_date' in result.columns and 'end_date' in result.columns

    def test_the_result_holds_numeric_averages(self, daily_info):
        # The point of the batch is the numbers, so at least one field has to average.
        result = DataProcessing().get_average_datas(
            daily_info, pd.Timestamp(INTEGRATION_END_DATE, tz='UTC'), 30
        )
        fields = result.drop(columns=['uid', 'start_date', 'end_date'])
        assert any(pd.api.types.is_numeric_dtype(fields[c]) for c in fields.columns)

    def test_the_bookkeeping_columns_are_not_averaged(self, daily_info):
        # A column such as year is not a measurement.
        result = DataProcessing().get_average_datas(
            daily_info, pd.Timestamp(INTEGRATION_END_DATE, tz='UTC'), 30
        )
        for column in ('_time', '_start', '_stop', '_measurement', 'year', 'month', 'year_week'):
            assert column not in result.columns


class TestAverageDataExecutorEndToEnd:
    """AverageDataExecutor against the real api, writing to a temporary directory."""

    def test_writes_the_averages_of_the_configured_uid(self, api_token, api_uid, tmp_path,
                                                       live_loader):
        # The whole batch has to run against the real api and produce a result file.
        input_file = tmp_path / 'input_uid.csv'
        pd.DataFrame({'UID list': [api_uid]}).to_csv(input_file, index=False)

        executor = AverageDataExecutor(api_token, 30, str(input_file), str(tmp_path))
        executor.execute()

        results = sorted(tmp_path.glob('*_user_uid.csv'))
        assert len(results) == 1
        written = pd.read_csv(results[0])
        assert list(written.columns[:3]) == ['uid', 'start_date', 'end_date']
        assert set(written['uid']) == {api_uid}

    def test_completing_every_uid_sets_the_task_flag(self, api_token, api_uid, tmp_path,
                                                     live_loader):
        # A finished run tells the scheduler that there is nothing left to do.
        input_file = tmp_path / 'input_uid.csv'
        pd.DataFrame({'UID list': [api_uid]}).to_csv(input_file, index=False)

        executor = AverageDataExecutor(api_token, 30, str(input_file), str(tmp_path))
        executor.execute()

        assert executor.task_executed is True
        assert not list(tmp_path.glob('*_not_processed_uid.csv'))

    def test_an_unknown_uid_is_carried_over(self, api_token, api_uid, tmp_path, live_loader):
        # A uid the api cannot answer is left for the next run.
        input_file = tmp_path / 'input_uid.csv'
        pd.DataFrame({'UID list': ['this-uid-does-not-exist', api_uid]}).to_csv(
            input_file, index=False
        )

        executor = AverageDataExecutor(api_token, 30, str(input_file), str(tmp_path))
        executor.execute()

        leftover = sorted(tmp_path.glob('*_not_processed_uid.csv'))
        assert len(leftover) == 1
        assert list(pd.read_csv(leftover[0])['UID list']) == ['this-uid-does-not-exist']
        assert executor.task_executed is False
        assert executor.input_file == str(leftover[0])

    def test_a_run_outside_its_window_writes_nothing(self, api_token, api_uid, tmp_path):
        # The time window is checked before anything is fetched or written.
        input_file = tmp_path / 'input_uid.csv'
        pd.DataFrame({'UID list': [api_uid]}).to_csv(input_file, index=False)

        executor = AverageDataExecutor(api_token, 30, str(input_file), str(tmp_path))
        executor.execute(process_start_time='00:00', process_end_time='00:01')

        assert list(tmp_path.glob('*.csv')) == [input_file]
