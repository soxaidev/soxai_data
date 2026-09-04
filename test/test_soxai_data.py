"""Unit tests for soxai_data.soxai_data. Every external call is mocked."""

import datetime
import json

import httpx
import pandas as pd
import pytest

from soxai_data import DataLoader
from soxai_data.soxai_data import _to_aware_timestamp

from conftest import (
    FakeResponse,
    make_daily_info_row,
    make_daily_info_rows,
    make_legacy_daily_info_row,
)

UTC_EPOCH = 1788393600        # 2026-09-03T00:00:00+00:00
JST_EPOCH = 1788361200        # 2026-09-03T00:00:00+09:00


class TestToAwareTimestamp:
    """The timezone rule shared by every date argument."""

    @pytest.mark.parametrize('value', [
        '2026-09-03',
        '2026-09-03T00:00:00',
        '2026-09-03 00:00:00',
        datetime.date(2026, 9, 3),
        datetime.datetime(2026, 9, 3),
        pd.Timestamp('2026-09-03'),
    ])
    def test_value_without_timezone_is_read_as_utc(self, value):
        # A value carrying no timezone information is interpreted as utc.
        assert int(_to_aware_timestamp(value, 'start_date').timestamp()) == UTC_EPOCH

    @pytest.mark.parametrize('value', [
        '2026-09-03T00:00:00+09:00',
        '2026-09-03 00:00:00+09:00',
        datetime.datetime(2026, 9, 3, tzinfo=datetime.timezone(datetime.timedelta(hours=9))),
        pd.Timestamp('2026-09-03T00:00:00+09:00'),
    ])
    def test_value_with_timezone_keeps_its_offset(self, value):
        # A value carrying timezone information keeps the given offset.
        assert int(_to_aware_timestamp(value, 'start_date').timestamp()) == JST_EPOCH

    def test_z_suffix_is_read_as_utc(self):
        # A 'Z' suffix is an explicit utc offset.
        assert int(_to_aware_timestamp('2026-09-03T00:00:00Z', 'x').timestamp()) == UTC_EPOCH

    def test_negative_offset_is_followed(self):
        # A negative offset shifts the instant forward relative to utc.
        got = _to_aware_timestamp('2026-09-03T00:00:00-05:00', 'x')
        assert int(got.timestamp()) == UTC_EPOCH + 5 * 3600

    def test_result_is_always_timezone_aware(self):
        # The returned Timestamp always carries a timezone.
        assert _to_aware_timestamp('2026-09-03', 'x').tzinfo is not None

    @pytest.mark.parametrize('value', [None, '', 'abc', '2026-99-99'])
    def test_unparsable_value_raises_value_error(self, value):
        # A value that cannot be read as a date raises ValueError, including None and ''.
        with pytest.raises(ValueError):
            _to_aware_timestamp(value, 'start_date')

    @pytest.mark.parametrize('value', [5, 0, -1, 2026.0])
    def test_number_is_rejected(self, value):
        # A bare number would silently become a 1970 timestamp, so it is rejected.
        with pytest.raises(ValueError):
            _to_aware_timestamp(value, 'start_date')

    def test_object_is_rejected(self):
        # A value pandas cannot read at all raises ValueError, not TypeError.
        with pytest.raises(ValueError):
            _to_aware_timestamp(object(), 'start_date')

    def test_error_message_names_the_argument(self):
        # The error message names the argument so the caller knows which one is wrong.
        with pytest.raises(ValueError, match='end_datetime'):
            _to_aware_timestamp('abc', 'end_datetime')


class TestDataLoaderInit:
    """Construction of DataLoader."""

    def test_token_is_sent_in_the_api_key_header(self):
        # The token is placed in the soxai-api-key header.
        loader = DataLoader(token='my-token')
        assert loader.headers['soxai-api-key'] == 'my-token'

    def test_base_url_ends_with_a_slash(self):
        # Every method appends to the base url, so it has to end with a slash.
        assert DataLoader(token='t').url.endswith('/')

    def test_org_id_starts_unset(self):
        # The organization id is only known after getMyInfo runs.
        assert DataLoader(token='t').org_id is None


class TestGetMyInfo:
    """DataLoader.getMyInfo."""

    def test_returns_the_payload(self, loader, fake_httpx):
        # The parsed json payload is returned as is.
        fake_httpx.route('myOrg', FakeResponse({'isOrgUser': False, 'name': 'me'}))
        assert loader.getMyInfo() == {'isOrgUser': False, 'name': 'me'}

    def test_stores_the_org_id_for_an_org_user(self, loader, fake_httpx):
        # An org user's organization id is cached on the loader.
        fake_httpx.route('myOrg', FakeResponse({'isOrgUser': True, 'myOrg': {'orgId': 'org-1'}}))
        loader.getMyInfo()
        assert loader.org_id == 'org-1'

    def test_does_not_store_the_org_id_for_a_normal_user(self, loader, fake_httpx):
        # A normal user has no organization, so org_id stays None.
        fake_httpx.route('myOrg', FakeResponse({'isOrgUser': False}))
        loader.getMyInfo()
        assert loader.org_id is None

    def test_missing_org_key_does_not_raise(self, loader, fake_httpx):
        # A payload that claims an org but omits it is tolerated.
        fake_httpx.route('myOrg', FakeResponse({'isOrgUser': True}))
        loader.getMyInfo()
        assert loader.org_id is None

    def test_non_dict_payload_does_not_raise(self, loader, fake_httpx):
        # A payload without .keys() is tolerated and returned unchanged.
        fake_httpx.route('myOrg', FakeResponse(['unexpected']))
        assert loader.getMyInfo() == ['unexpected']

    def test_http_error_raises(self, loader, fake_httpx):
        # An invalid token has to surface instead of being swallowed.
        fake_httpx.route('myOrg', FakeResponse({'error': 'x'}, status_code=401))
        with pytest.raises(httpx.HTTPStatusError):
            loader.getMyInfo()

    def test_requests_the_my_org_endpoint(self, loader, fake_httpx):
        # The account information comes from the myOrg endpoint.
        fake_httpx.route('myOrg', FakeResponse({}))
        loader.getMyInfo()
        assert fake_httpx.last_url == loader.url + 'myOrg'


class TestGetMyOrgUsers:
    """DataLoader.getMyOrgUsers."""

    def test_explicit_org_id_is_used(self, loader, fake_httpx):
        # The given org_id goes into the url without looking up my own organization.
        fake_httpx.route('orgUsers', FakeResponse([{'uid': 'uid-aaa'}]))
        loader.getMyOrgUsers(org_id='other-org')
        assert 'orgs/other-org/orgUsers' in fake_httpx.last_url
        assert len(fake_httpx.calls) == 1

    def test_cached_org_id_is_used_when_no_argument_is_given(self, loader, fake_httpx):
        # A cached org_id saves the getMyInfo round trip.
        loader.org_id = 'org-1'
        fake_httpx.route('orgUsers', FakeResponse([{'uid': 'uid-aaa'}]))
        loader.getMyOrgUsers()
        assert 'orgs/org-1/orgUsers' in fake_httpx.last_url
        assert len(fake_httpx.calls) == 1

    def test_org_id_is_looked_up_when_unknown(self, loader, fake_httpx):
        # Without an argument or a cached id, getMyInfo resolves the organization first.
        fake_httpx.route('myOrg', FakeResponse({'isOrgUser': True, 'myOrg': {'orgId': 'org-9'}}))
        fake_httpx.route('orgUsers', FakeResponse([{'uid': 'uid-aaa'}]))
        loader.getMyOrgUsers()
        assert 'orgs/org-9/orgUsers' in fake_httpx.last_url
        assert len(fake_httpx.calls) == 2

    def test_returns_none_when_the_organization_cannot_be_resolved(self, loader, fake_httpx):
        # A normal user has no organization to list.
        fake_httpx.route('myOrg', FakeResponse({'isOrgUser': False}))
        assert loader.getMyOrgUsers() is None

    def test_returns_a_dataframe_of_the_users(self, loader, fake_httpx):
        # The user list is returned as a DataFrame.
        fake_httpx.route('orgUsers', FakeResponse([{'uid': 'uid-aaa'}, {'uid': 'uid-bbb'}]))
        df = loader.getMyOrgUsers(org_id='org-1')
        assert list(df['uid']) == ['uid-aaa', 'uid-bbb']

    def test_returns_none_when_the_payload_is_unreadable(self, loader, fake_httpx):
        # A body that is not json yields None instead of an exception.
        fake_httpx.route('orgUsers', FakeResponse(json_error=ValueError('not json')))
        assert loader.getMyOrgUsers(org_id='org-1') is None

    def test_http_error_raises(self, loader, fake_httpx):
        # An error status surfaces to the caller.
        fake_httpx.route('orgUsers', FakeResponse({}, status_code=403))
        with pytest.raises(httpx.HTTPStatusError):
            loader.getMyOrgUsers(org_id='org-1')


class TestPostProcessData:
    """DataLoader.post_process_data."""

    def test_indexes_by_local_time(self, loader):
        # The result is indexed by local_time.
        df = loader.post_process_data(pd.DataFrame([make_daily_info_row()]))
        assert df.index.name == 'local_time'

    def test_local_time_is_the_wall_clock_of_the_offset(self, loader):
        # A +09:00 user's midnight utc row becomes 09:00 local wall clock.
        df = loader.post_process_data(pd.DataFrame([make_daily_info_row(utc_offset_mins=540)]))
        assert str(df.index[0]) == '2026-09-03 09:00:00'

    def test_negative_offset_moves_the_wall_clock_back(self, loader):
        # A -300 minute offset moves the wall clock to the previous day.
        df = loader.post_process_data(pd.DataFrame([make_daily_info_row(utc_offset_mins=-300)]))
        assert str(df.index[0]) == '2026-09-02 19:00:00'

    def test_local_time_is_timezone_naive(self, loader):
        # Keeping the utc label would make the local wall clock look like utc.
        df = loader.post_process_data(pd.DataFrame([make_daily_info_row()]))
        assert df.index[0].tzinfo is None

    def test_time_without_timezone_is_read_as_utc(self, loader):
        # The _time column follows the same rule as the date arguments.
        row = make_daily_info_row(time='2026-09-03T00:00:00', utc_offset_mins=540)
        df = loader.post_process_data(pd.DataFrame([row]))
        assert str(df.index[0]) == '2026-09-03 09:00:00'

    def test_time_with_timezone_keeps_its_offset(self, loader):
        # An offset on _time is followed: 00:00+09:00 is 15:00 utc, which plus 540 minutes
        # is midnight of the next day.
        row = make_daily_info_row(time='2026-09-03T00:00:00+09:00', utc_offset_mins=540)
        df = loader.post_process_data(pd.DataFrame([row]))
        assert str(df.index[0]) == '2026-09-03 00:00:00'

    def test_the_raw_time_column_is_replaced(self, loader):
        # _time is superseded by the local_time index.
        df = loader.post_process_data(pd.DataFrame([make_daily_info_row()]))
        assert '_time' not in df.columns

    def test_legacy_columns_are_dropped_when_present(self, loader):
        # The v2 api no longer returns these, but a v1 shaped response is still handled.
        df = loader.post_process_data(pd.DataFrame([make_legacy_daily_info_row()]))
        for column in ('_start', '_stop', '_time', '_measurement'):
            assert column not in df.columns

    def test_metric_columns_are_kept(self, loader):
        # The measurements themselves survive the post processing.
        df = loader.post_process_data(pd.DataFrame([make_daily_info_row()]))
        assert 'sleep_score' in df.columns and 'uid' in df.columns

    def test_input_dataframe_is_not_modified(self, loader):
        # The caller's DataFrame must not be mutated in place.
        source = pd.DataFrame([make_daily_info_row()])
        before = list(source.columns)
        loader.post_process_data(source)
        assert list(source.columns) == before

    def test_missing_optional_columns_do_not_raise(self, loader):
        # The v2 api returns none of the legacy columns, so the drop has to tolerate that.
        df = loader.post_process_data(pd.DataFrame([make_daily_info_row()]))
        assert df.index.name == 'local_time'

    def test_rows_keep_their_own_offset(self, loader):
        # Each row is shifted by its own utc_offset_mins.
        rows = [make_daily_info_row(utc_offset_mins=540), make_daily_info_row(utc_offset_mins=0)]
        df = loader.post_process_data(pd.DataFrame(rows))
        assert [str(i) for i in df.index] == ['2026-09-03 09:00:00', '2026-09-03 00:00:00']


class TestGetRawData:
    """DataLoader.getRawData."""

    def test_explicit_dates_without_timezone_are_read_as_utc(self, loader, fake_httpx):
        # A naive date becomes the utc instant of that date.
        loader.getRawData('uid-aaa', start_date='2026-09-03', end_date='2026-09-05')
        assert f'start_time={UTC_EPOCH}' in fake_httpx.last_url

    def test_explicit_dates_with_timezone_follow_the_offset(self, loader, fake_httpx):
        # A +09:00 date is nine hours earlier than the same utc date.
        loader.getRawData('uid-aaa', start_date='2026-09-03T00:00:00+09:00')
        assert f'start_time={JST_EPOCH}' in fake_httpx.last_url

    def test_default_range_is_the_last_seven_days_in_utc(self, loader, fake_httpx, frozen_now):
        # Without arguments the range is the seven days ending at the current utc time.
        loader.getRawData('uid-aaa')
        query = fake_httpx.last_url.split('?')[1]
        start = int(query.split('start_time=')[1].split('&')[0])
        stop = int(query.split('stop_time=')[1].split('&')[0])
        assert stop == int(frozen_now.timestamp())
        assert stop - start == 7 * 24 * 3600

    def test_uid_is_part_of_the_path(self, loader, fake_httpx):
        # The endpoint is scoped to one uid.
        loader.getRawData('uid-aaa')
        assert 'RawData/uid-aaa' in fake_httpx.last_url

    def test_list_payload_becomes_a_dataframe(self, loader, fake_httpx):
        # A json array is loaded directly.
        fake_httpx.route('*', FakeResponse([{'hr': 60}, {'hr': 61}]))
        assert list(loader.getRawData('uid-aaa')['hr']) == [60, 61]

    def test_json_encoded_string_payload_is_decoded(self, loader, fake_httpx):
        # This endpoint has been seen returning the payload as a json string.
        fake_httpx.route('*', FakeResponse(json.dumps([{'hr': 60}, {'hr': 61}])))
        assert list(loader.getRawData('uid-aaa')['hr']) == [60, 61]

    def test_http_error_returns_none(self, loader, fake_httpx):
        # An error status is reported and yields None.
        fake_httpx.route('*', FakeResponse({}, status_code=500))
        assert loader.getRawData('uid-aaa') is None

    def test_request_failure_returns_none(self, loader, fake_httpx):
        # A transport failure yields None rather than propagating.
        fake_httpx.route('*', httpx.ConnectError('boom'))
        assert loader.getRawData('uid-aaa') is None

    def test_invalid_date_raises_before_the_request(self, loader, fake_httpx):
        # A bad date is a caller error, so it is not turned into None.
        with pytest.raises(ValueError):
            loader.getRawData('uid-aaa', start_date='abc')
        assert fake_httpx.calls == []

    def test_timeout_is_passed_through(self, loader, fake_httpx):
        # The timeout argument reaches httpx.
        loader.getRawData('uid-aaa', timeout=12.5)
        assert fake_httpx.calls[-1]['timeout'] is not None


class TestGetDailyInfoV2:
    """DataLoader.getDailyInfoV2."""

    def test_explicit_dates_are_sent_as_days(self, loader, fake_httpx):
        # A plain date is sent unchanged.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyInfoV2(start_date='2026-09-03', end_date='2026-09-05', uid_list=['uid-aaa'])
        assert fake_httpx.last_params['start_day'] == '2026-09-03'
        assert fake_httpx.last_params['end_day'] == '2026-09-05'

    def test_offset_selects_the_day_seen_at_that_offset(self, loader, fake_httpx):
        # 23:00 at -05:00 is still the third in that timezone, even though it is the fourth in utc.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyInfoV2(start_date='2026-09-03T23:00:00-05:00', end_date='2026-09-05',
                              uid_list=['uid-aaa'])
        assert fake_httpx.last_params['start_day'] == '2026-09-03'

    def test_time_of_day_is_dropped(self, loader, fake_httpx):
        # The endpoint works in day units, so the time of day is not sent.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyInfoV2(start_date='2026-09-03T18:30:00Z', end_date='2026-09-05',
                              uid_list=['uid-aaa'])
        assert fake_httpx.last_params['start_day'] == '2026-09-03'

    def test_default_range_is_the_last_seven_days_in_utc(self, loader, fake_httpx, frozen_now):
        # Without arguments the range is the seven utc days ending today.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyInfoV2(uid_list=['uid-aaa'])
        assert fake_httpx.last_params['end_day'] == '2026-09-04'
        assert fake_httpx.last_params['start_day'] == '2026-08-28'

    def test_start_after_end_raises(self, loader, fake_httpx):
        # An inverted range is a caller error.
        with pytest.raises(ValueError, match='must not be after'):
            loader.getDailyInfoV2(start_date='2026-09-05', end_date='2026-09-03',
                                  uid_list=['uid-aaa'])
        assert fake_httpx.calls == []

    def test_inverted_days_across_offsets_raise(self, loader, fake_httpx):
        # The check runs on the days actually sent, not on the instants.
        with pytest.raises(ValueError, match='must not be after'):
            loader.getDailyInfoV2(start_date='2026-09-03T00:00:00+09:00',
                                  end_date='2026-09-02T20:00:00+00:00', uid_list=['uid-aaa'])

    def test_same_day_range_is_allowed(self, loader, fake_httpx):
        # A single day range is valid.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyInfoV2(start_date='2026-09-03', end_date='2026-09-03', uid_list=['uid-aaa'])
        assert fake_httpx.last_params['start_day'] == fake_httpx.last_params['end_day']

    def test_invalid_date_raises(self, loader, fake_httpx):
        # An unreadable date raises before any request is made.
        with pytest.raises(ValueError):
            loader.getDailyInfoV2(start_date='abc', uid_list=['uid-aaa'])
        assert fake_httpx.calls == []

    def test_datetime_object_is_accepted(self, loader, fake_httpx):
        # A datetime object is a valid date argument.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyInfoV2(start_date=datetime.datetime(2026, 9, 3), end_date='2026-09-05',
                              uid_list=['uid-aaa'])
        assert fake_httpx.last_params['start_day'] == '2026-09-03'

    def test_no_uid_makes_no_request(self, loader, fake_httpx):
        # Without uids there is nothing to fetch.
        assert loader.getDailyInfoV2(start_date='2026-09-03', end_date='2026-09-05') is None
        assert fake_httpx.calls == []

    def test_one_request_per_uid(self, loader, fake_httpx):
        # The endpoint takes a single uid, so the uids are fetched one by one.
        fake_httpx.route('uid-aaa', FakeResponse([make_daily_info_row(uid='uid-aaa')]))
        fake_httpx.route('uid-bbb', FakeResponse([make_daily_info_row(uid='uid-bbb')]))
        df = loader.getDailyInfoV2(start_date='2026-09-03', end_date='2026-09-05',
                                   uid_list=['uid-aaa', 'uid-bbb'])
        assert len(fake_httpx.calls) == 2
        assert sorted(df['uid']) == ['uid-aaa', 'uid-bbb']

    def test_format_json_is_requested(self, loader, fake_httpx):
        # The json format is what the library parses.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyInfoV2(start_date='2026-09-03', uid_list=['uid-aaa'])
        assert fake_httpx.last_params['format'] == 'json'

    def test_convert_to_local_time_indexes_by_local_time(self, loader, fake_httpx):
        # The flag has to reach post_process_data and its result has to be returned.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        df = loader.getDailyInfoV2(start_date='2026-09-03', uid_list=['uid-aaa'],
                                   convert_to_local_time=True)
        assert df.index.name == 'local_time'

    def test_raw_time_column_is_kept_by_default(self, loader, fake_httpx):
        # Without the flag the response is returned untouched.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        df = loader.getDailyInfoV2(start_date='2026-09-03', uid_list=['uid-aaa'])
        assert '_time' in df.columns

    def test_response_columns_are_preserved(self, loader, fake_httpx):
        # Every column of the payload reaches the caller.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        df = loader.getDailyInfoV2(start_date='2026-09-03', uid_list=['uid-aaa'])
        assert set(make_daily_info_row().keys()) == set(df.columns)


class TestGetDailyDataV2:
    """DataLoader.getDailyDataV2."""

    def test_datetime_without_timezone_is_sent_as_utc(self, loader, fake_httpx):
        # A naive datetime is read as utc and sent with an explicit offset.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyDataV2('2026-09-03T00:00:00', '2026-09-05T00:00:00', uid_list=['uid-aaa'])
        assert fake_httpx.last_params['start_day'] == '2026-09-03T00:00:00+00:00'

    def test_datetime_with_timezone_keeps_its_offset(self, loader, fake_httpx):
        # A given offset is passed through to the api.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyDataV2('2026-09-03T00:00:00+09:00', '2026-09-05T00:00:00+09:00',
                              uid_list=['uid-aaa'])
        assert fake_httpx.last_params['start_day'] == '2026-09-03T00:00:00+09:00'
        assert fake_httpx.last_params['end_day'] == '2026-09-05T00:00:00+09:00'

    def test_sub_second_precision_is_dropped(self, loader, fake_httpx):
        # The api format is second precision.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyDataV2('2026-09-03T00:00:00.500+09:00', '2026-09-05T00:00:00+09:00',
                              uid_list=['uid-aaa'])
        assert fake_httpx.last_params['start_day'] == '2026-09-03T00:00:00+09:00'

    def test_space_separator_is_normalised(self, loader, fake_httpx):
        # A space separated datetime is sent in the iso format the api documents.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyDataV2('2026-09-03 00:00:00+09:00', '2026-09-05T00:00:00+09:00',
                              uid_list=['uid-aaa'])
        assert fake_httpx.last_params['start_day'] == '2026-09-03T00:00:00+09:00'

    def test_start_equal_to_end_raises(self, loader, fake_httpx):
        # The range has to be non empty.
        with pytest.raises(ValueError, match='must be before'):
            loader.getDailyDataV2('2026-09-03T00:00:00Z', '2026-09-03T00:00:00Z',
                                  uid_list=['uid-aaa'])

    def test_start_after_end_raises(self, loader, fake_httpx):
        # An inverted range is a caller error.
        with pytest.raises(ValueError, match='must be before'):
            loader.getDailyDataV2('2026-09-05T00:00:00Z', '2026-09-03T00:00:00Z',
                                  uid_list=['uid-aaa'])
        assert fake_httpx.calls == []

    def test_order_is_compared_across_offsets(self, loader, fake_httpx):
        # 00:00+09:00 is 15:00 utc of the day before, so this range is valid.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyDataV2('2026-09-03T00:00:00+09:00', '2026-09-02T20:00:00+00:00',
                              uid_list=['uid-aaa'])
        assert len(fake_httpx.calls) == 1

    def test_invalid_datetime_raises(self, loader, fake_httpx):
        # An unreadable datetime raises before any request is made.
        with pytest.raises(ValueError, match='start_datetime'):
            loader.getDailyDataV2('abc', '2026-09-05T00:00:00Z', uid_list=['uid-aaa'])
        assert fake_httpx.calls == []

    def test_no_uid_makes_no_request(self, loader, fake_httpx):
        # Without uids there is nothing to fetch.
        result = loader.getDailyDataV2('2026-09-03T00:00:00Z', '2026-09-05T00:00:00Z')
        assert result is None
        assert fake_httpx.calls == []

    def test_requests_the_daily_detail_endpoint(self, loader, fake_httpx):
        # The detail data comes from a different endpoint than the daily info data.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        loader.getDailyDataV2('2026-09-03T00:00:00Z', '2026-09-05T00:00:00Z', uid_list=['uid-aaa'])
        assert 'v2/DailyDetailData/uid-aaa' in fake_httpx.last_url

    def test_convert_to_local_time_indexes_by_local_time(self, loader, fake_httpx):
        # The flag behaves the same as on getDailyInfoV2.
        fake_httpx.route('*', FakeResponse([make_daily_info_row()]))
        df = loader.getDailyDataV2('2026-09-03T00:00:00Z', '2026-09-05T00:00:00Z',
                                   uid_list=['uid-aaa'], convert_to_local_time=True)
        assert df.index.name == 'local_time'


class TestFetchV2Data:
    """DataLoader.fetch_v2_data, the shared uid loop of the v2 methods."""

    def params(self):
        """
        Build the query parameters used by these tests.

        returns:
            - a params dict for a two day range
        """
        return {'start_day': '2026-09-03', 'end_day': '2026-09-05', 'format': 'json'}

    def test_rows_of_every_uid_are_combined(self, loader, fake_httpx):
        # The per uid responses end up in one DataFrame.
        fake_httpx.route('uid-aaa', FakeResponse(make_daily_info_rows(uid='uid-aaa')))
        fake_httpx.route('uid-bbb', FakeResponse(make_daily_info_rows(uid='uid-bbb')))
        df = loader.fetch_v2_data(loader.url, self.params(), ['uid-aaa', 'uid-bbb'], False, 60.0)
        assert len(df) == 4

    def test_a_failing_uid_does_not_discard_the_others(self, loader, fake_httpx):
        # One broken uid must not throw away the data already fetched.
        fake_httpx.route('uid-aaa', FakeResponse(make_daily_info_rows(uid='uid-aaa')))
        fake_httpx.route('uid-bbb', httpx.ConnectError('boom'))
        fake_httpx.route('uid-ccc', FakeResponse(make_daily_info_rows(uid='uid-ccc')))
        df = loader.fetch_v2_data(loader.url, self.params(),
                                  ['uid-aaa', 'uid-bbb', 'uid-ccc'], False, 60.0)
        assert sorted(set(df['uid'])) == ['uid-aaa', 'uid-ccc']

    def test_an_error_status_skips_only_that_uid(self, loader, fake_httpx):
        # A 401 on one uid leaves the other results intact.
        fake_httpx.route('uid-aaa', FakeResponse({}, status_code=401))
        fake_httpx.route('uid-bbb', FakeResponse(make_daily_info_rows(uid='uid-bbb')))
        df = loader.fetch_v2_data(loader.url, self.params(), ['uid-aaa', 'uid-bbb'], False, 60.0)
        assert set(df['uid']) == {'uid-bbb'}

    def test_a_dict_response_is_not_turned_into_rows(self, loader, fake_httpx):
        # Extending with an error dict would add its keys as rows.
        fake_httpx.route('uid-aaa', FakeResponse({'detail': 'Not Found'}))
        fake_httpx.route('uid-bbb', FakeResponse(make_daily_info_rows(uid='uid-bbb')))
        df = loader.fetch_v2_data(loader.url, self.params(), ['uid-aaa', 'uid-bbb'], False, 60.0)
        assert set(df['uid']) == {'uid-bbb'}

    def test_returns_none_when_every_uid_fails(self, loader, fake_httpx):
        # With nothing fetched there is no DataFrame to build.
        fake_httpx.route('*', httpx.ConnectError('boom'))
        assert loader.fetch_v2_data(loader.url, self.params(), ['uid-aaa'], False, 60.0) is None

    def test_returns_none_when_the_response_is_empty(self, loader, fake_httpx):
        # An empty result set is reported as None.
        fake_httpx.route('*', FakeResponse([]))
        assert loader.fetch_v2_data(loader.url, self.params(), ['uid-aaa'], False, 60.0) is None

    def test_failed_uids_are_reported(self, loader, fake_httpx, capsys):
        # The failed uids are printed so that the caller can retry them.
        fake_httpx.route('uid-aaa', httpx.ConnectError('boom'))
        fake_httpx.route('uid-bbb', FakeResponse(make_daily_info_rows(uid='uid-bbb')))
        loader.fetch_v2_data(loader.url, self.params(), ['uid-aaa', 'uid-bbb'], False, 60.0)
        assert 'uid-aaa' in capsys.readouterr().out

    def test_params_are_shared_by_every_request(self, loader, fake_httpx):
        # Every uid is fetched over the same range.
        fake_httpx.route('*', FakeResponse(make_daily_info_rows()))
        loader.fetch_v2_data(loader.url, self.params(), ['uid-aaa', 'uid-bbb'], False, 60.0)
        assert all(call['params'] == self.params() for call in fake_httpx.calls)

    def test_api_key_header_is_sent(self, loader, fake_httpx):
        # Authentication travels on every request.
        fake_httpx.route('*', FakeResponse(make_daily_info_rows()))
        loader.fetch_v2_data(loader.url, self.params(), ['uid-aaa'], False, 60.0)
        assert fake_httpx.calls[-1]['headers']['soxai-api-key'] == 'dummy-token'

    def test_convert_to_local_time_is_applied(self, loader, fake_httpx):
        # The conversion runs once on the combined DataFrame.
        fake_httpx.route('*', FakeResponse(make_daily_info_rows()))
        df = loader.fetch_v2_data(loader.url, self.params(), ['uid-aaa'], True, 60.0)
        assert df.index.name == 'local_time'
