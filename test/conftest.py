"""Shared fixtures for the soxai_data test suite."""

import datetime
import pathlib
from typing import Optional

import pandas as pd
import pytest

# Structural columns of the v2 DailyInfoData response, confirmed against the real api.
# Every other column is a measurement field. test_integration.py asserts that a real
# response still carries all of them, so the unit tests stay faithful to the api.
DAILY_INFO_COLUMNS = [
    '_time',
    'uid',
    'utc_offset_mins',
]

# Columns the influx based v1 api used to return. The v2 api does not, but the library
# still drops them defensively, so one fixture keeps them to cover that path.
LEGACY_DAILY_INFO_COLUMNS = [
    '_start',
    '_stop',
    '_measurement',
    'year',
    'month',
    'year_week',
    'workday',
]


def make_daily_info_row(uid='uid-aaa', time='2026-09-03T00:00:00+00:00', utc_offset_mins=540,
                        sleep_score=80, health_hr_day_mean=60):
    """
    Build one v2 DailyInfoData row in the shape the real api returns.

    Besides the numeric fields the api also returns fields that hold no number:
    ML_ver is always None and sleep_start_time_true is a timestamp string. Both have to
    survive get_average_data without becoming a column of NaN.

    args:
        - uid : value of the uid column
        - time : value of the _time column
        - utc_offset_mins : offset of the user's timezone in minutes
        - sleep_score : an integer measurement field
        - health_hr_day_mean : another integer measurement field
    returns:
        - the row as a dict
    """
    return {
        '_time': time,
        'uid': uid,
        'utc_offset_mins': utc_offset_mins,
        'ML_ver': None,
        'fw_ver': 120,
        'sleep_score': sleep_score,
        'sleep_efficiency': 92.5,
        'health_hr_day_mean': health_hr_day_mean,
        'sleep_start_time_true': '2026-09-02T23:10:00+09:00',
    }


def make_legacy_daily_info_row(**kwargs):
    """
    Build a DailyInfoData row that also carries the columns of the v1 api.

    args:
        - kwargs : passed to make_daily_info_row
    returns:
        - the row as a dict
    """
    row = make_daily_info_row(**kwargs)
    parsed = pd.Timestamp(row['_time'])
    row.update({
        '_start': '2026-09-01T00:00:00+00:00',
        '_stop': '2026-09-30T00:00:00+00:00',
        '_measurement': 'daily_info',
        'year': parsed.year,
        'month': parsed.month,
        'year_week': f'{parsed.year}-{parsed.week:02d}',
        'workday': True,
    })
    return row


def make_daily_info_rows(uid='uid-aaa', day_offsets=(0, 1), start='2026-09-03', **kwargs):
    """
    Build one v2 DailyInfoData row per day offset.

    args:
        - uid : value of the uid column
        - day_offsets : day offsets from start, one row each
        - start : the day offset 0 refers to
        - kwargs : passed to make_daily_info_row
    returns:
        - the rows as a list of dicts
    """
    rows = []
    for offset in day_offsets:
        day = pd.Timestamp(start) + pd.Timedelta(days=offset)
        rows.append(make_daily_info_row(
            uid=uid,
            time=day.strftime('%Y-%m-%dT00:00:00+00:00'),
            sleep_score=80 + offset,
            **kwargs,
        ))
    return rows


class FakeResponse:
    """Stand in for httpx.Response with a fixed payload and status."""

    def __init__(self, payload=None, status_code=200, json_error=None):
        """
        Set up the response.

        args:
            - payload : value returned by json()
            - status_code : http status used by raise_for_status
            - json_error : exception raised by json() instead of returning payload
        """
        self.payload = payload
        self.status_code = status_code
        self.json_error = json_error
        self.text = str(payload)

    def raise_for_status(self):
        """
        Raise when the status is an error, the way httpx does.

        raises:
            - httpx.HTTPStatusError : if status_code is 400 or above
        """
        import httpx

        if self.status_code >= 400:
            request = httpx.Request('GET', 'https://web-api.example/api/')
            raise httpx.HTTPStatusError(
                f'{self.status_code} error',
                request=request,
                response=httpx.Response(self.status_code, request=request),
            )

    def json(self):
        """
        Return the payload.

        returns:
            - the payload given to the constructor
        raises:
            - Exception : the json_error given to the constructor, if any
        """
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class FakeHttpx:
    """Record the calls made to httpx.get and answer them from a route table."""

    def __init__(self):
        """Start with an empty route table and no recorded calls."""
        self.calls = []
        # last path segment of the url -> FakeResponse or Exception. '*' is the fallback
        self.routes = {}

    def route(self, key, response):
        """
        Register the answer for one url key.

        args:
            - key : last path segment of the url, or '*' for the fallback
            - response : FakeResponse to return, or Exception to raise
        """
        self.routes[key] = response

    def get(self, url, headers=None, params=None, timeout=None):
        """
        Answer a request from the route table and record it.

        args:
            - url : the requested url
            - headers : the request headers
            - params : the query parameters
            - timeout : the request timeout
        returns:
            - the FakeResponse registered for the url
        raises:
            - Exception : if an exception was registered for the url
        """
        self.calls.append({'url': url, 'headers': headers, 'params': params, 'timeout': timeout})
        key = url.rstrip('/').split('/')[-1].split('?')[0]
        answer = self.routes.get(key, self.routes.get('*', FakeResponse([])))
        if isinstance(answer, Exception):
            raise answer
        return answer

    @property
    def last_params(self):
        """
        Return the query parameters of the last call.

        returns:
            - the params dict of the last recorded call
        """
        return self.calls[-1]['params']

    @property
    def last_url(self):
        """
        Return the url of the last call.

        returns:
            - the url of the last recorded call
        """
        return self.calls[-1]['url']


@pytest.fixture
def fake_httpx(monkeypatch):
    """
    Replace httpx.get inside soxai_data.soxai_data with a recording fake.

    returns:
        - the FakeHttpx instance the module now calls
    """
    from soxai_data import soxai_data as module

    fake = FakeHttpx()
    monkeypatch.setattr(module.httpx, 'get', fake.get)
    return fake


@pytest.fixture
def loader():
    """
    Build a DataLoader with a dummy token.

    returns:
        - the DataLoader under test
    """
    from soxai_data import DataLoader

    return DataLoader(token='dummy-token')


@pytest.fixture
def frozen_now(monkeypatch):
    """
    Freeze pandas.Timestamp.now so that the default date ranges are predictable.

    returns:
        - the utc Timestamp that now() returns
    """
    fixed = pd.Timestamp('2026-09-04T12:34:56+00:00')

    class FrozenTimestamp(pd.Timestamp):
        """A Timestamp whose now() is fixed, so default date ranges are predictable."""

        @classmethod
        def now(cls, tz=None):
            """
            Return the frozen instant.

            args:
                - tz : timezone of the returned Timestamp, or None for a naive one
            returns:
                - the frozen Timestamp in the requested timezone
            """
            return fixed.tz_convert(tz) if tz is not None else fixed.tz_localize(None)

    from soxai_data import soxai_data as module

    monkeypatch.setattr(module.pd, 'Timestamp', FrozenTimestamp)
    return fixed


def read_env_file() -> dict:
    """
    Read the .env files that may hold the integration test credentials.

    returns:
        - the variables found, as a dict. Missing files yield an empty dict
    """
    values = {}
    root = pathlib.Path(__file__).resolve().parent.parent
    for path in (root / '.env', root / 'soxai_data' / '.env'):
        if not path.is_file():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            values.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return values


def get_credential(name: str) -> Optional[str]:
    """
    Read one integration test credential from the environment or a .env file.

    args:
        - name : the variable name, e.g. 'SOXAI_API_TOKEN'
    returns:
        - the value, or None when it is not set anywhere
    """
    import os

    return os.environ.get(name) or read_env_file().get(name)


# the datetime range the integration tests know holds data
INTEGRATION_START_DATETIME = '2026-09-03T00:00:00+09:00'
INTEGRATION_END_DATETIME = '2026-09-05T00:00:00+09:00'
INTEGRATION_START_DATE = '2026-09-03'
INTEGRATION_END_DATE = '2026-09-05'


@pytest.fixture(scope='session')
def api_token():
    """
    Return the api token for the integration tests, skipping them when it is absent.

    returns:
        - the SOXAI api token
    """
    token = get_credential('SOXAI_API_TOKEN')
    if not token:
        pytest.skip('SOXAI_API_TOKEN is not set, skipping the integration tests')
    return token


@pytest.fixture(scope='session')
def api_uid():
    """
    Return the uid for the integration tests, skipping them when it is absent.

    returns:
        - the uid to fetch data for
    """
    uid = get_credential('SOXAI_UID')
    if not uid:
        pytest.skip('SOXAI_UID is not set, skipping the integration tests')
    return uid


@pytest.fixture
def uid_csv(tmp_path):
    """
    Write an input csv holding two uids for AverageDataExecutor.

    returns:
        - the path of the csv file
    """
    path = tmp_path / 'input_uid.csv'
    pd.DataFrame({'UID list': ['uid-aaa', 'uid-bbb']}).to_csv(path, index=False)
    return path


@pytest.fixture
def jst():
    """
    Return the JST timezone.

    returns:
        - a datetime.timezone of +09:00
    """
    return datetime.timezone(datetime.timedelta(hours=9))
