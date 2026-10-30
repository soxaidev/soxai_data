"""
Record the observable behaviour of soxai_data as JSON, so that two Python versions can
be compared with compare.py.

Every case calls the public api (or the helper that shapes its output) with fixed
inputs and records what comes out: the request the api would receive, the returned
DataFrame with its dtypes, the exception raised and what was printed. The http layer
is replaced with the fake of the test suite, so the offline cases need no network.

With credentials in the environment or a .env file (SOXAI_API_TOKEN and SOXAI_UID,
read the same way as the integration tests) the live cases fetch real data as well,
from SOXAI_API_URL when it is set.

usage:
    python scripts/compat/snapshot.py --out snapshot.json [--no-live]
"""

import argparse
import contextlib
import datetime
import io
import json
import pathlib
import platform
import sys
import tempfile
import types
from typing import Any, Callable, Dict, List
from unittest import mock

import httpx
import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'test'))

from conftest import (  # noqa: E402
    INTEGRATION_END_DATE,
    INTEGRATION_END_DATETIME,
    INTEGRATION_START_DATE,
    INTEGRATION_START_DATETIME,
    FakeHttpx,
    FakeResponse,
    get_api_url,
    get_credential,
    make_daily_info_rows,
    make_legacy_daily_info_row,
)
from soxai_data import DataLoader  # noqa: E402
from soxai_data import get_ave_data  # noqa: E402
from soxai_data.get_ave_data import AverageDataExecutor, DataProcessing, SoxaiWebApi  # noqa: E402

# the instant the time dependent cases treat as now
FROZEN_NOW = datetime.datetime(2026, 10, 15, 3, 0, 0, tzinfo=datetime.timezone.utc)


def to_jsonable(value: Any) -> Any:
    """
    Convert one cell or scalar into a JSON value that keeps the distinctions that matter.

    Missing values keep their kind (None, nan, NaT, pd.NA), since a change between them
    is itself a change of behaviour.

    args:
        - value : the value to convert
    returns:
        - a JSON serialisable value
    """
    if value is None:
        return '<None>'
    if value is pd.NaT:
        return '<NaT>'
    if value is pd.NA:
        return '<NA>'
    if isinstance(value, (float, np.floating)) and np.isnan(value):
        return '<nan>'
    if isinstance(value, (pd.Timestamp, datetime.datetime, datetime.date, datetime.time)):
        return {'type': type(value).__name__, 'iso': value.isoformat()}
    if isinstance(value, (pd.Timedelta, datetime.timedelta)):
        return {'type': 'timedelta', 'value': str(value)}
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    return {'type': type(value).__name__, 'repr': repr(value)}


def describe_frame(df: Any) -> Any:
    """
    Describe a DataFrame by its columns, dtypes, index and values.

    args:
        - df : the DataFrame, or any other return value
    returns:
        - the description, or the converted value when df is not a DataFrame
    """
    if not isinstance(df, pd.DataFrame):
        return to_jsonable(df)
    return {
        'columns': [str(c) for c in df.columns],
        'dtypes': {str(c): str(t) for c, t in df.dtypes.items()},
        'index_name': to_jsonable(df.index.name),
        'index_dtype': str(df.index.dtype),
        'index': [to_jsonable(v) for v in df.index],
        'values': [[to_jsonable(v) for v in row] for row in df.itertuples(index=False, name=None)],
    }


def run_case(func: Callable[[], Any]) -> Dict[str, Any]:
    """
    Run one case and record its result, its exception and what it printed.

    args:
        - func : the case, returning the value to record
    returns:
        - a dict of result or error, and stdout
    """
    stdout = io.StringIO()
    record: Dict[str, Any] = {}
    with contextlib.redirect_stdout(stdout):
        try:
            record['result'] = func()
        except Exception as e:
            record['error'] = {'type': type(e).__name__, 'message': str(e)}
    record['stdout'] = stdout.getvalue()
    return record


def call_with_fake(method_name: str, args: tuple, kwargs: dict, routes: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call a DataLoader method against the fake http layer.

    args:
        - method_name : the DataLoader method to call
        - args : positional arguments of the call
        - kwargs : keyword arguments of the call
        - routes : url key -> FakeResponse or Exception, as for FakeHttpx.route
    returns:
        - the returned value and the requests that were sent
    """
    fake = FakeHttpx()
    for key, answer in routes.items():
        fake.route(key, answer)
    loader = DataLoader(token='dummy-token')
    with mock.patch.object(httpx, 'get', fake.get):
        value = getattr(loader, method_name)(*args, **kwargs)
    requests = [
        {'url': c['url'], 'params': to_jsonable(c['params']), 'timeout': repr(c['timeout'])}
        for c in fake.calls
    ]
    return {'value': describe_frame(value), 'org_id': loader.org_id, 'requests': requests}


def request_cases() -> Dict[str, Callable[[], Any]]:
    """
    Build the cases that pin how arguments are turned into requests.

    returns:
        - case name -> case
    """
    jst = datetime.timezone(datetime.timedelta(hours=9))
    rows = make_daily_info_rows(day_offsets=(0,))
    info_args = {
        'str_dates': ('2026-09-03', '2026-09-05'),
        'offset_dates': ('2026-09-03T00:00:00+09:00', '2026-09-05T23:00:00-05:00'),
        'date_objects': (datetime.date(2026, 9, 3), datetime.date(2026, 9, 5)),
        'datetime_objects': (datetime.datetime(2026, 9, 3, 23, 0), datetime.datetime(2026, 9, 5, 1, 0, tzinfo=jst)),
        'timestamps': (pd.Timestamp('2026-09-03'), pd.Timestamp('2026-09-05T12:00:00Z')),
        'limit': ('2025-09-03', '2026-09-03'),
        'over_limit': ('2025-09-02', '2026-09-03'),
        'inverted': ('2026-09-05', '2026-09-03'),
        'invalid': ('abc', '2026-09-03'),
        'number': (5, '2026-09-03'),
        'empty': ('', '2026-09-03'),
    }
    data_args = {
        'str_dates': ('2026-09-03', '2026-09-05'),
        'same_date': ('2026-09-03', '2026-09-03'),
        'padded_date': (' 2026-09-03 ', '2026-09-03'),
        'naive_datetimes': ('2026-09-03T00:00:00', '2026-09-05 12:30:15.123'),
        'offset_datetimes': ('2026-09-03T00:00:00+09:00', '2026-09-05T00:00:00+09:00'),
        'z_suffix': ('2026-09-03T00:00:00Z', '2026-09-04T00:00:00Z'),
        'date_objects': (datetime.date(2026, 9, 3), datetime.date(2026, 9, 4)),
        'datetime_objects': (datetime.datetime(2026, 9, 3, tzinfo=jst), datetime.datetime(2026, 9, 4)),
        'timestamps': (pd.Timestamp('2026-09-03T00:00:00+09:00'), pd.Timestamp('2026-09-04')),
        'datetime_start_date_end': ('2026-09-03T12:00:00', '2026-09-03'),
        'datetime_start_after_date_end': ('2026-09-03T12:00:01Z', '2026-09-02'),
        'date_limit': ('2025-09-03', '2026-09-03'),
        'date_over_limit': ('2025-09-02', '2026-09-03'),
        'datetime_over_limit': ('2025-09-03T00:00:00Z', '2026-09-04T00:00:01Z'),
        'equal_datetimes': ('2026-09-03T00:00:00Z', '2026-09-03T00:00:00Z'),
        'invalid_date': ('2026-02-30', '2026-03-01'),
        'invalid_value': ('abc', '2026-03-01'),
    }
    raw_args = {
        'str_dates': ('2026-09-03', '2026-09-04'),
        'offset_dates': ('2026-09-03T00:00:00+09:00', '2026-09-04T00:00:00-05:00'),
        'datetime_objects': (datetime.datetime(2026, 9, 3), datetime.datetime(2026, 9, 4, tzinfo=jst)),
        'invalid': ('abc', '2026-09-04'),
    }
    cases: Dict[str, Callable[[], Any]] = {}
    for name, (start, end) in info_args.items():
        cases[f'getDailyInfoV2/{name}'] = (
            lambda s=start, e=end: call_with_fake(
                'getDailyInfoV2', (s, e, ['uid-aaa']), {}, {'*': FakeResponse(rows)}
            )
        )
    for name, (start, end) in data_args.items():
        cases[f'getDailyDataV2/{name}'] = (
            lambda s=start, e=end: call_with_fake(
                'getDailyDataV2', (s, e, ['uid-aaa']), {}, {'*': FakeResponse(rows)}
            )
        )
    for name, (start, end) in raw_args.items():
        cases[f'getRawData/{name}'] = (
            lambda s=start, e=end: call_with_fake(
                'getRawData', ('uid-aaa', s, e), {'timeout': 7.5}, {'*': FakeResponse([])}
            )
        )
    return cases


def response_cases() -> Dict[str, Callable[[], Any]]:
    """
    Build the cases that pin how responses are turned into return values.

    returns:
        - case name -> case
    """
    mixed_rows = (
        make_daily_info_rows(uid='uid-aaa', day_offsets=(0, 1, 2))
        + make_daily_info_rows(uid='uid-bbb', day_offsets=(0,), utc_offset_mins=-300)
    )
    # a row whose _time has no offset and one with a non utc offset
    mixed_rows[1]['_time'] = '2026-09-04T00:00:00'
    mixed_rows[2]['_time'] = '2026-09-05T09:00:00+09:00'
    legacy_rows = [make_legacy_daily_info_row(), make_legacy_daily_info_row(time='2026-09-04T00:00:00+00:00')]
    raw_rows = [{'_time': '2026-09-03T00:00:00Z', 'hr': 60, 'spo2': 98.5}, {'_time': '2026-09-03T00:01:00Z', 'hr': None, 'spo2': 97}]
    error = httpx.ConnectError('connection refused')
    status_500 = FakeResponse({'error': 'x'}, status_code=500)
    info = ('2026-09-03', '2026-09-05')

    def info_call(routes: Dict[str, Any], uids: List[str], convert: bool) -> Callable[[], Any]:
        return lambda: call_with_fake(
            'getDailyInfoV2', (*info, uids), {'convert_to_local_time': convert}, routes
        )

    return {
        'getDailyInfoV2/two_uids': info_call(
            {'uid-aaa': FakeResponse(mixed_rows[:3]), 'uid-bbb': FakeResponse(mixed_rows[3:])},
            ['uid-aaa', 'uid-bbb'], False),
        'getDailyInfoV2/two_uids_local_time': info_call(
            {'uid-aaa': FakeResponse(mixed_rows[:3]), 'uid-bbb': FakeResponse(mixed_rows[3:])},
            ['uid-aaa', 'uid-bbb'], True),
        'getDailyInfoV2/legacy_local_time': info_call({'*': FakeResponse(legacy_rows)}, ['uid-aaa'], True),
        'getDailyInfoV2/one_uid_fails': info_call(
            {'uid-aaa': error, 'uid-bbb': FakeResponse(mixed_rows[3:])}, ['uid-aaa', 'uid-bbb'], False),
        'getDailyInfoV2/dict_response': info_call({'*': FakeResponse({'error': 'x'})}, ['uid-aaa'], False),
        'getDailyInfoV2/every_uid_fails': info_call({'*': status_500}, ['uid-aaa', 'uid-bbb'], False),
        'getDailyInfoV2/empty_response': info_call({'*': FakeResponse([])}, ['uid-aaa'], False),
        'getDailyInfoV2/no_uid': info_call({}, [], False),
        'getDailyDataV2/local_time': lambda: call_with_fake(
            'getDailyDataV2', ('2026-09-03', '2026-09-05', ['uid-aaa']),
            {'convert_to_local_time': True}, {'*': FakeResponse(mixed_rows[:3])}),
        'getRawData/list_payload': lambda: call_with_fake(
            'getRawData', ('uid-aaa', '2026-09-03', '2026-09-04'), {}, {'*': FakeResponse(raw_rows)}),
        'getRawData/json_string_payload': lambda: call_with_fake(
            'getRawData', ('uid-aaa', '2026-09-03', '2026-09-04'), {},
            {'*': FakeResponse(json.dumps(raw_rows))}),
        'getRawData/http_error': lambda: call_with_fake(
            'getRawData', ('uid-aaa', '2026-09-03', '2026-09-04'), {}, {'*': status_500}),
        'getMyInfo/org_user': lambda: call_with_fake(
            'getMyInfo', (), {}, {'myOrg': FakeResponse({'isOrgUser': True, 'myOrg': {'orgId': 'org-1'}})}),
        'getMyInfo/http_error': lambda: call_with_fake(
            'getMyInfo', (), {}, {'myOrg': FakeResponse({}, status_code=401)}),
        'getMyOrgUsers/lookup': lambda: call_with_fake(
            'getMyOrgUsers', (), {}, {
                'myOrg': FakeResponse({'isOrgUser': True, 'myOrg': {'orgId': 'org-1'}}),
                'orgUsers': FakeResponse([{'uid': 'uid-aaa', 'name': 'a', 'age': 30}, {'uid': 'uid-bbb', 'name': None, 'age': None}]),
            }),
        'getMyOrgUsers/unresolved': lambda: call_with_fake(
            'getMyOrgUsers', (), {}, {'myOrg': FakeResponse({'isOrgUser': False})}),
    }


def processing_cases() -> Dict[str, Callable[[], Any]]:
    """
    Build the cases that pin the averaging of DataProcessing.

    returns:
        - case name -> case
    """
    def frame(**overrides: List[Any]) -> pd.DataFrame:
        df = pd.DataFrame(make_daily_info_rows(day_offsets=range(0, 70, 3)))
        for column, values in overrides.items():
            df[column] = (values * len(df))[:len(df)]
        return df

    current = FROZEN_NOW
    processing = DataProcessing()
    return {
        'get_average_datas/plain': lambda: describe_frame(processing.get_average_datas(frame(), current, 30)),
        'get_average_datas/numbers_as_strings': lambda: describe_frame(processing.get_average_datas(
            frame(health_hr_day_mean=['60', '62', 'junk', None]), current, 30)),
        'get_average_datas/nullable_ints': lambda: describe_frame(processing.get_average_datas(
            frame(sleep_score=[80, None, 81]), current, 7)),
        'get_average_datas/booleans_and_text': lambda: describe_frame(processing.get_average_datas(
            frame(flag=[True, False], note=['abc', None]), current, 30)),
        'get_average_datas/mixed_offsets': lambda: describe_frame(processing.get_average_datas(
            frame().assign(_time=lambda d: [t.replace('+00:00', '+09:00') if i % 2 else t for i, t in enumerate(d['_time'])]),
            current, 10)),
        'get_average_datas/naive_time': lambda: describe_frame(processing.get_average_datas(
            frame().assign(_time=lambda d: d['_time'].str.replace('+00:00', '', regex=False)),
            current.replace(tzinfo=None), 30)),
        'get_average_datas/legacy': lambda: describe_frame(processing.get_average_datas(
            pd.DataFrame([make_legacy_daily_info_row(), make_legacy_daily_info_row(time='2026-09-04T00:00:00+00:00')]),
            current, 30)),
        'get_average_datas/no_data_in_range': lambda: describe_frame(processing.get_average_datas(
            frame().iloc[:0], current, 30)),
    }


def executor_cases() -> Dict[str, Callable[[], Any]]:
    """
    Build the case that runs AverageDataExecutor.execute end to end with a fake api.

    Unlike the unit tests, CsvFile and DataProcessing are the real ones, so the csv
    text that is written (number and date formatting included) is recorded.

    returns:
        - case name -> case
    """
    responses = {
        'uid-aaa': pd.DataFrame(make_daily_info_rows(uid='uid-aaa', day_offsets=range(0, 40, 2))),
        'uid-bbb': pd.DataFrame(make_daily_info_rows(uid='uid-bbb', day_offsets=(0, 1), utc_offset_mins=-300)),
        'uid-empty': pd.DataFrame(),
        'uid-down': None,
    }

    class FrozenDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return FROZEN_NOW if tz is not None else FROZEN_NOW.replace(tzinfo=None)

    frozen_module = types.SimpleNamespace(
        datetime=FrozenDatetime, timedelta=datetime.timedelta,
        timezone=datetime.timezone, time=datetime.time,
    )

    def run() -> Dict[str, Any]:
        with tempfile.TemporaryDirectory() as tmp:
            input_file = pathlib.Path(tmp) / 'input.csv'
            pd.DataFrame({'UID list': list(responses)}).to_csv(input_file, index=False)
            requests = []

            def fake_fetch(self, **kwargs):
                requests.append(to_jsonable(kwargs))
                return responses[kwargs['uid_list'][0]]

            executor = AverageDataExecutor('dummy-token', 10, str(input_file), tmp)
            with mock.patch.object(get_ave_data, 'datetime', frozen_module), \
                    mock.patch.object(SoxaiWebApi, 'get_daily_data_by_uid', fake_fetch):
                executor.execute()
            files = {
                path.name.split('_', 1)[1]: path.read_text()
                for path in sorted(pathlib.Path(tmp).glob('*.csv')) if path != input_file
            }
            return {
                'requests': requests,
                'files': files,
                'task_executed': executor.task_executed,
                'fruitless_run_cnt': executor.fruitless_run_cnt,
                'input_file_suffix': pathlib.Path(executor.input_file).name.split('_', 1)[-1],
            }

    return {'AverageDataExecutor.execute/offline': run}


def live_cases(token: str, uid: str) -> Dict[str, Callable[[], Any]]:
    """
    Build the cases that fetch real data from the SOXAI web api.

    args:
        - token : the api token
        - uid : the uid to fetch the data of
    returns:
        - case name -> case
    """
    loader = DataLoader(token=token)
    # the same SOXAI_API_URL override as the integration tests
    loader.url = get_api_url() or loader.url
    start_raw = pd.Timestamp(INTEGRATION_START_DATE, tz='UTC')
    return {
        'live/getMyInfo_keys': lambda: sorted(loader.getMyInfo().keys()),
        'live/getDailyInfoV2': lambda: describe_frame(loader.getDailyInfoV2(
            INTEGRATION_START_DATE, INTEGRATION_END_DATE, [uid])),
        'live/getDailyInfoV2_local_time': lambda: describe_frame(loader.getDailyInfoV2(
            INTEGRATION_START_DATE, INTEGRATION_END_DATE, [uid], convert_to_local_time=True)),
        'live/getDailyDataV2': lambda: describe_frame(loader.getDailyDataV2(
            INTEGRATION_START_DATETIME, INTEGRATION_END_DATETIME, [uid])),
        'live/getRawData': lambda: describe_frame(loader.getRawData(
            uid, start_raw, start_raw + pd.Timedelta(hours=1), timeout=60.0)),
        'live/get_average_datas': lambda: describe_frame(DataProcessing().get_average_datas(
            loader.getDailyInfoV2(INTEGRATION_START_DATE, INTEGRATION_END_DATE, [uid]), FROZEN_NOW, 1)),
    }


def main() -> int:
    """
    Run every case and write the snapshot.

    returns:
        - the process exit code
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument('--out', required=True, help='path of the snapshot json to write')
    parser.add_argument('--no-live', action='store_true', help='skip the cases that call the real api')
    options = parser.parse_args()

    cases: Dict[str, Callable[[], Any]] = {}
    cases.update(request_cases())
    cases.update(response_cases())
    cases.update(processing_cases())
    cases.update(executor_cases())

    token = get_credential('SOXAI_API_TOKEN')
    uid = get_credential('SOXAI_UID')
    live = not options.no_live and bool(token) and bool(uid)
    if live:
        cases.update(live_cases(token, uid))

    snapshot = {
        'meta': {
            'python': platform.python_version(),
            'pandas': pd.__version__,
            'numpy': np.__version__,
            'httpx': httpx.__version__,
            'live': live,
            'api_url': (get_api_url() or DataLoader(token='').url) if live else None,
        },
        'cases': {name: run_case(case) for name, case in cases.items()},
    }
    pathlib.Path(options.out).write_text(json.dumps(snapshot, indent=1, ensure_ascii=False, sort_keys=True))
    print(f'wrote {len(cases)} cases to {options.out} (live: {live})')
    return 0


if __name__ == '__main__':
    sys.exit(main())
