import datetime
import pytest
import pandas as pd
from soxai_data.get_ave_data import DataProcessing, AverageDataExecutor


@pytest.fixture
def data_processing():
    return DataProcessing()


@pytest.fixture
def executor(tmp_path):
    return AverageDataExecutor(
        api_key="test-key",
        period_cnt=30,
        input_file=str(tmp_path / "input.csv"),
        output_file_path=str(tmp_path),
    )


class TestGetDateAfterIncludingTheDate:
    def test_plus_one_day(self, data_processing):
        start = datetime.datetime(2024, 1, 1)
        result = data_processing.get_date_after_including_the_date(start, 1)
        assert result == datetime.datetime(2024, 1, 1)

    def test_plus_seven_days(self, data_processing):
        start = datetime.datetime(2024, 1, 1)
        result = data_processing.get_date_after_including_the_date(start, 7)
        assert result == datetime.datetime(2024, 1, 7)

    def test_plus_thirty_days(self, data_processing):
        start = datetime.datetime(2024, 1, 1)
        result = data_processing.get_date_after_including_the_date(start, 30)
        assert result == datetime.datetime(2024, 1, 30)


class TestSortDfByTime:
    def test_sorts_ascending(self, data_processing):
        df = pd.DataFrame({
            "_time": ["2024-01-03", "2024-01-01", "2024-01-02"],
            "value": [3, 1, 2],
        })
        result = data_processing.sort_df_by_time(df)
        assert list(result["value"]) == [1, 2, 3]

    def test_converts_time_to_datetime(self, data_processing):
        df = pd.DataFrame({
            "_time": ["2024-01-01", "2024-01-02"],
            "value": [1, 2],
        })
        result = data_processing.sort_df_by_time(df)
        assert pd.api.types.is_datetime64_any_dtype(result["_time"])


class TestWithinTimeRange:
    def test_none_start_returns_true(self, executor):
        assert executor.within_time_range(None, datetime.time(23, 59)) is True

    def test_none_end_returns_true(self, executor):
        assert executor.within_time_range(datetime.time(0, 0), None) is True

    def test_both_none_returns_true(self, executor):
        assert executor.within_time_range(None, None) is True

    def test_current_time_within_range(self, executor):
        start = datetime.time(0, 0)
        end = datetime.time(23, 59)
        assert executor.within_time_range(start, end) is True

    def test_current_time_outside_range(self, executor):
        # A range in the past that has already passed (or will never match)
        start = datetime.time(0, 0)
        end = datetime.time(0, 0)
        # start == end, so the only valid time is exactly midnight
        current = datetime.datetime.now().time()
        expected = start <= current <= end
        assert executor.within_time_range(start, end) == expected


class TestGetTime:
    def test_valid_time_string(self, executor):
        result = executor.get_time("09:30")
        assert result == datetime.time(9, 30, 0)

    def test_midnight(self, executor):
        result = executor.get_time("00:00")
        assert result == datetime.time(0, 0, 0)

    def test_none_returns_none(self, executor):
        assert executor.get_time(None) is None

    def test_invalid_string_returns_none(self, executor):
        result = executor.get_time("not-a-time")
        assert result is None
