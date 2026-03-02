import pytest
from soxai_data.soxai_data import DataLoader


@pytest.fixture
def loader():
    return DataLoader(token="test-token")


class TestAddUidFilterToFluxQuery:
    def test_single_uid(self, loader):
        query = 'from(bucket: "SOXAI")\n|> range(start: -7d, stop: now() )\n|> filter(fn: (r) => r["_measurement"] == "SX_Daily_Prod")\n'
        result = loader.add_uid_filter_to_flux_query(query, ["uid1"])
        assert 'r["uid"] == "uid1"' in result

    def test_multiple_uids(self, loader):
        query = 'from(bucket: "SOXAI")\n|> range(start: -7d, stop: now() )\n|> filter(fn: (r) => r["_measurement"] == "SX_Daily_Prod")\n'
        result = loader.add_uid_filter_to_flux_query(query, ["uid1", "uid2"])
        assert 'r["uid"] == "uid1"' in result
        assert 'r["uid"] == "uid2"' in result
        assert ' or ' in result

    def test_filter_inserted_after_range(self, loader):
        query = 'from(bucket: "SOXAI")\n|> range(start: -7d, stop: now() )\n|> filter(fn: (r) => r["_measurement"] == "SX_Daily_Prod")\n'
        result = loader.add_uid_filter_to_flux_query(query, ["uid1"])
        range_idx = result.find('|> range(')
        filter_idx = result.find('|> filter(fn: (r) => r["uid"]')
        assert range_idx < filter_idx

    def test_no_range_statement_raises(self, loader):
        query = 'from(bucket: "SOXAI")\n|> filter(fn: (r) => r["_measurement"] == "SX_Daily_Prod")\n'
        with pytest.raises(Exception):
            loader.add_uid_filter_to_flux_query(query, ["uid1"])


class TestGetDailyInfoV2Validation:
    def test_invalid_start_date_format_raises(self, loader):
        with pytest.raises(ValueError, match="start_date"):
            loader.getDailyInfoV2(start_date="01-01-2024", uid_list=[])

    def test_invalid_end_date_format_raises(self, loader):
        with pytest.raises(ValueError, match="end_date"):
            loader.getDailyInfoV2(start_date="2024-01-01", end_date="2024/01/31", uid_list=[])

    def test_start_date_after_end_date_raises(self, loader):
        with pytest.raises(ValueError, match="must not be after"):
            loader.getDailyInfoV2(start_date="2024-12-31", end_date="2024-01-01", uid_list=[])

    def test_valid_dates_no_error(self, loader):
        # Should not raise for valid dates with empty uid_list (returns None due to no data)
        result = loader.getDailyInfoV2(start_date="2024-01-01", end_date="2024-01-31", uid_list=[])
        assert result is None


class TestGetDailyDataV2Validation:
    def test_invalid_start_datetime_format_raises(self, loader):
        with pytest.raises(ValueError, match="start_datetime"):
            loader.getDailyDataV2(start_datetime="2026-01-20", end_datetime="2026-01-20T02:00:00+09:00")

    def test_invalid_end_datetime_format_raises(self, loader):
        with pytest.raises(ValueError, match="end_datetime"):
            loader.getDailyDataV2(start_datetime="2026-01-20T00:00:00+09:00", end_datetime="2026-01-20")

    def test_start_datetime_equal_to_end_datetime_raises(self, loader):
        with pytest.raises(ValueError, match="must be before"):
            loader.getDailyDataV2(
                start_datetime="2026-01-20T00:00:00+09:00",
                end_datetime="2026-01-20T00:00:00+09:00",
            )

    def test_start_datetime_after_end_datetime_raises(self, loader):
        with pytest.raises(ValueError, match="must be before"):
            loader.getDailyDataV2(
                start_datetime="2026-01-20T02:00:00+09:00",
                end_datetime="2026-01-20T00:00:00+09:00",
            )

    def test_valid_datetimes_no_error(self, loader):
        # Should not raise for valid datetimes with empty uid_list (returns None due to no data)
        result = loader.getDailyDataV2(
            start_datetime="2026-01-20T00:00:00+09:00",
            end_datetime="2026-01-20T02:00:00+09:00",
            uid_list=[],
        )
        assert result is None
