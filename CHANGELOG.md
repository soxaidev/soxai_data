# Changelog

All notable changes to this project are documented here. This project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!--
How to add a release
--------------------
1. Add a new `## [x.y.z] - YYYY-MM-DD` section directly below this comment, so that the
   newest release is always on top. Add ` (planned)` after the date until it ships.
2. Use only these headings, in this order, and drop the ones you do not need:
   `### Breaking changes`, `### Added`, `### Changed`, `### Fixed`. Anything that makes a
   caller change their code, including a removal, goes in the two column table so that
   readers get the migration step next to the change, and is not repeated elsewhere.
3. Write for someone who calls this library: what breaks in their code, what changes in
   the data they get back, what they can now do. Leave out refactors, tests, tooling and
   anything that is not visible from the outside.
4. Set the version in `pyproject.toml` to the same number. The release workflow publishes
   exactly that version, so the two must match.
5. Add the compare link at the bottom of this file.

The release workflow copies the section of the version it publishes into the GitHub
release notes, and refuses to release a version that has no section here.
-->

## [0.1.0] - 2026-09-08 (planned)

The InfluxDB based endpoints are replaced by the v2 web api, every date argument now
follows one timezone rule, and several bugs that returned wrong or incomplete data are
fixed.

### Breaking changes

| Change | What to do |
| --- | --- |
| `getDailyData()` and `getDetailData()` are removed. | Use `getDailyInfoV2(start_date, end_date, uid_list=[...])` and `getDailyDataV2(start_datetime, end_datetime, uid_list=[...])`. `uid_list` is required, there is no implicit "my own data". |
| `add_uid_filter_to_flux_query()` is removed. | No replacement, it only built Flux queries. |
| The v2 endpoints do not return `_start`, `_stop`, `_measurement`, `year`, `month`, `year_week` or `workday`. | Derive what you need from `_time`, which carries an explicit offset. |
| `convert_to_local_time` was inserted before `timeout` on both v2 methods. A positional `timeout` now lands in the wrong parameter. | Pass it by keyword: `getDailyInfoV2(..., timeout=120.0)`. |
| `getMyInfo()` and `getMyOrgUsers()` raise `httpx.HTTPStatusError` on an error status instead of returning the error body. | Catch it if you inspected the error body before. An invalid token now fails loudly. |
| With `convert_to_local_time=True` the `local_time` index is timezone naive. It used to be labelled `+00:00` while holding local wall clock time. | Remove any `tz_convert()` on the index. The values themselves are unchanged. |
| `soxai_data.get_ave_data.InfluxDb` is renamed to `SoxaiWebApi`, and `initialize_dataloder()` to `initialize_dataloader()`. | Update the import. `AverageDataExecutor` is unchanged. |
| The `AverageDataExecutor` result csv no longer starts with an unnamed row number column. | Update readers that address columns by position. |
| `schedule` is now a required dependency. | `pip install -U soxai_data`. |

### Added

- **One timezone rule for every date argument.** A value without timezone information is
  read as UTC; a value that carries timezone information keeps its offset. Strings,
  `datetime.date`, `datetime.datetime` and `pandas.Timestamp` are all accepted, so you no
  longer have to format dates yourself.

  ```python
  sx_data.getDailyInfoV2(start_date='2026-09-03', uid_list=['uid1'])                 # UTC
  sx_data.getDailyInfoV2(start_date='2026-09-03T00:00:00+09:00', uid_list=['uid1'])  # JST
  sx_data.getDailyInfoV2(start_date=datetime.date(2026, 9, 3), uid_list=['uid1'])    # UTC
  ```

  `getDailyInfoV2()` works in day units, so an offset selects the calendar day as seen at
  that offset. `getDailyDataV2()` works with instants and always sends an explicit offset.
  A value that cannot be read as a date raises `ValueError` naming the argument.
- **`convert_to_local_time` on both v2 methods**, indexing the result by `local_time`.
- **One failing uid no longer discards the others.** The failed uids are reported and the
  rows of the remaining uids are returned.

### Changed

- `getDailyDataV2()` accepts a datetime without an offset and reads it as UTC. It used to
  reject it with `ValueError`.
- `getRawData()` defaults to the last seven days as unix timestamps. It used to send the
  Flux literals `-7d` and `now()`, which the endpoint does not understand.
- `uid_list` defaults to `None` instead of a shared empty list.

### Fixed

- `convert_to_local_time=True` had no effect on the v2 methods: `_time` was dropped
  without `local_time` becoming the index.
- `getMyOrgUsers(org_id='other-org')` ignored the argument and always queried your own
  organization, and gave up when your own organization was unknown.
- A single failing uid made the v2 methods return `None`, discarding the uids that had
  already succeeded.
- An api error response was expanded into rows, producing a nonsensical DataFrame instead
  of an error.
- `getRawData()` never returned data: it sent Flux literals as its range and decoded the
  payload twice. It now also reports http error statuses, not only transport failures.
- `getDailyInfoV2()` raised an opaque `TypeError` when given a `datetime` object.
- `post_process_data()` raised `KeyError` on v2 responses, which do not carry the columns
  it removed, and modified the DataFrame it was given.
- `AverageDataExecutor`:
  - could not fetch anything at all, because the api was called with `uid_list` and
    `convert_to_local_time` swapped and with a `datetime` where a date string was needed;
  - silently dropped every period after a period without data, so a user who stopped
    wearing the ring for a month lost all later averages;
  - treated the end of its daily time window as "all uids processed" and stopped the
    scheduler for good, instead of carrying the remaining uids over to the next run;
  - aborted every remaining uid when one uid could not be processed;
  - produced no output when the api returned timestamps without a timezone;
  - rejected a processing window that crosses midnight, such as `22:00` to `02:00`;
  - accepted a malformed `"hh:mm"` window by silently disabling the time restriction;
  - looped forever when `period_cnt` was below 1.
- Text fields such as `sleep_start_time_true` no longer appear as columns of `NaN` in the
  averaged output.

[0.1.0]: https://github.com/soxaidev/soxai_data/compare/main...feat/delete_querydata_endpoint
