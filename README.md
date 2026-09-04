# soxai_data Python Package

This package provides a data loader for SOXAI ring users to access and analyze their data.

## Installation

Install the package using pip:

```bash
pip install soxai_data
```

## Usage

First, obtain your token by logging into the [SOXAI Platform](https://soxai-web-api-tiufu2wgva-df.a.run.app/). After logging in, generate your token and use it to load the data.

### Timezone Handling

Every date argument of the data retrieval methods follows the same rule:

- **Without timezone information** (e.g. `'2026-01-20'`, `'2026-01-20T10:00:00'`, a naive `datetime`) the value is interpreted as **UTC**.
- **With timezone information** (e.g. `'2026-01-20T10:00:00+09:00'`, `'2026-01-20T10:00:00Z'`, an aware `datetime`) the given **offset is followed**.

Strings, `datetime.date`, `datetime.datetime` and `pandas.Timestamp` are all accepted.
The same rule applies to the `_time` column when `convert_to_local_time=True`.
Default values are derived from the current UTC time, since there is no argument whose timezone could be followed.

### Initialize DataLoader

```python
from soxai_data import DataLoader

# Initialize the DataLoader with your token
sx_data = DataLoader(token='your_token')
```

### Get Daily Info Data (V2)

To retrieve daily info data for specific users and plot it:

```python
# Retrieve daily info data for specified uids
df = sx_data.getDailyInfoV2(
    start_date='2026-01-01',
    end_date='2026-01-07',
    uid_list=['uid1', 'uid2']
)
# Plot the data
df.plot()
```

`start_date` and `end_date` are optional. When omitted, the range defaults to the last 7 days
based on the current UTC date. This endpoint works in day units, so a value that carries a
timezone offset is reduced to the calendar day seen at that offset
(`'2026-01-20T23:00:00-05:00'` is sent as `2026-01-20`).

Pass `convert_to_local_time=True` to index the DataFrame by `local_time` (the wall clock time of
each user's timezone) instead of keeping the raw `_time` column:

```python
df = sx_data.getDailyInfoV2(uid_list=['uid1'], convert_to_local_time=True)
```

### Get Account Information

To get your account information:

```python
# Retrieve account information
my_info = sx_data.getMyInfo()
print(my_info)
```

### Get Organization Users

If you have an organization ID, you can get the users associated with it:

```python
# Retrieve organization users
my_org_id = my_info['myOrg']['orgId']
org_df = sx_data.getMyOrgUsers(my_org_id)
print(org_df)
```

### Merge DataFrames

You can merge the data with organization user information based on a common field:

```python
# Merge daily info data with organization user data
merged_df = df.merge(org_df, on='uid', how='left')
print(merged_df)
```

### Get Daily Detail Data (V2)

To retrieve daily detail data for specific users with datetime-level precision using the V2 API.
A datetime without a timezone offset is read as UTC, and the request always carries an explicit
offset:

```python
# Retrieve daily detail data for specified uids within a datetime range
daily_detail_df = sx_data.getDailyDataV2(
    start_datetime='2026-01-20T00:00:00+09:00',
    end_datetime='2026-01-20T02:00:00+09:00',
    uid_list=['uid1', 'uid2']
)
print(daily_detail_df)
```

### Get Raw Data

To retrieve the raw sensor data of a single user:

```python
# Retrieve raw data for one uid
raw_df = sx_data.getRawData('uid1', start_date='2026-01-20', end_date='2026-01-21')
print(raw_df)
```

### Complete Example

Here's a complete example that includes retrieving and merging data:

```python
from soxai_data import DataLoader

# Initialize the DataLoader
sx_data = DataLoader(token='your_token')

# Get account information
my_info = sx_data.getMyInfo()
my_org_id = my_info['myOrg']['orgId']

# Get organization users
org_df = sx_data.getMyOrgUsers(my_org_id)

# Get daily info data for those users
df = sx_data.getDailyInfoV2(uid_list=org_df['uid'].tolist())

# Merge data
merged_df = df.merge(org_df, on='uid', how='left')

# Display the merged DataFrame
print(merged_df)
```

## Methods

### `DataLoader.getMyInfo()`

Retrieves the account information.

**Returns:**  
`dict`: My personal information.

**Raises:**  
`httpx.HTTPStatusError`: If the API returns an error status.

### `DataLoader.getMyOrgUsers(org_id=None)`

Retrieves the users associated with the specified organization.

**Parameters:**  
- `org_id` (str, optional): The ID of the organization. If not provided, the ID of your own organization is looked up with `getMyInfo()`.

**Returns:**  
`pandas.DataFrame`: The DataFrame containing the users associated with the specified organization, or `None` if the organization could not be resolved.

**Raises:**  
`httpx.HTTPStatusError`: If the API returns an error status.

### `DataLoader.getRawData(uid, start_date=None, end_date=None, timeout=5.0)`

Retrieves the raw sensor data of one user within the specified date range.

**Parameters:**  
- `uid` (str): The uid to fetch data for.
- `start_date` (optional): The start of the data range. See [Timezone Handling](#timezone-handling). Defaults to 7 days before the current UTC time.
- `end_date` (optional): The end of the data range. Defaults to the current UTC time.
- `timeout` (float, optional): Timeout in seconds. Defaults to 5.0. (Up to 60.0)

**Raises:**  
`ValueError`: If a date argument cannot be read as a date or datetime.

**Returns:**  
`pandas.DataFrame`: A DataFrame containing the retrieved data, or `None` if the request failed.

### `DataLoader.getDailyInfoV2(start_date=None, end_date=None, uid_list=None, *, convert_to_local_time=False, timeout=60.0)`

Retrieves daily info data from the SOXAI v2 API for the specified users and date range.

**Parameters:**
- `start_date` (optional): The start date of the data range, e.g. `2026-01-20`. See [Timezone Handling](#timezone-handling). Defaults to 7 days before the current UTC date.
- `end_date` (optional): The end date of the data range. Defaults to the current UTC date.
- `uid_list` (list): List of uids to fetch data for.
- `convert_to_local_time` (bool, optional, keyword only): Whether to index the result by the `local_time` wall clock. Defaults to False.
- `timeout` (float, optional, keyword only): Timeout in seconds. Defaults to 60.0. (Up to 120.0)
  It is keyword only because it used to be the fourth positional argument; passing it positionally now raises `TypeError` instead of silently setting `convert_to_local_time`.

**Returns:**
`pandas.DataFrame`: A DataFrame containing the retrieved data, or `None` if no data could be fetched.
A uid whose request fails is reported and skipped, so the data of the remaining uids is still returned.

**Raises:**
`ValueError`: If a date argument cannot be read as a date or datetime, or `start_date` is after `end_date`.  
`TypeError`: If `convert_to_local_time` or `timeout` is passed positionally.

### `DataLoader.getDailyDataV2(start_datetime, end_datetime, uid_list=None, *, convert_to_local_time=False, timeout=60.0)`

Retrieves daily detail data from the SOXAI v2 API for the specified users and datetime range.
Unlike `getDailyInfoV2`, this method keeps the time of day, enabling hour-level data retrieval.

**Parameters:**
- `start_datetime`: The start of the datetime range, e.g. `2026-01-20T00:00:00+09:00`. See [Timezone Handling](#timezone-handling).
- `end_datetime`: The end of the datetime range, e.g. `2026-01-20T02:00:00+09:00`.
- `uid_list` (list): List of uids to fetch data for.
- `convert_to_local_time` (bool, optional, keyword only): Whether to index the result by the `local_time` wall clock. Defaults to False.
- `timeout` (float, optional, keyword only): Timeout in seconds. Defaults to 60.0. (Up to 120.0)
  It is keyword only because it used to be the fourth positional argument; passing it positionally now raises `TypeError` instead of silently setting `convert_to_local_time`.

**Returns:**
`pandas.DataFrame`: A DataFrame containing the retrieved data, or `None` if no data could be fetched.
A uid whose request fails is reported and skipped, so the data of the remaining uids is still returned.

**Raises:**
`ValueError`: If a datetime argument cannot be read as a date or datetime, or `start_datetime` is not before `end_datetime`.  
`TypeError`: If `convert_to_local_time` or `timeout` is passed positionally.

## Development

Install the test dependencies and run the offline test suite:

```bash
pip install -e ".[dev]"
pytest
```

`pytest` runs the unit tests only. Every external call is mocked, so the run needs no
network and no credentials.

The integration tests call the real api and are opt in:

```bash
pytest -m integration
```

They read `SOXAI_API_TOKEN` and `SOXAI_UID` from the environment or from a `.env` file in
the repository root, and skip themselves when those are missing or do not authenticate.
Never commit a token: `.env` is listed in `.gitignore`.

## Additional Notes

- Ensure your token is valid and has not expired.
- Keep your token out of source control. Read it from an environment variable or a secret store instead of hardcoding it.
- Handle exceptions and errors gracefully while making API calls.
- Utilize Pandas' powerful data manipulation capabilities to analyze and visualize your data efficiently.

By following this guide, you should be able to effectively use the `soxai_data` package to retrieve, analyze, and visualize data from the SOXAI platform.
