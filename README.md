# soxai_data Python Package

This package provides a data loader for SOXAI ring users to access and analyze their data.

## Installation

Install the package using pip:

```bash
pip install soxai_data
```

## Usage

First, obtain your token by logging into the [SOXAI Platform](https://soxai-web-api-tiufu2wgva-df.a.run.app/). After logging in, generate your token and use it to load the data.

### Initialize DataLoader

```python
from soxai_data import DataLoader

# Initialize the DataLoader with your token
sx_data = DataLoader(token='your_token')
```

### Get Daily Data

You can retrieve daily data and plot it as follows:

```python
# Retrieve daily data
df = sx_data.getDailyData()
# Plot the data
df.plot()
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
# Merge daily data with organization user data
merged_df = df.merge(org_df, on='uid', how='left')
print(merged_df)
```

### Get Detail Data

To retrieve detailed data within a specified date range:

```python
# Retrieve detailed data
detail_df = sx_data.getDetailData(start_date='2023-01-01', end_date='2023-01-31')
print(detail_df)
```

### Get Daily Info Data (V2)

To retrieve daily info data for specific users using the V2 API:

```python
# Retrieve daily info data for specified uids
daily_info_df = sx_data.getDailyInfoV2(
    start_date='2026-01-01',
    end_date='2026-01-07',
    uid_list=['uid1', 'uid2']
)
print(daily_info_df)
```

### Get Daily Detail Data (V2)

To retrieve daily detail data for specific users with datetime-level precision using the V2 API.
The datetime must be in `YYYY-MM-DDThh:mm:ss+HH:MM` format with a timezone offset:

```python
# Retrieve daily detail data for specified uids within a datetime range
daily_detail_df = sx_data.getDailyDataV2(
    start_datetime='2026-01-20T00:00:00+09:00',
    end_datetime='2026-01-20T02:00:00+09:00',
    uid_list=['uid1', 'uid2']
)
print(daily_detail_df)
```

### Complete Example

Here's a complete example that includes retrieving and merging data:

```python
from soxai_data import DataLoader

# Initialize the DataLoader
sx_data = DataLoader(token='your_token')

# Get daily data
df = sx_data.getDailyData()

# Get account information
my_info = sx_data.getMyInfo()
my_org_id = my_info['myOrg']['orgId']

# Get organization users
org_df = sx_data.getMyOrgUsers(my_org_id)

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

### `DataLoader.getMyOrgUsers(org_id=None)`

Retrieves the users associated with the specified organization.

**Parameters:**  
- `org_id` (str, optional): The ID of the organization. If not provided, the method will use the default organization ID.

**Returns:**  
`pandas.DataFrame`: The DataFrame containing the users associated with the specified organization.

### `DataLoader.getDailyData(start_date=None, end_date=None, convert_to_local_time=True)`

Retrieves daily data from the SOXAI database within the specified date range.

**Parameters:**  
- `start_date` (str, optional): The start date of the data range. Defaults to '-7d'.
- `end_date` (str, optional): The end date of the data range. Defaults to 'now()'.
- `convert_to_local_time` (bool, optional): Whether to convert the time to local time. Defaults to True.

**Returns:**  
`pandas.DataFrame`: A DataFrame containing the retrieved data.

### `DataLoader.getDetailData(start_date=None, end_date=None, convert_to_local_time=True)`

Retrieves detailed data from the SOXAI database within the specified date range.

**Parameters:**  
- `start_date` (str, optional): The start date of the data range. Defaults to '-1d'.
- `end_date` (str, optional): The end date of the data range. Defaults to 'now()'.
- `convert_to_local_time` (bool, optional): Whether to convert the time to local time. Defaults to True.

**Returns:**  
`pandas.DataFrame`: A DataFrame containing the retrieved data.

### `DataLoader.getDailyInfoV2(start_date=None, end_date=None, uid_list=[], timeout=60.0)`

Retrieves daily info data from the SOXAI v2 API for the specified users and date range.

**Parameters:**
- `start_date` (str, optional): The start date in `YYYY-MM-DD` format. Defaults to 7 days before today.
- `end_date` (str, optional): The end date in `YYYY-MM-DD` format. Defaults to today.
- `uid_list` (list): List of uids to fetch data for.
- `timeout` (float, optional): Timeout in seconds. Defaults to 60.0.

**Returns:**
`pandas.DataFrame`: A DataFrame containing the retrieved data, or `None` if no data or an error occurred.

**Raises:**
`ValueError`: If the date format is invalid or `start_date` is after `end_date`.

### `DataLoader.getDailyDataV2(start_datetime, end_datetime, uid_list=[], timeout=60.0)`

Retrieves daily detail data from the SOXAI v2 API for the specified users and datetime range.
Unlike `getDailyInfoV2`, this method accepts datetime strings with time and timezone information, enabling hour-level data retrieval.

**Parameters:**
- `start_datetime` (str): The start datetime in `YYYY-MM-DDThh:mm:ss+HH:MM` format (timezone required). e.g. `2026-01-20T00:00:00+09:00`
- `end_datetime` (str): The end datetime in `YYYY-MM-DDThh:mm:ss+HH:MM` format (timezone required). e.g. `2026-01-20T02:00:00+09:00`
- `uid_list` (list): List of uids to fetch data for.
- `timeout` (float, optional): Timeout in seconds. Defaults to 60.0.

**Returns:**
`pandas.DataFrame`: A DataFrame containing the retrieved data, or `None` if no data or an error occurred.

**Raises:**
`ValueError`: If the datetime format is invalid, timezone is missing, or `start_datetime` is not before `end_datetime`.

## Additional Notes

- Ensure your token is valid and has not expired.
- Handle exceptions and errors gracefully while making API calls.
- Utilize Pandas' powerful data manipulation capabilities to analyze and visualize your data efficiently.

By following this guide, you should be able to effectively use the `soxai_data` package to retrieve, analyze, and visualize data from the SOXAI platform.