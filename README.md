# soxai_data python package

This package contains the data loader for SOXAI ring user.

## Installation

```bash
pip install soxai_data
```

## Usage

Get your token with your SOXAI account and use it to load the data.

go to [SOXAI_Platform](https://soxai-web-api-tiufu2wgva-df.a.run.app/) and login with your soxai account.
then generate your token and use it to load the data.

```python
from soxai_data import DataLoader

sx_data = DataLoader(token='your_token')
df = sx_data.getDailyData()
df.plot()
```