
from soxai_data import DataLoader

sx_data = DataLoader(token='9WscrAcZLinH2NVj7DrzXoA-OXmbtdy9yDXpLKt0NEs')
df = sx_data.getDailyData()
df.plot()