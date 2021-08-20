from datetime import datetime, timedelta
import json
today = datetime.now()
start_date = today - timedelta(days=5)
docs = [{"date": start_date + timedelta(days=i), "val": i} for i in range(10)]

with open('test.json', 'w') as f:
    f.write(json.dumps(docs))
