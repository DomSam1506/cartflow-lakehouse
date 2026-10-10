import json
import os


class LocalWatermarks:
    def __init__(self, path="data/local_watermarks.json"):
        self.path = path

    def _load(self):
        if not os.path.exists(self.path):
            return {}
        with open(self.path) as f:
            return json.load(f)

    def get(self, table):
        return self._load().get(table)

    def put(self, table, value):
        d = self._load()
        d[table] = value
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(d, f, indent=2, sort_keys=True)
        os.replace(tmp, self.path)  # atomic: a crash never leaves a half-written file


class DynamoWatermarks:
    """Day 4: same interface, backed by the cartflow-watermarks table."""

    def __init__(self, table="cartflow-watermarks"):
        import boto3
        self.t = boto3.resource("dynamodb").Table(table)

    def get(self, table):
        return self.t.get_item(Key={"table_name": table}).get("Item", {}).get("value")

    def put(self, table, value):
        self.t.put_item(Item={"table_name": table, "value": value})


def advance(store, table, dt):
    """Move the watermark forward only. Re-running an older day never moves it back."""
    cur = store.get(table)
    if cur is None or dt > cur:  # ISO dates compare correctly as strings
        store.put(table, dt)
        return True
    return False