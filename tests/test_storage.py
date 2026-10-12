import pytest

from glue_jobs.common import storage


class FakeS3:
    """Minimal S3 stand-in with small pages, so pagination is exercised."""

    def __init__(self, keys, page=2):
        self.keys, self.page = sorted(keys), page

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000, ContinuationToken=None):
        ks = [k for k in self.keys if k.startswith(Prefix)]
        if ContinuationToken:
            ks = [k for k in ks if k > ContinuationToken]
        limit = min(MaxKeys, self.page)
        chunk, more = ks[:limit], len(ks) > limit
        resp = {"KeyCount": len(chunk), "Contents": [{"Key": k} for k in chunk],
                "IsTruncated": more}
        if more:
            resp["NextContinuationToken"] = chunk[-1]
        return resp

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.keys.remove(o["Key"])


@pytest.fixture
def s3(monkeypatch):
    keys = [f"clean/events/ingest_dt=2017-11-24/part-{i}.parquet" for i in range(5)]
    keys += ["clean/events/ingest_dt=2017-11-25/part-0.parquet",
             "clean/events/ingest_dt=2017-11-240/part-0.parquet"]
    fake = FakeS3(keys)
    monkeypatch.setattr(storage, "_client", lambda: fake)
    return fake


def test_join_handles_slashes():
    assert storage.join("s3://b/", "/raw/", "dt=1") == "s3://b/raw/dt=1"
    assert storage.join("data/lake", "clean") == "data/lake/clean"


def test_exists_dir_s3_matches_whole_folder_names_only(s3):
    assert storage.exists_dir("s3://b/clean/events/ingest_dt=2017-11-24")
    assert not storage.exists_dir("s3://b/clean/events/ingest_dt=2017-11-2")
    assert not storage.exists_dir("s3://b/clean/orders")


def test_delete_prefix_s3_pages_through_and_spares_neighbours(s3):
    storage.delete_prefix("s3://b/clean/events/ingest_dt=2017-11-24/")
    assert s3.keys == ["clean/events/ingest_dt=2017-11-240/part-0.parquet",
                       "clean/events/ingest_dt=2017-11-25/part-0.parquet"]


def test_delete_prefix_refuses_whole_bucket(s3):
    with pytest.raises(ValueError):
        storage.delete_prefix("s3://b/")


def test_local_exists_and_delete(tmp_path):
    d = tmp_path / "ingest_dt=2017-11-24"
    d.mkdir()
    (d / "f").write_text("x")
    assert storage.exists_dir(str(d))
    storage.delete_prefix(str(d))
    assert not storage.exists_dir(str(d))


def test_read_text_local(tmp_path):
    p = tmp_path / "a.txt"
    p.write_text("hello")
    assert storage.read_text(str(p)) == "hello"