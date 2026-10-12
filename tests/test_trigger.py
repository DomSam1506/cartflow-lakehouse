import pytest

from lambdas.trigger import handler as h


class FakeGlue:
    def __init__(self):
        self.calls = []

    def start_job_run(self, JobName, Arguments):
        self.calls.append((JobName, Arguments))
        return {"JobRunId": "jr_1"}


def event(*keys):
    return {"Records": [{"s3": {"object": {"key": k}}} for k in keys]}


@pytest.fixture
def glue(monkeypatch):
    fake = FakeGlue()
    monkeypatch.setattr(h, "_glue", lambda: fake)
    monkeypatch.setenv("JOB_NAME", "cartflow-clean-daily")
    return fake


def test_marker_starts_the_job_with_the_day(glue):
    assert h.handler(event("dt=2017-11-20/_READY"), None) == {"started": ["2017-11-20"]}
    assert glue.calls == [("cartflow-clean-daily", {"--dt": "2017-11-20"})]


def test_url_encoded_key_from_s3_is_decoded(glue):
    h.handler(event("dt%3D2017-11-24/_READY"), None)
    assert glue.calls[0][1] == {"--dt": "2017-11-24"}


def test_data_files_and_lookalikes_are_ignored(glue):
    h.handler(
        event(
            "dt=2017-11-20/orders/part-0.csv.gz",
            "raw/dt=2017-11-20/_READY",
            "dt=2017-11-20/_READY.tmp",
            "dt=17-11-20/_READY",
        ),
        None,
    )
    assert glue.calls == []
