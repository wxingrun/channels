from django.core.management import call_command

from channels.management.commands import runworker as runworker_command


class FakeWorker:
    instances = []

    def __init__(self, *, application, channels, channel_layer):
        self.application = application
        self.channels = channels
        self.channel_layer = channel_layer
        self.run_called = False
        self.__class__.instances.append(self)

    def run(self):
        self.run_called = True


def test_runworker_parses_channel_arguments(monkeypatch):
    fake_application = object()
    fake_channel_layer = object()
    FakeWorker.instances = []

    monkeypatch.setattr(runworker_command.Command, "worker_class", FakeWorker)
    monkeypatch.setattr(
        runworker_command,
        "get_default_application",
        lambda: fake_application,
    )
    monkeypatch.setattr(
        runworker_command,
        "get_channel_layer",
        lambda alias=None: fake_channel_layer,
    )

    call_command("runworker", "alpha", "beta", verbosity=0)

    assert len(FakeWorker.instances) == 1
    worker = FakeWorker.instances[0]
    assert worker.channels == ["alpha", "beta"]
    assert worker.application is fake_application
    assert worker.channel_layer is fake_channel_layer


def test_runworker_initializes_worker_and_starts(monkeypatch):
    fake_application = object()
    fake_channel_layer = object()
    requested_layers = []
    FakeWorker.instances = []

    monkeypatch.setattr(runworker_command.Command, "worker_class", FakeWorker)
    monkeypatch.setattr(
        runworker_command,
        "get_default_application",
        lambda: fake_application,
    )

    def get_channel_layer(alias=None):
        requested_layers.append(alias)
        return fake_channel_layer

    monkeypatch.setattr(runworker_command, "get_channel_layer", get_channel_layer)

    call_command("runworker", "emails", layer="background", verbosity=0)

    assert requested_layers == ["background"]
    assert len(FakeWorker.instances) == 1
    worker = FakeWorker.instances[0]
    assert worker.channels == ["emails"]
    assert worker.application is fake_application
    assert worker.channel_layer is fake_channel_layer
    assert worker.run_called is True
