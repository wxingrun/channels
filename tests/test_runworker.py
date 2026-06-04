from io import StringIO
from unittest.mock import patch, Mock

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import override_settings

from channels.management.commands.runworker import Command
from channels.worker import Worker


@override_settings(
    CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}
)
def test_command_parses_channel_args():
    """
    Test that runworker command correctly parses channel arguments.
    """
    command = Command()

    with patch("argparse.ArgumentParser") as mock_parser:
        command.add_arguments(mock_parser)
        mock_parser.add_argument.assert_any_call(
            "--layer",
            action="store",
            dest="layer",
            default="default",
            help="Channel layer alias to use, if not the default.",
        )
        mock_parser.add_argument.assert_any_call(
            "channels", nargs="+", help="Channels to listen on."
        )


@override_settings(
    CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}
)
def test_command_initializes_worker():
    """
    Test that runworker command correctly initializes Worker.
    """
    test_channels = ["test.channel.1", "test.channel.2"]

    mock_application = Mock()
    mock_channel_layer = Mock()
    mock_worker = Mock()

    with patch("channels.management.commands.runworker.get_default_application", return_value=mock_application):
        with patch("channels.management.commands.runworker.get_channel_layer", return_value=mock_channel_layer):
            with patch.object(Command, "worker_class", return_value=mock_worker):
                call_command("runworker", *test_channels)

                Command.worker_class.assert_called_once_with(
                    application=mock_application,
                    channels=test_channels,
                    channel_layer=mock_channel_layer,
                )
                mock_worker.run.assert_called_once()


def test_command_raises_error_without_channel_layers():
    """
    Test that runworker command raises CommandError when no channel layers are configured.
    """
    with override_settings(CHANNEL_LAYERS={}):
        with pytest.raises(CommandError, match="You do not have any CHANNEL_LAYERS configured."):
            call_command("runworker", "test.channel")


@override_settings(
    CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}
)
def test_command_uses_specified_layer():
    """
    Test that runworker command uses specified channel layer.
    """
    test_channels = ["test.channel"]
    test_layer = "custom-layer"

    mock_application = Mock()
    mock_channel_layer = Mock()
    mock_worker = Mock()

    with patch("channels.management.commands.runworker.get_default_application", return_value=mock_application):
        with patch("channels.management.commands.runworker.get_channel_layer", return_value=mock_channel_layer) as mock_get_channel_layer:
            with patch.object(Command, "worker_class", return_value=mock_worker):
                call_command("runworker", *test_channels, layer=test_layer)

                mock_get_channel_layer.assert_called_once_with(test_layer)


@override_settings(
    CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}
)
def test_command_sets_verbosity():
    """
    Test that runworker command correctly sets verbosity.
    """
    test_channels = ["test.channel"]

    mock_application = Mock()
    mock_channel_layer = Mock()
    mock_worker = Mock()

    with patch("channels.management.commands.runworker.get_default_application", return_value=mock_application):
        with patch("channels.management.commands.runworker.get_channel_layer", return_value=mock_channel_layer):
            with patch.object(Command, "worker_class", return_value=mock_worker):
                out = StringIO()
                call_command("runworker", *test_channels, verbosity=2, stdout=out)
