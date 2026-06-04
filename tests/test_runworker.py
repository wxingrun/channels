import logging
from argparse import ArgumentParser
from unittest import mock

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from channels.management.commands.runworker import Command
from channels.worker import Worker


class TestRunworkerArgumentParsing:
    """Tests for runworker command argument parsing."""

    def test_add_arguments_registers_channels_argument(self):
        cmd = Command()
        parser = ArgumentParser()
        cmd.add_arguments(parser)

        actions = {action.dest: action for action in parser._actions}
        assert "channels" in actions
        assert actions["channels"].nargs == "+"

    def test_add_arguments_registers_layer_option(self):
        cmd = Command()
        parser = ArgumentParser()
        cmd.add_arguments(parser)

        actions = {action.dest: action for action in parser._actions}
        assert "layer" in actions
        assert actions["layer"].default == "default"
        assert actions["layer"].option_strings == ["--layer"]


class TestRunworkerCommandExecution:
    """Tests for runworker command execution with mocked dependencies."""

    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch.object(Worker, "run")
    def test_command_passes_channel_args_to_worker(self, mock_run, mock_get_app, mock_get_layer):
        """
        Test that the command correctly parses channel arguments and passes
        them to the Worker constructor.
        """
        mock_layer = mock.Mock()
        mock_get_layer.return_value = mock_layer
        mock_app = mock.Mock()
        mock_get_app.return_value = mock_app

        call_command("runworker", "chan_one", "chan_two", verbosity=0)

        mock_get_layer.assert_called_once_with("default")
        mock_get_app.assert_called_once()
        mock_run.assert_called_once()

        assert len(mock_get_app.call_args_list) == 1

    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch.object(Worker, "run")
    def test_command_uses_custom_layer_alias(self, mock_run, mock_get_app, mock_get_layer):
        """
        Test that the --layer option is correctly forwarded to get_channel_layer.
        """
        mock_get_layer.return_value = mock.Mock()
        mock_get_app.return_value = mock.Mock()

        call_command("runworker", "my_channel", "--layer", "custom_backend", verbosity=0)

        mock_get_layer.assert_called_once_with("custom_backend")

    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch.object(Worker, "run")
    def test_command_single_channel(self, mock_run, mock_get_app, mock_get_layer):
        """
        Test that a single channel argument is handled correctly.
        """
        mock_get_layer.return_value = mock.Mock()
        mock_get_app.return_value = mock.Mock()

        call_command("runworker", "solo_channel", verbosity=0)

        assert mock_get_app.call_count == 1

    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    def test_command_raises_error_when_no_channel_layer_configured(self, mock_get_layer):
        """
        Test that the command raises CommandError when no channel layer
        is configured (get_channel_layer returns None).
        """
        mock_get_layer.return_value = None

        with pytest.raises(CommandError, match="You do not have any CHANNEL_LAYERS configured"):
            call_command("runworker", "test_channel", verbosity=0)

    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch.object(Worker, "run")
    def test_command_uses_default_layer_when_not_specified(self, mock_run, mock_get_app, mock_get_layer):
        """
        Test that the command uses the default layer alias when --layer is not provided.
        """
        mock_get_layer.return_value = mock.Mock()
        mock_get_app.return_value = mock.Mock()

        call_command("runworker", "test_channel", verbosity=0)

        mock_get_layer.assert_called_once_with("default")

    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch.object(Worker, "run")
    def test_command_logs_channel_info(self, mock_run, mock_get_app, mock_get_layer):
        """
        Test that the command logs the channels it is running for.
        """
        mock_get_layer.return_value = mock.Mock()
        mock_get_app.return_value = mock.Mock()

        with mock.patch.object(logging.getLogger("django.channels.worker"), "info") as mock_log:
            call_command("runworker", "ch1", "ch2", verbosity=0)
            mock_log.assert_called_once_with(
                "Running worker for channels %s", ["ch1", "ch2"]
            )