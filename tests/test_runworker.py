from unittest import mock

import pytest
from django.core.management import CommandError, call_command

from channels.management.commands.runworker import Command


class TestRunworkerChannelArguments:
    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    def test_single_channel(self, mock_get_layer, mock_get_app):
        mock_get_layer.return_value = mock.MagicMock()
        mock_get_app.return_value = mock.MagicMock()
        mock_worker_instance = mock.MagicMock()

        with mock.patch.object(
            Command, "worker_class", return_value=mock_worker_instance
        ) as mock_wc:
            call_command("runworker", "test-channel")

            mock_wc.assert_called_once_with(
                application=mock_get_app.return_value,
                channels=["test-channel"],
                channel_layer=mock_get_layer.return_value,
            )
            mock_worker_instance.run.assert_called_once()

    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    def test_multiple_channels(self, mock_get_layer, mock_get_app):
        mock_get_layer.return_value = mock.MagicMock()
        mock_get_app.return_value = mock.MagicMock()
        mock_worker_instance = mock.MagicMock()

        with mock.patch.object(
            Command, "worker_class", return_value=mock_worker_instance
        ) as mock_wc:
            call_command("runworker", "ch1", "ch2", "ch3")

            mock_wc.assert_called_once_with(
                application=mock_get_app.return_value,
                channels=["ch1", "ch2", "ch3"],
                channel_layer=mock_get_layer.return_value,
            )
            mock_worker_instance.run.assert_called_once()

    def test_no_channels_raises_error(self):
        with pytest.raises((CommandError, SystemExit)):
            call_command("runworker")


class TestRunworkerLayerOption:
    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    def test_default_layer(self, mock_get_layer, mock_get_app):
        mock_get_layer.return_value = mock.MagicMock()
        mock_get_app.return_value = mock.MagicMock()
        mock_worker_instance = mock.MagicMock()

        with mock.patch.object(Command, "worker_class", return_value=mock_worker_instance):
            call_command("runworker", "ch1")

        mock_get_layer.assert_called_with("default")

    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    def test_custom_layer(self, mock_get_layer, mock_get_app):
        mock_get_layer.return_value = mock.MagicMock()
        mock_get_app.return_value = mock.MagicMock()
        mock_worker_instance = mock.MagicMock()

        with mock.patch.object(Command, "worker_class", return_value=mock_worker_instance):
            call_command("runworker", "ch1", layer="custom-layer")

        mock_get_layer.assert_called_with("custom-layer")

    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    def test_no_channel_layer_configured_raises_error(self, mock_get_layer):
        mock_get_layer.return_value = None

        with pytest.raises(
            CommandError, match="You do not have any CHANNEL_LAYERS configured"
        ):
            call_command("runworker", "ch1")


class TestRunworkerWorkerInit:
    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    def test_worker_initialized_with_correct_params(self, mock_get_layer, mock_get_app):
        mock_layer = mock.MagicMock()
        mock_app = mock.MagicMock()
        mock_get_layer.return_value = mock_layer
        mock_get_app.return_value = mock_app
        mock_worker_instance = mock.MagicMock()

        with mock.patch.object(
            Command, "worker_class", return_value=mock_worker_instance
        ) as mock_wc:
            call_command("runworker", "ch1", "ch2")

            mock_wc.assert_called_once_with(
                application=mock_app,
                channels=["ch1", "ch2"],
                channel_layer=mock_layer,
            )
            mock_worker_instance.run.assert_called_once()

    @mock.patch("channels.management.commands.runworker.get_default_application")
    @mock.patch("channels.management.commands.runworker.get_channel_layer")
    def test_worker_run_called(self, mock_get_layer, mock_get_app):
        mock_get_layer.return_value = mock.MagicMock()
        mock_get_app.return_value = mock.MagicMock()
        mock_worker_instance = mock.MagicMock()

        with mock.patch.object(Command, "worker_class", return_value=mock_worker_instance):
            call_command("runworker", "ch1")

        mock_worker_instance.run.assert_called_once()
