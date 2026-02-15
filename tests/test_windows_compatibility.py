"""Test Windows compatibility fixes for pytest-postgresql."""

import subprocess
from unittest.mock import MagicMock, patch

from pytest_postgresql.executor import PostgreSQLExecutor


class TestCommandTemplates:
    """Test platform-specific command templates."""

    def test_unix_command_template_has_single_quotes(self) -> None:
        """Test that Unix template uses single quotes for PostgreSQL config values.

        Single quotes are PostgreSQL config-level quoting that protects paths
        with spaces in unix_socket_directories. On Unix, mirakuru uses
        shlex.split() which properly handles single quotes inside double-quoted strings.
        """
        template = PostgreSQLExecutor.UNIX_PROC_START_COMMAND

        # Unix template should use single quotes around config values
        assert "log_destination='stderr'" in template
        assert "unix_socket_directories='{unixsocketdir}'" in template

    def test_windows_command_template_no_single_quotes(self) -> None:
        """Test that Windows template has no single quotes.

        Windows cmd.exe treats single quotes as literal characters, not
        delimiters, which causes errors when passed to pg_ctl.
        """
        template = PostgreSQLExecutor.WINDOWS_PROC_START_COMMAND

        # Windows template should NOT use single quotes
        assert "log_destination=stderr" in template
        assert "log_destination='stderr'" not in template
        assert "'" not in template

    def test_windows_command_template_omits_unix_socket_directories(self) -> None:
        """Test that Windows template does not include unix_socket_directories.

        PostgreSQL ignores unix_socket_directories on Windows entirely, so
        including it is unnecessary and avoids any quoting complexity.
        """
        template = PostgreSQLExecutor.WINDOWS_PROC_START_COMMAND

        assert "unix_socket_directories" not in template
        assert "{unixsocketdir}" not in template

    def test_unix_command_template_includes_unix_socket_directories(self) -> None:
        """Test that Unix template includes unix_socket_directories."""
        template = PostgreSQLExecutor.UNIX_PROC_START_COMMAND

        assert "unix_socket_directories='{unixsocketdir}'" in template

    def test_unix_template_protects_paths_with_spaces(self) -> None:
        """Test that Unix template properly quotes paths containing spaces.

        When unixsocketdir contains spaces (e.g., custom temp directories),
        the single quotes in the Unix template protect the path from being
        split by PostgreSQL's argument parser.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Linux"):
            executor = PostgreSQLExecutor(
                executable="/usr/lib/postgresql/16/bin/pg_ctl",
                host="localhost",
                port=5432,
                datadir="/tmp/data",
                unixsocketdir="/tmp/my socket dir",
                logfile="/tmp/log",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # The path with spaces should be enclosed in single quotes
        assert "unix_socket_directories='/tmp/my socket dir'" in command

    def test_windows_template_selected_on_windows(self) -> None:
        """Test that Windows template is selected when platform is Windows."""
        with patch("pytest_postgresql.executor.platform.system", return_value="Windows"):
            executor = PostgreSQLExecutor(
                executable="C:/Program Files/PostgreSQL/bin/pg_ctl.exe",
                host="localhost",
                port=5432,
                datadir="C:/temp/data",
                unixsocketdir="C:/temp/socket",
                logfile="C:/temp/log",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # Windows template should not have unix_socket_directories
        assert "unix_socket_directories" not in command
        # Windows template should not have single quotes
        assert "log_destination=stderr" in command
        assert "log_destination='stderr'" not in command

    def test_unix_template_selected_on_linux(self) -> None:
        """Test that Unix template is selected when platform is Linux."""
        with patch("pytest_postgresql.executor.platform.system", return_value="Linux"):
            executor = PostgreSQLExecutor(
                executable="/usr/lib/postgresql/16/bin/pg_ctl",
                host="localhost",
                port=5432,
                datadir="/tmp/data",
                unixsocketdir="/tmp/socket",
                logfile="/tmp/log",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # Unix template should have unix_socket_directories with single quotes
        assert "unix_socket_directories='/tmp/socket'" in command
        assert "log_destination='stderr'" in command


class TestWindowsCompatibility:
    """Test Windows-specific process management functionality."""

    def test_windows_terminate_process(self) -> None:
        """Test Windows process termination."""
        executor = PostgreSQLExecutor(
            executable="/path/to/pg_ctl",
            host="localhost",
            port=5432,
            datadir="/tmp/data",
            unixsocketdir="/tmp/socket",
            logfile="/tmp/log",
            startparams="-w",
            dbname="test",
        )

        # Mock process
        mock_process = MagicMock()
        executor.process = mock_process

        # No need to mock platform.system() since the method doesn't check it anymore
        executor._windows_terminate_process()

        # Should call terminate first
        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called()

    def test_windows_terminate_process_force_kill(self) -> None:
        """Test Windows process termination with force kill on timeout."""
        executor = PostgreSQLExecutor(
            executable="/path/to/pg_ctl",
            host="localhost",
            port=5432,
            datadir="/tmp/data",
            unixsocketdir="/tmp/socket",
            logfile="/tmp/log",
            startparams="-w",
            dbname="test",
        )

        # Mock process that times out
        mock_process = MagicMock()
        mock_process.wait.side_effect = [subprocess.TimeoutExpired(cmd="test", timeout=5), None]
        executor.process = mock_process

        # No need to mock platform.system() since the method doesn't check it anymore
        executor._windows_terminate_process()

        # Should call terminate, wait (timeout), then kill, then wait again
        mock_process.terminate.assert_called_once()
        mock_process.kill.assert_called_once()
        assert mock_process.wait.call_count == 2

    def test_stop_method_windows(self) -> None:
        """Test stop method on Windows."""
        executor = PostgreSQLExecutor(
            executable="/path/to/pg_ctl",
            host="localhost",
            port=5432,
            datadir="/tmp/data",
            unixsocketdir="/tmp/socket",
            logfile="/tmp/log",
            startparams="-w",
            dbname="test",
        )

        # Mock subprocess and process
        with (
            patch("pytest_postgresql.executor.subprocess.check_output") as mock_subprocess,
            patch("platform.system", return_value="Windows"),
            patch.object(executor, "_windows_terminate_process") as mock_terminate,
        ):
            result = executor.stop()

            # Should call pg_ctl stop and Windows terminate
            mock_subprocess.assert_called_once_with(
                ["/path/to/pg_ctl", "stop", "-D", "/tmp/data", "-m", "f"],
            )
            mock_terminate.assert_called_once_with(None)
            assert result is executor

    def test_stop_method_unix(self) -> None:
        """Test stop method on Unix systems."""
        executor = PostgreSQLExecutor(
            executable="/path/to/pg_ctl",
            host="localhost",
            port=5432,
            datadir="/tmp/data",
            unixsocketdir="/tmp/socket",
            logfile="/tmp/log",
            startparams="-w",
            dbname="test",
        )

        # Mock subprocess and super().stop
        with (
            patch("pytest_postgresql.executor.subprocess.check_output") as mock_subprocess,
            patch("platform.system", return_value="Linux"),
            patch("pytest_postgresql.executor.TCPExecutor.stop") as mock_super_stop,
        ):
            mock_super_stop.return_value = executor
            result = executor.stop()

            # Should call pg_ctl stop and parent class stop
            mock_subprocess.assert_called_once()
            mock_super_stop.assert_called_once_with(None, None)
            assert result is executor

    def test_stop_method_fallback_on_killpg_error(self) -> None:
        """Test stop method falls back to Windows termination on killpg AttributeError."""
        import pytest_postgresql.executor

        executor = PostgreSQLExecutor(
            executable="/path/to/pg_ctl",
            host="localhost",
            port=5432,
            datadir="/tmp/data",
            unixsocketdir="/tmp/socket",
            logfile="/tmp/log",
            startparams="-w",
            dbname="test",
        )

        # Mock subprocess and super().stop to raise AttributeError
        with (
            patch("pytest_postgresql.executor.subprocess.check_output") as mock_subprocess,
            patch("platform.system", return_value="Linux"),
            patch(
                "pytest_postgresql.executor.TCPExecutor.stop",
                side_effect=AttributeError("module 'os' has no attribute 'killpg'"),
            ),
            patch.object(executor, "_windows_terminate_process") as mock_terminate,
        ):
            # Temporarily remove os.killpg so hasattr(os, "killpg") returns False
            real_killpg = getattr(pytest_postgresql.executor.os, "killpg", None)
            try:
                if real_killpg is not None:
                    delattr(pytest_postgresql.executor.os, "killpg")
                result = executor.stop()
            finally:
                if real_killpg is not None:
                    pytest_postgresql.executor.os.killpg = real_killpg

            # Should call pg_ctl stop, fail on super().stop, then use Windows terminate
            mock_subprocess.assert_called_once()
            mock_terminate.assert_called_once()
            assert result is executor

    def test_command_formatting_windows(self) -> None:
        """Test that command is properly formatted for Windows paths."""
        with patch("pytest_postgresql.executor.platform.system", return_value="Windows"):
            executor = PostgreSQLExecutor(
                executable="C:/Program Files/PostgreSQL/bin/pg_ctl.exe",
                host="localhost",
                port=5555,
                datadir="C:/temp/data",
                unixsocketdir="C:/temp/socket",
                logfile="C:/temp/log.txt",
                startparams="-w -s",
                dbname="testdb",
                postgres_options="-c shared_preload_libraries=test",
            )

        # The command should be properly formatted without single quotes
        # and without unix_socket_directories (irrelevant on Windows)
        expected_parts = [
            "C:/Program Files/PostgreSQL/bin/pg_ctl.exe start",
            '-D "C:/temp/data"',
            '-o "-F -p 5555 -c log_destination=stderr',
            "-c logging_collector=off",
            '-c shared_preload_libraries=test"',
            '-l "C:/temp/log.txt"',
            "-w -s",
        ]

        command = executor.command
        for part in expected_parts:
            assert part in command, f"Expected '{part}' in command: {command}"

        # Verify unix_socket_directories is NOT in the Windows command
        assert "unix_socket_directories" not in command, (
            f"unix_socket_directories should not be in Windows command: {command}"
        )
