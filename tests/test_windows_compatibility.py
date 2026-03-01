"""Test Windows compatibility fixes for pytest-postgresql."""

import os
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

    def test_darwin_template_selection(self) -> None:
        """Test that Darwin/macOS uses Unix template.

        macOS should use the same Unix template as Linux since it's a Unix-like
        system and supports unix_socket_directories.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Darwin"):
            executor = PostgreSQLExecutor(
                executable="/opt/homebrew/bin/pg_ctl",
                host="localhost",
                port=5432,
                datadir="/tmp/data",
                unixsocketdir="/tmp/socket",
                logfile="/tmp/log",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # Darwin should use Unix template with unix_socket_directories and single quotes
        assert "unix_socket_directories='/tmp/socket'" in command
        assert "log_destination='stderr'" in command

    def test_darwin_locale_setting(self) -> None:
        """Test that Darwin/macOS sets en_US.UTF-8 locale.

        Darwin requires en_US.UTF-8 instead of C.UTF-8 which is used on Linux.
        This test verifies the locale environment variables are set correctly.
        """
        # Patch the _LOCALE variable to simulate Darwin environment
        with patch("pytest_postgresql.executor._LOCALE", "en_US.UTF-8"):
            executor = PostgreSQLExecutor(
                executable="/opt/homebrew/bin/pg_ctl",
                host="localhost",
                port=5432,
                datadir="/tmp/data",
                unixsocketdir="/tmp/socket",
                logfile="/tmp/log",
                startparams="-w",
                dbname="test",
            )

        # Darwin should set en_US.UTF-8 locale
        assert executor.envvars["LC_ALL"] == "en_US.UTF-8"
        assert executor.envvars["LC_CTYPE"] == "en_US.UTF-8"
        assert executor.envvars["LANG"] == "en_US.UTF-8"

    def test_postgres_options_with_single_quotes_unix(self) -> None:
        """Test postgres_options containing single quotes on Unix.

        Single quotes in postgres_options should be preserved and passed through
        to PostgreSQL on Unix systems.
        """
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
                postgres_options="-c shared_buffers='128MB' -c work_mem='64MB'",
            )

        command = executor.command
        # postgres_options should be included as-is with single quotes preserved
        assert "-c shared_buffers='128MB' -c work_mem='64MB'" in command

    def test_postgres_options_with_single_quotes_windows(self) -> None:
        """Test postgres_options containing single quotes on Windows.

        Single quotes in postgres_options should work on Windows since they're
        inside the -o parameter's double quotes.
        """
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
                postgres_options="-c shared_buffers='128MB' -c work_mem='64MB'",
            )

        command = executor.command
        # postgres_options should be included with single quotes preserved
        assert "-c shared_buffers='128MB' -c work_mem='64MB'" in command

    def test_postgres_options_with_double_quotes(self) -> None:
        """Test postgres_options containing double quotes.

        Double quotes in postgres_options need careful handling as they interact
        with the shell's quote parsing.
        """
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
                postgres_options='-c search_path="public,other"',
            )

        command = executor.command
        # postgres_options with double quotes should be preserved
        assert '-c search_path="public,other"' in command

    def test_postgres_options_with_paths_containing_spaces(self) -> None:
        """Test postgres_options with file paths containing spaces.

        Config options that reference file paths with spaces should be properly
        quoted within postgres_options.
        """
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
                postgres_options="""-c config_file='/etc/postgres/my config.conf'""",
            )

        command = executor.command
        # postgres_options with paths containing spaces should be preserved
        assert """-c config_file='/etc/postgres/my config.conf'""" in command

    def test_empty_postgres_options(self) -> None:
        """Test command generation with empty postgres_options.

        When postgres_options is empty (default), the command should still be
        properly formatted without extra spaces or malformed syntax.
        """
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
                postgres_options="",
            )

        command = executor.command
        # Command should still be valid with empty postgres_options
        assert "/usr/lib/postgresql/16/bin/pg_ctl start" in command
        assert '-D "/tmp/data"' in command
        assert "unix_socket_directories='/tmp/socket'" in command
        # Should not have trailing space before closing quote in -o parameter
        expected_opts = (
            "-o \"-F -p 5432 -c log_destination='stderr' "
            "-c logging_collector=off -c unix_socket_directories='/tmp/socket'\""
        )
        assert expected_opts in command

    def test_empty_startparams(self) -> None:
        """Test command generation with empty startparams.

        When startparams is empty (default), the command should still be
        properly formatted at the end.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Linux"):
            executor = PostgreSQLExecutor(
                executable="/usr/lib/postgresql/16/bin/pg_ctl",
                host="localhost",
                port=5432,
                datadir="/tmp/data",
                unixsocketdir="/tmp/socket",
                logfile="/tmp/log",
                startparams="",
                dbname="test",
            )

        command = executor.command
        # Command should be valid with empty startparams
        assert "/usr/lib/postgresql/16/bin/pg_ctl start" in command
        assert '-l "/tmp/log"' in command
        # Command should not have trailing spaces at the end
        assert not command.endswith("  ")

    def test_both_empty_postgres_options_and_startparams(self) -> None:
        """Test command generation with both postgres_options and startparams empty.

        When both optional parameters are empty, the command should still
        be properly formatted.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Windows"):
            executor = PostgreSQLExecutor(
                executable="C:/Program Files/PostgreSQL/bin/pg_ctl.exe",
                host="localhost",
                port=5432,
                datadir="C:/temp/data",
                unixsocketdir="C:/temp/socket",
                logfile="C:/temp/log",
                startparams="",
                dbname="test",
                postgres_options="",
            )

        command = executor.command
        # Command should be valid with both empty
        assert "C:/Program Files/PostgreSQL/bin/pg_ctl.exe start" in command
        assert '-D "C:/temp/data"' in command
        assert '-l "C:/temp/log"' in command
        # Windows template should not have unix_socket_directories
        assert "unix_socket_directories" not in command

    def test_unixsocketdir_ignored_on_windows_in_command(self) -> None:
        """Test that unixsocketdir value doesn't appear in Windows command.

        Even when unixsocketdir is passed to the executor on Windows, its value
        should not appear anywhere in the generated command since Windows doesn't
        use unix_socket_directories.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Windows"):
            executor = PostgreSQLExecutor(
                executable="C:/Program Files/PostgreSQL/bin/pg_ctl.exe",
                host="localhost",
                port=5432,
                datadir="C:/temp/data",
                unixsocketdir="C:/this/should/not/appear",
                logfile="C:/temp/log",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # The unixsocketdir value should NOT appear in the Windows command
        assert "C:/this/should/not/appear" not in command
        assert "unix_socket_directories" not in command

    def test_paths_with_multiple_consecutive_spaces(self) -> None:
        """Test paths with multiple consecutive spaces.

        Paths with multiple spaces should be properly quoted and preserved.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Linux"):
            executor = PostgreSQLExecutor(
                executable="/usr/lib/postgresql/16/bin/pg_ctl",
                host="localhost",
                port=5432,
                datadir="/tmp/data",
                unixsocketdir="/tmp/my    socket    dir",
                logfile="/tmp/log",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # Multiple spaces should be preserved
        assert "unix_socket_directories='/tmp/my    socket    dir'" in command

    def test_paths_with_special_shell_characters(self) -> None:
        """Test paths with special shell characters.

        Paths with shell metacharacters should be properly quoted to prevent
        shell interpretation. Testing with ampersand, semicolon, and pipe.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Linux"):
            executor = PostgreSQLExecutor(
                executable="/usr/lib/postgresql/16/bin/pg_ctl",
                host="localhost",
                port=5432,
                datadir="/tmp/data",
                unixsocketdir="/tmp/socket&test",
                logfile="/tmp/log;file",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # Special characters should be inside quotes
        assert "unix_socket_directories='/tmp/socket&test'" in command
        assert '-l "/tmp/log;file"' in command

    def test_paths_with_unicode_characters(self) -> None:
        """Test paths with Unicode characters.

        Unicode characters in paths should be properly handled and preserved.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Linux"):
            executor = PostgreSQLExecutor(
                executable="/usr/lib/postgresql/16/bin/pg_ctl",
                host="localhost",
                port=5432,
                datadir="/tmp/data",
                unixsocketdir="/tmp/sóckét_dïr_日本語",
                logfile="/tmp/lög_文件.log",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # Unicode characters should be preserved
        assert "unix_socket_directories='/tmp/sóckét_dïr_日本語'" in command
        assert '-l "/tmp/lög_文件.log"' in command

    def test_command_with_all_special_characters_combined(self) -> None:
        """Test command with multiple types of special characters.

        This comprehensive test combines spaces, quotes, special shell chars,
        and Unicode to ensure the command handles complex real-world scenarios.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Linux"):
            executor = PostgreSQLExecutor(
                executable="/usr/lib/postgresql/16/bin/pg_ctl",
                host="localhost",
                port=5432,
                datadir="/tmp/my data & files",
                unixsocketdir="/tmp/sóckét dir (test)",
                logfile="/tmp/log file; output.log",
                startparams="-w -t 30",
                dbname="test",
                postgres_options="-c shared_buffers='256MB' -c config_file='/etc/pg/main.conf'",
            )

        command = executor.command
        # All special characters should be properly handled
        assert '-D "/tmp/my data & files"' in command
        assert "unix_socket_directories='/tmp/sóckét dir (test)'" in command
        assert '-l "/tmp/log file; output.log"' in command
        assert "-c shared_buffers='256MB'" in command
        assert "-c config_file='/etc/pg/main.conf'" in command
        assert "-w -t 30" in command


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
            patch("pytest_postgresql.executor.platform.system", return_value="Windows"),
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
            patch("pytest_postgresql.executor.platform.system", return_value="Linux"),
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
            patch("pytest_postgresql.executor.platform.system", return_value="Linux"),
            patch(
                "pytest_postgresql.executor.TCPExecutor.stop",
                side_effect=AttributeError("module 'os' has no attribute 'killpg'"),
            ),
            patch.object(executor, "_windows_terminate_process") as mock_terminate,
        ):
            # Temporarily remove os.killpg so hasattr(os, "killpg") returns False
            real_killpg = getattr(os, "killpg", None)
            try:
                if real_killpg is not None:
                    delattr(os, "killpg")
                result = executor.stop()
            finally:
                if real_killpg is not None:
                    os.killpg = real_killpg

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

    def test_windows_datadir_with_spaces(self) -> None:
        """Test Windows datadir with spaces in path.

        Windows paths with spaces should be properly quoted with double quotes.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Windows"):
            executor = PostgreSQLExecutor(
                executable="C:/Program Files/PostgreSQL/bin/pg_ctl.exe",
                host="localhost",
                port=5432,
                datadir="C:/Program Files/PostgreSQL/my data dir",
                unixsocketdir="C:/temp/socket",
                logfile="C:/temp/log",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # datadir with spaces should be quoted
        assert '-D "C:/Program Files/PostgreSQL/my data dir"' in command

    def test_windows_logfile_with_spaces(self) -> None:
        """Test Windows logfile with spaces in path.

        Windows log file paths with spaces should be properly quoted with double quotes.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Windows"):
            executor = PostgreSQLExecutor(
                executable="C:/Program Files/PostgreSQL/bin/pg_ctl.exe",
                host="localhost",
                port=5432,
                datadir="C:/temp/data",
                unixsocketdir="C:/temp/socket",
                logfile="C:/Program Files/PostgreSQL/logs/my log file.log",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # logfile with spaces should be quoted
        assert '-l "C:/Program Files/PostgreSQL/logs/my log file.log"' in command

    def test_windows_unc_paths(self) -> None:
        """Test Windows UNC (Universal Naming Convention) paths.

        UNC paths like \\\\server\\share should be properly handled on Windows.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Windows"):
            executor = PostgreSQLExecutor(
                executable="C:/Program Files/PostgreSQL/bin/pg_ctl.exe",
                host="localhost",
                port=5432,
                datadir="//server/share/postgres/data",
                unixsocketdir="//server/share/postgres/socket",
                logfile="//server/share/postgres/logs/postgresql.log",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # UNC paths should be properly quoted (using forward slashes in Python)
        assert '-D "//server/share/postgres/data"' in command
        assert '-l "//server/share/postgres/logs/postgresql.log"' in command

    def test_windows_mixed_slashes(self) -> None:
        """Test Windows paths with mixed forward and backslashes.

        Windows accepts both forward slashes and backslashes, and the command
        should handle both properly.
        """
        with patch("pytest_postgresql.executor.platform.system", return_value="Windows"):
            executor = PostgreSQLExecutor(
                executable="C:\\Program Files\\PostgreSQL\\bin\\pg_ctl.exe",
                host="localhost",
                port=5432,
                datadir="C:\\temp\\data",
                unixsocketdir="C:\\temp\\socket",
                logfile="C:\\temp\\log.txt",
                startparams="-w",
                dbname="test",
            )

        command = executor.command
        # Paths with backslashes should be properly quoted
        assert "C:\\Program Files\\PostgreSQL\\bin\\pg_ctl.exe start" in command
        assert '-D "C:\\temp\\data"' in command
        assert '-l "C:\\temp\\log.txt"' in command
