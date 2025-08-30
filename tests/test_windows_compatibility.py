"""Test Windows compatibility fixes for pytest-postgresql."""

import platform
import pytest
from unittest.mock import Mock, patch, MagicMock
import subprocess
import signal

from pytest_postgresql.executor import PostgreSQLExecutor


class TestWindowsCompatibility:
    """Test Windows-specific functionality."""

    def test_get_base_command_windows(self):
        """Test that Windows base command doesn't use single quotes."""
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
        
        with patch('platform.system', return_value='Windows'):
            command = executor._get_base_command()
            # Windows command should not have single quotes around stderr
            assert "log_destination=stderr" in command
            assert "log_destination='stderr'" not in command
            assert "unix_socket_directories={unixsocketdir}" in command
            assert "unix_socket_directories='{unixsocketdir}'" not in command

    def test_get_base_command_unix(self):
        """Test that Unix base command uses single quotes."""
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
        
        with patch('platform.system', return_value='Linux'):
            command = executor._get_base_command()
            # Unix command should have single quotes around stderr
            assert "log_destination='stderr'" in command
            assert "unix_socket_directories='{unixsocketdir}'" in command

    def test_windows_terminate_process(self):
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
        
        with patch('platform.system', return_value='Windows'):
            executor._windows_terminate_process()
            
            # Should call terminate first
            mock_process.terminate.assert_called_once()
            mock_process.wait.assert_called()

    def test_windows_terminate_process_force_kill(self):
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
        
        with patch('platform.system', return_value='Windows'):
            executor._windows_terminate_process()
            
            # Should call terminate, wait (timeout), then kill, then wait again
            mock_process.terminate.assert_called_once()
            mock_process.kill.assert_called_once()
            assert mock_process.wait.call_count == 2

    def test_stop_method_windows(self):
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
        with patch('subprocess.check_output') as mock_subprocess, \
             patch('platform.system', return_value='Windows'), \
             patch.object(executor, '_windows_terminate_process') as mock_terminate:
            
            result = executor.stop()
            
            # Should call pg_ctl stop and Windows terminate
            mock_subprocess.assert_called_once()
            mock_terminate.assert_called_once_with(None)
            assert result is executor

    def test_stop_method_unix(self):
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
        with patch('subprocess.check_output') as mock_subprocess, \
             patch('platform.system', return_value='Linux'), \
             patch('pytest_postgresql.executor.TCPExecutor.stop') as mock_super_stop:
            
            mock_super_stop.return_value = executor
            result = executor.stop()
            
            # Should call pg_ctl stop and parent class stop
            mock_subprocess.assert_called_once()
            mock_super_stop.assert_called_once_with(None, None)
            assert result is executor

    def test_stop_method_fallback_on_killpg_error(self):
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
        with patch('subprocess.check_output') as mock_subprocess, \
             patch('platform.system', return_value='Linux'), \
             patch('pytest_postgresql.executor.TCPExecutor.stop', 
                   side_effect=AttributeError("module 'os' has no attribute 'killpg'")), \
             patch.object(executor, '_windows_terminate_process') as mock_terminate:
            
            result = executor.stop()
            
            # Should call pg_ctl stop, fail on super().stop, then use Windows terminate
            mock_subprocess.assert_called_once()
            mock_terminate.assert_called_once_with(None)
            assert result is executor

    def test_command_formatting_windows(self):
        """Test that command is properly formatted for Windows."""
        with patch('platform.system', return_value='Windows'):
            executor = PostgreSQLExecutor(
                executable="C:/Program Files/PostgreSQL/bin/pg_ctl.exe",
                host="localhost",
                port=5555,
                datadir="C:/temp/data",
                unixsocketdir="C:/temp/socket",
                logfile="C:/temp/log.txt",
                startparams="-w -s",
                dbname="testdb",
                postgres_options="-c shared_preload_libraries=test"
            )
            
            # The command should be properly formatted without quotes around stderr
            expected_parts = [
                "C:/Program Files/PostgreSQL/bin/pg_ctl.exe start",
                '-D "C:/temp/data"',
                "-o \"-F -p 5555 -c log_destination=stderr",
                "-c logging_collector=off",
                "-c unix_socket_directories=C:/temp/socket",
                "-c shared_preload_libraries=test\"",
                '-l "C:/temp/log.txt"',
                "-w -s"
            ]
            
            # Check if all expected parts are in the command
            command = executor.command
            for part in expected_parts:
                assert part in command, f"Expected '{part}' in command: {command}"
