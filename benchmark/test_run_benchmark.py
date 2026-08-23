import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import run_benchmark


class FakeScalar:
    def __init__(self, value):
        self.value = value

    def as_py(self):
        return self.value


class FakeColumn:
    def __init__(self, values):
        self.values = values

    def __getitem__(self, index):
        return FakeScalar(self.values[index])


class FakeTable:
    def __init__(self, names, rows):
        self.schema = SimpleNamespace(names=names)
        self.num_rows = len(rows)
        self.columns = [
            FakeColumn([row[index] for row in rows])
            for index in range(len(names))
        ]


class LakeletRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.bin_path = self.root / "lakelet"
        self.bin_path.touch(mode=0o755)
        self.config_path = self.root / "config.toml"
        self.config_path.write_text("", encoding="utf-8")
        self.sql_path = self.root / "q01.sql"
        self.sql_path.write_text("select 1 as value;\n", encoding="utf-8")
        self.log_path = self.root / "lakelet-server.log"

    def tearDown(self):
        self.temp_dir.cleanup()

    def make_runner(self):
        return run_benchmark.LakeletRunner(
            repo_root=self.root,
            bin_path=self.bin_path,
            config_path=self.config_path,
            default_catalog="hive",
            default_schema="tpch",
            server_log_path=self.log_path,
        )

    def test_reads_default_and_custom_server_ports(self):
        self.assertEqual(
            run_benchmark.read_flight_sql_server_port(self.config_path), 32010
        )

        self.config_path.write_text(
            "[server]\nflight-sql-server-port = 32123\n", encoding="utf-8"
        )
        self.assertEqual(
            run_benchmark.read_flight_sql_server_port(self.config_path), 32123
        )

    def test_rejects_invalid_server_port(self):
        self.config_path.write_text(
            '[server]\nflight-sql-server-port = "32010"\n', encoding="utf-8"
        )
        with self.assertRaisesRegex(
            run_benchmark.BenchmarkInfrastructureError,
            "must be an integer between 1 and 65535",
        ):
            run_benchmark.read_flight_sql_server_port(self.config_path)

    @patch("run_benchmark.wait_for_server")
    @patch("run_benchmark.ensure_port_available")
    @patch("run_benchmark.subprocess.Popen")
    @patch("run_benchmark.load_adbc_driver")
    def test_prepare_starts_one_server_and_connects_with_headers(
        self, load_driver, popen, ensure_port, wait_for_server
    ):
        connection = MagicMock()
        flight_sql = MagicMock()
        flight_sql.connect.return_value = connection
        database_options = SimpleNamespace(
            RPC_CALL_HEADER_PREFIX=SimpleNamespace(
                value="adbc.flight.sql.rpc.call_header."
            )
        )
        load_driver.return_value = (flight_sql, database_options)
        process = MagicMock()
        process.poll.return_value = None
        popen.return_value = process
        runner = self.make_runner()

        runner.prepare()

        popen.assert_called_once()
        command = popen.call_args.args[0]
        self.assertEqual(
            command,
            [
                str(self.bin_path),
                "--config",
                str(self.config_path),
                "--flight-sql-server",
            ],
        )
        ensure_port.assert_called_once_with("127.0.0.1", 32010)
        wait_for_server.assert_called_once_with(process, "127.0.0.1", 32010, 30)
        flight_sql.connect.assert_called_once_with(
            "grpc://127.0.0.1:32010",
            db_kwargs={
                "adbc.flight.sql.rpc.call_header.default-catalog": "hive",
                "adbc.flight.sql.rpc.call_header.default-schema": "tpch",
            },
        )

        runner.close()
        connection.close.assert_called_once_with()
        process.terminate.assert_called_once_with()

    def test_run_query_fetches_full_result_before_stopping_timer(self):
        runner = self.make_runner()
        runner.server_process = MagicMock()
        runner.server_process.poll.return_value = None
        cursor = MagicMock()
        cursor.fetch_arrow_table.return_value = FakeTable(
            ["value", "note"], [[1, None], [2, b"ok"]]
        )
        runner.connection = MagicMock()
        runner.connection.cursor.return_value.__enter__.return_value = cursor

        with patch("run_benchmark.time.perf_counter", side_effect=[10.0, 12.5]):
            first = runner.run_query(self.sql_path)
        with patch("run_benchmark.time.perf_counter", side_effect=[20.0, 21.0]):
            second = runner.run_query(self.sql_path)

        self.assertEqual(first.elapsed_seconds, 2.5)
        self.assertEqual(second.elapsed_seconds, 1.0)
        self.assertEqual(runner.connection.cursor.call_count, 2)
        cursor.execute.assert_called_with("select 1 as value;\n")
        self.assertEqual(cursor.fetch_arrow_table.call_count, 2)
        self.assertIn("protocol=adbc-flight-sql", first.raw_output)
        self.assertIn("1     | NULL", first.raw_output)
        self.assertIn("2     | 6f6b", first.raw_output)

    def test_query_failure_is_recordable_when_server_is_alive(self):
        runner = self.make_runner()
        runner.server_process = MagicMock()
        runner.server_process.poll.return_value = None
        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("bad SQL")
        runner.connection = MagicMock()
        runner.connection.cursor.return_value.__enter__.return_value = cursor

        with self.assertRaises(run_benchmark.BenchmarkError) as raised:
            runner.run_query(self.sql_path)

        self.assertNotIsInstance(
            raised.exception, run_benchmark.BenchmarkInfrastructureError
        )
        self.assertIn("error=bad SQL", raised.exception.raw_output)

    def test_server_exit_is_an_infrastructure_error(self):
        runner = self.make_runner()
        runner.connection = MagicMock()
        runner.server_process = MagicMock()
        runner.server_process.poll.return_value = 7
        self.log_path.write_text("server failed\n", encoding="utf-8")

        with self.assertRaisesRegex(
            run_benchmark.BenchmarkInfrastructureError,
            "exited with status 7",
        ):
            runner.run_query(self.sql_path)

    @patch("run_benchmark.wait_for_server")
    @patch("run_benchmark.ensure_port_available")
    @patch("run_benchmark.subprocess.Popen")
    @patch("run_benchmark.load_adbc_driver")
    def test_startup_failure_stops_server(
        self, load_driver, popen, ensure_port, wait_for_server
    ):
        load_driver.return_value = (MagicMock(), MagicMock())
        process = MagicMock()
        process.poll.return_value = None
        popen.return_value = process
        wait_for_server.side_effect = run_benchmark.BenchmarkInfrastructureError(
            "not ready"
        )
        runner = self.make_runner()

        with self.assertRaisesRegex(
            run_benchmark.BenchmarkInfrastructureError, "not ready"
        ):
            runner.prepare()

        process.terminate.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=10)
        self.assertIsNone(runner.server_process)

    @patch("run_benchmark.time.sleep")
    @patch("run_benchmark.time.monotonic", side_effect=[0.0, 0.0, 1.0])
    @patch("run_benchmark.socket.create_connection")
    def test_wait_for_server_retries_until_port_is_ready(
        self, create_connection, _monotonic, sleep
    ):
        ready_socket = MagicMock()
        create_connection.side_effect = [OSError("not ready"), ready_socket]
        process = MagicMock()
        process.poll.return_value = None

        run_benchmark.wait_for_server(process, "127.0.0.1", 32010, 30)

        self.assertEqual(create_connection.call_count, 2)
        sleep.assert_called_once_with(0.1)

    def test_stop_process_kills_after_timeout(self):
        process = MagicMock()
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired("lakelet", 10), 0]

        run_benchmark.stop_process(process, 10)

        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()
        self.assertEqual(process.wait.call_args_list[-1].args, ())


if __name__ == "__main__":
    unittest.main()
