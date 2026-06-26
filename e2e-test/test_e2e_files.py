from common.testfile import run_test_file


def test_e2e_test_file(test_file, aws_context, dobbydb_runner):
    run_test_file(test_file, aws_context, dobbydb_runner)
