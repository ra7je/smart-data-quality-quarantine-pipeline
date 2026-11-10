
import unittest
from unittest.mock import patch

from src.monitoring import pipeline_recovery


class TestPipelineRecovery(unittest.TestCase):

    @patch.object(pipeline_recovery, "run_bulk_silver_pipeline")
    @patch.object(pipeline_recovery, "get_batch_status")
    def test_successful_batch_is_skipped(self, mock_status, mock_run):
        mock_status.return_value = "SUCCESS"

        pipeline_recovery.recover_bulk_pipeline()

        mock_run.assert_not_called()

    @patch.object(pipeline_recovery, "run_bulk_silver_pipeline")
    @patch.object(pipeline_recovery, "get_batch_status")
    def test_partial_batch_is_not_reprocessed(self, mock_status, mock_run):
        mock_status.return_value = "PARTIAL_OR_UNAUDITED"

        with self.assertRaises(RuntimeError):
            pipeline_recovery.recover_bulk_pipeline()

        mock_run.assert_not_called()

    @patch.object(pipeline_recovery.time, "sleep")
    @patch.object(pipeline_recovery, "record_failure")
    @patch.object(pipeline_recovery, "run_bulk_silver_pipeline")
    @patch.object(pipeline_recovery, "get_batch_status")
    def test_failed_attempt_can_be_retried(
        self, mock_status, mock_run, mock_failure, mock_sleep
    ):
        mock_status.side_effect = [
            "NOT_PROCESSED",
            "NOT_PROCESSED"
        ]

        mock_run.side_effect = [
            RuntimeError("Simulated isolated test failure"),
            None
        ]

        pipeline_recovery.recover_bulk_pipeline()

        self.assertEqual(mock_run.call_count, 2)

    @patch.object(pipeline_recovery.time, "sleep")
    @patch.object(pipeline_recovery, "record_failure")
    @patch.object(pipeline_recovery, "run_bulk_silver_pipeline")
    @patch.object(pipeline_recovery, "get_batch_status")
    def test_failure_is_recorded_after_retries(
        self, mock_status, mock_run, mock_failure, mock_sleep
    ):
        mock_status.return_value = "NOT_PROCESSED"
        mock_run.side_effect = RuntimeError("Simulated test failure")

        with self.assertRaises(RuntimeError):
            pipeline_recovery.recover_bulk_pipeline()

        self.assertGreaterEqual(mock_failure.call_count, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
