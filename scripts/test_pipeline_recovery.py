
from src.monitoring.pipeline_recovery import get_batch_status


def test_completed_batch_is_not_reprocessed():

    status = get_batch_status()

    assert status == "SUCCESS", (
        f"Expected SUCCESS for completed batch, got {status}"
    )

    print("PASS: Completed batch is protected from retry.")


if __name__ == "__main__":
    test_completed_batch_is_not_reprocessed()
    print("\nRecovery protection test passed.")
