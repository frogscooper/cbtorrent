from dataclasses import asdict, dataclass
from time import perf_counter, process_time


@dataclass
class Metrics:
    wire_sent_bytes: int = 0
    wire_received_bytes: int = 0
    payload_received_bytes: int = 0
    verified_bytes: int = 0
    resumed_bytes: int = 0
    uploaded_bytes: int = 0
    tracker_response_bytes: int = 0
    tracker_requests: int = 0
    tracker_failures: int = 0
    connections: int = 0
    peer_failures: int = 0
    hash_failures: int = 0
    policy_seconds: float = 0.0
    policy_update_seconds: float = 0.0
    policy_deferrals: int = 0

    def start(self):
        self._started = perf_counter()
        self._cpu_started = process_time()

    def report(self, *, complete: bool):
        result = asdict(self)
        result.update(
            complete=complete,
            elapsed_seconds=perf_counter() - self._started,
            cpu_seconds=process_time() - self._cpu_started,
            protocol_overhead_bytes=self.wire_sent_bytes + self.wire_received_bytes - self.payload_received_bytes - self.uploaded_bytes,
            wasted_payload_bytes=self.payload_received_bytes - self.verified_bytes,
        )
        result["completion_seconds"] = result["elapsed_seconds"] if complete else None
        return result
