from __future__ import annotations
from datetime import datetime, timezone
from domain.ports import ReportSink
from domain.models import WindowReport

class PrintSink(ReportSink):
    def handle(self, report: WindowReport) -> None:
        stamp = datetime.fromtimestamp(report.stamp_epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        backlog = report.total_enqueued - report.total_processed

        lines = []
        lines.append(
            f"[{stamp}] window={report.window_sec:.3f}s "
            f"total_enqueued={report.total_enqueued:,} total_processed={report.total_processed:,} "
            f"backlog={backlog:,} dropped={report.total_dropped:,} "
            f"batch={report.batch_size_last:,} shards={report.shards}"
        )

        if report.rows:
            lines.append("TOP window max latency (ms): key | count | mean | max | last | dropped")
            for r in report.rows:
                lines.append(
                    f"  {r.key:>6} | {r.count:>5} | {r.mean_ms:>8.3f} | {r.max_ms:>8.3f} | {r.last_ms:>8.3f} | {r.dropped:,}"
                )
        else:
            lines.append("No events in this window.")

        print("\n".join(lines) + "\n", flush=True)
