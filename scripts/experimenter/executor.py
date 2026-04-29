"""
Async subprocess executor — runs sim arm scripts with kill support.

Key design:
  - Each arm runs as a separate subprocess (separate Python process = separate GIL,
    so parallel arms genuinely use separate cores)
  - Processes are tracked by run_id so they can be killed by name
  - Result is parsed from the EXPERIMENT_RESULT_JSON: marker in stdout
  - Hard timeout prevents hung sims from blocking forever
  - Early-kill: after ETA_CHECK_AFTER_S seconds, tqdm stderr is parsed; if the
    projected total runtime exceeds MAX_PROJECTED_S the run is killed immediately
  - env PYTHONHASHSEED=0 is always set for determinism
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import time
from pathlib import Path
from typing import Optional

from .models import RunResult, SimRun

logger = logging.getLogger(__name__)

_JSON_MARKER = "EXPERIMENT_RESULT_JSON:"
DEFAULT_TIMEOUT_S = 1200       # 20 min hard cap per run
ETA_CHECK_AFTER_S = 90        # check tqdm ETA after 90s of runtime
MAX_PROJECTED_S = 1200        # kill if tqdm projects total > 20 min

PROGRESS_DIR = Path(__file__).parent / "progress"


def _parse_tqdm_line(line: str) -> Optional[dict]:
    """
    Parse a single tqdm stderr line into a progress dict.

    tqdm format: " 45%|████▌   | 450/1000 [02:15<02:45,  3.33it/s]"
    Returns {pct, current, total, elapsed_s, eta_s} or None.
    """
    m = re.search(
        r"(\d+)%\|.*?\|\s*(\d+)/(\d+)\s*\[(\d+):(\d+)<(\d+):(\d+)",
        line,
    )
    if not m:
        return None
    pct = int(m.group(1))
    current, total = int(m.group(2)), int(m.group(3))
    elapsed_s = int(m.group(4)) * 60 + int(m.group(5))
    eta_s = int(m.group(6)) * 60 + int(m.group(7))
    return {"pct": pct, "current": current, "total": total, "elapsed_s": elapsed_s, "eta_s": eta_s}


def _write_progress(run_id: str, data: dict) -> None:
    try:
        PROGRESS_DIR.mkdir(exist_ok=True)
        (PROGRESS_DIR / f"{run_id}.json").write_text(json.dumps(data))
    except OSError:
        pass


def _clear_progress(run_id: str) -> None:
    try:
        (PROGRESS_DIR / f"{run_id}.json").unlink(missing_ok=True)
    except OSError:
        pass


def _parse_tqdm_eta(stderr_text: str) -> Optional[float]:
    """
    Parse the most recent tqdm ETA from stderr output.
    Returns remaining seconds, or None if no tqdm line found.

    tqdm format: [elapsed<remaining, rate]
    e.g. "[01:23<12:34, 67.3it/s]" → 12*60+34 = 754 remaining seconds
    """
    pattern = r"\[[\d:]+<([\d:]+),\s*[\d.]+\s*it/s\]"
    matches = re.findall(pattern, stderr_text)
    if not matches:
        return None
    parts = matches[-1].split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        pass
    return None


class RunningProcess:
    """Wraps a live asyncio subprocess for one sim run."""

    def __init__(self, run_id: str, process: asyncio.subprocess.Process, script_path: str) -> None:
        self.run_id = run_id
        self.process = process
        self.script_path = script_path
        self._start_time = time.monotonic()
        self.cancelled = False
        self.cancel_reason = ""

    def cancel(self, reason: str = "cancelled by orchestrator") -> None:
        if self.process.returncode is None:  # still running
            self.cancelled = True
            self.cancel_reason = reason
            try:
                self.process.send_signal(signal.SIGTERM)
                logger.info("SIGTERM → run %s (pid %d): %s", self.run_id, self.process.pid, reason)
            except (ProcessLookupError, OSError):
                pass

    @property
    def wall_seconds(self) -> float:
        return time.monotonic() - self._start_time


class Executor:
    """
    Manages running sim subprocesses.

    Two cancellation mechanisms (both result in SIGTERM):
      1. Hard timeout — cancels any run that exceeds `timeout_s` wall-clock minutes,
         regardless of its results. Set via --timeout-minutes CLI flag.
      2. Result-based cancellation — cancels a still-running run when another run's
         result makes it pointless. Triggered by CancelRules or the LLM evaluator.

    Usage:
        async with Executor(max_parallel=2) as ex:
            handle = await ex.launch(run, script_path)
            result = await ex.wait(handle, run, exp_number, timeout_s=3600)
    """

    def __init__(self, max_parallel: int = 2) -> None:
        self.max_parallel = max_parallel
        self._running: dict[str, RunningProcess] = {}

    async def __aenter__(self) -> "Executor":
        return self

    async def __aexit__(self, *_) -> None:
        for proc in list(self._running.values()):
            proc.cancel("executor shutdown")
        for proc in list(self._running.values()):
            try:
                await asyncio.wait_for(proc.process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                try:
                    proc.process.kill()
                except (ProcessLookupError, OSError):
                    pass

    async def launch(self, run: SimRun, script_path: Path) -> RunningProcess:
        """
        Launch a subprocess for `run`. The orchestrator is responsible for
        ensuring at most max_parallel runs are active simultaneously.
        """
        env = {**os.environ, "PYTHONHASHSEED": "0"}
        proc = await asyncio.create_subprocess_exec(
            "python3",
            str(script_path),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(script_path.parent.parent),  # scripts/
        )
        handle = RunningProcess(run.run_id, proc, str(script_path))
        self._running[run.run_id] = handle
        logger.info("Launched run %s (pid %d): %s", run.run_id, proc.pid, script_path.name)
        return handle

    def cancel_run(self, run_id: str, reason: str) -> None:
        """Cancel a running sim by run_id (result-based, not timeout)."""
        handle = self._running.get(run_id)
        if handle:
            handle.cancel(reason)
        else:
            logger.warning("cancel_run: run %s not found in running set", run_id)

    async def wait(
        self,
        handle: RunningProcess,
        run_spec: SimRun,
        exp_number: int,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        eta_check_after_s: float = ETA_CHECK_AFTER_S,
        max_projected_s: float = MAX_PROJECTED_S,
    ) -> RunResult:
        """
        Wait for a run to complete (or be cancelled / timeout / ETA-killed).

        Streams stdout and stderr into buffers while the process runs.
        After eta_check_after_s seconds, parses tqdm output to project total
        runtime. If projected > max_projected_s, sends SIGTERM immediately so
        the model can plan a smaller experiment instead.
        """
        stdout_chunks: list[bytes] = []
        stderr_chunks: list[bytes] = []

        async def _drain(stream: asyncio.StreamReader, buf: list[bytes], is_stderr: bool = False) -> None:
            partial = b""
            while True:
                chunk = await stream.read(8192)
                if not chunk:
                    break
                buf.append(chunk)
                if is_stderr:
                    # Parse complete lines for tqdm progress
                    data = partial + chunk
                    lines = data.split(b"\r")  # tqdm uses \r for in-place updates
                    partial = lines[-1]
                    for raw_line in lines[:-1]:
                        line = raw_line.decode(errors="replace").strip()
                        if line:
                            prog = _parse_tqdm_line(line)
                            if prog:
                                _write_progress(handle.run_id, {
                                    **prog,
                                    "wall_s": handle.wall_seconds,
                                })

        async def _eta_check() -> None:
            await asyncio.sleep(eta_check_after_s)
            if handle.process.returncode is not None or handle.cancelled:
                return
            stderr_so_far = b"".join(stderr_chunks).decode(errors="replace")
            eta = _parse_tqdm_eta(stderr_so_far)
            if eta is not None:
                projected = handle.wall_seconds + eta
                logger.info(
                    "Run %s ETA check: %.0fs elapsed, %.0fs remaining → projected %.0fs",
                    handle.run_id, handle.wall_seconds, eta, projected,
                )
                if projected > max_projected_s:
                    logger.warning(
                        "Run %s projected %.0fs > %.0fs limit — killing early",
                        handle.run_id, projected, max_projected_s,
                    )
                    handle.cancel(
                        f"ETA kill: projected {projected:.0f}s > {max_projected_s:.0f}s limit"
                    )
            else:
                logger.debug("Run %s: no tqdm ETA found in stderr after %.0fs", handle.run_id, eta_check_after_s)

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    handle.process.wait(),
                    _drain(handle.process.stdout, stdout_chunks),
                    _drain(handle.process.stderr, stderr_chunks, is_stderr=True),
                    _eta_check(),
                ),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            logger.warning("Run %s hard timeout after %.0fs — cancelling", handle.run_id, timeout_s)
            handle.cancel(f"hard timeout after {timeout_s:.0f}s")
        except Exception as exc:
            logger.warning("Run %s wait error: %s", handle.run_id, exc)

        # Ensure process is dead
        if handle.process.returncode is None:
            try:
                await asyncio.wait_for(handle.process.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                try:
                    handle.process.kill()
                except (ProcessLookupError, OSError):
                    pass

        stdout = b"".join(stdout_chunks).decode(errors="replace")
        stderr = b"".join(stderr_chunks).decode(errors="replace")
        exit_code = handle.process.returncode or -1
        wall_s = handle.wall_seconds

        if stderr.strip():
            for line in stderr.strip().splitlines()[-5:]:
                logger.debug("[%s stderr] %s", handle.run_id, line)

        metrics = _parse_result_json(stdout)

        result = RunResult(
            run_id=handle.run_id,
            exp_number=exp_number,
            metrics=metrics,
            stdout=stdout,
            exit_code=exit_code,
            wall_seconds=wall_s,
            cancelled=handle.cancelled,
            cancel_reason=handle.cancel_reason,
            script_path=handle.script_path,
        )

        self._running.pop(handle.run_id, None)
        _clear_progress(handle.run_id)

        logger.info(
            "Run %s done: exit=%d wall=%.0fs%s served%%=%.1f",
            handle.run_id,
            exit_code,
            wall_s,
            f" [CANCELLED: {handle.cancel_reason}]" if handle.cancelled else "",
            metrics.get("served_pct", 0),
        )
        return result

    def running_run_ids(self) -> list[str]:
        return [rid for rid, h in self._running.items() if h.process.returncode is None]


def _parse_result_json(stdout: str) -> dict:
    """
    Extract the EXPERIMENT_RESULT_JSON: marker from process stdout.
    Returns an empty dict if not found (e.g. script crashed before printing it).
    """
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if line.startswith(_JSON_MARKER):
            raw = line[len(_JSON_MARKER):]
            try:
                return json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.warning("Could not parse result JSON: %s\nRaw: %.200s", exc, raw)
                return {}
    logger.warning("EXPERIMENT_RESULT_JSON marker not found in stdout (script may have crashed)")
    return {}


async def run_batch(
    executor: Executor,
    run_specs: list[SimRun],
    script_paths: dict[str, Path],
    exp_number: int,
    cancel_callback,  # async callable(RunResult) -> list[str] run_ids to cancel
    timeout_s: float = DEFAULT_TIMEOUT_S,
    max_projected_s: float = MAX_PROJECTED_S,
) -> list[RunResult]:
    """
    Execute a batch of sim runs in parallel.

    After each run finishes, cancel_callback fires so the orchestrator can
    preempt still-running sibling runs if the result makes them pointless.
    Hard timeout is enforced per-run inside executor.wait().

    Returns results for all runs (cancelled runs have result.cancelled=True).
    """
    handles: dict[str, RunningProcess] = {}
    for run in run_specs:
        h = await executor.launch(run, script_paths[run.run_id])
        handles[run.run_id] = h

    pending = {
        asyncio.create_task(
            executor.wait(h, run, exp_number, timeout_s, max_projected_s=max_projected_s),
            name=run.run_id,
        ): run
        for run, h in zip(run_specs, handles.values())
    }
    results: list[RunResult] = []

    while pending:
        done, pending_set = await asyncio.wait(
            pending.keys(), return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            run = pending.pop(task)
            result = task.result()
            results.append(result)

            if not result.cancelled and result.exit_code == 0:
                cancel_ids = await cancel_callback(result)
                for cid in cancel_ids:
                    if cid in handles and cid not in [r.run_id for r in results]:
                        executor.cancel_run(cid, f"result-based cancel after {result.run_id} finished")

        pending = {t: r for t, r in pending.items() if t in pending_set}

    return results
