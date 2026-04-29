"""
Autonomous experiment orchestrator — the main loop.

Run:
    export OPENROUTER_API_KEY=sk-or-...
    caffeinate -i python3 scripts/experimenter/orchestrator.py

Options:
    --budget-usd 10.0         Hard spend cap (default $10)
    --max-experiments 50      Max experiment groups to run (default 50)
    --max-parallel 2          Max concurrent sim arms (default 2, M4 16GB sweet spot)
    --timeout-minutes 60      Per-arm timeout (default 60 min)
    --model minimax/minimax-m2  LLM model via OpenRouter (default minimax/minimax-m2)
    --dry-run                 Plan and print next experiment without executing
    --reset-state             Delete state.json and start from Exp 76

Flow per iteration:
    1. plan_next_group()  → ExperimentPlan (JSON from LLM)
    2. Generate scripts   → scripts/experimenter/generated/expN_armX.py
    3. For each Stage in plan.stages:
         a. Launch up to max_parallel arm subprocesses
         b. As each arm finishes: call evaluate_kills() → maybe SIGTERM siblings
         c. Append partial result to RESULTS.md
    4. interpret_group()  → finding paragraph appended to RESULTS.md
    5. Update state.json, loop
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

from experimenter.budget import Budget, State
from experimenter.executor import Executor, run_batch
from experimenter.llm import LLMClient
from experimenter.models import RunResult, SimRun, ExperimentPlan
from experimenter.planner import interpret_experiment, plan_next_experiment
from experimenter.reporter import append_run_result, append_group_summary
from experimenter.template import generate_script

GENERATED_DIR = Path(__file__).parent / "generated"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("orchestrator")


# ---------------------------------------------------------------------------
# Core loop
# ---------------------------------------------------------------------------

async def run_experiment(
    plan: ExperimentPlan,
    client: LLMClient,
    budget: Budget,
    executor: Executor,
    timeout_s: float,
    max_projected_s: float,
    dry_run: bool,
) -> dict[str, RunResult]:
    """Execute one ExperimentPlan end-to-end. Returns all run results."""

    # Generate scripts for all runs upfront
    script_paths: dict[str, Path] = {}
    for run in plan.runs:
        sp = generate_script(run, plan.exp_number, GENERATED_DIR)
        script_paths[run.run_id] = sp
        logger.info("Generated script: %s", sp.name)

    if dry_run:
        logger.info("DRY RUN — skipping execution for Exp%d", plan.exp_number)
        return {}

    all_results: dict[str, RunResult] = {}
    is_first_written = False

    for batch_idx, batch in enumerate(plan.batches):
        run_specs = [plan.run_by_id(rid) for rid in batch.run_ids]
        run_specs = [r for r in run_specs if r is not None]
        run_specs = [r for r in run_specs if r.run_id in script_paths]
        run_specs = [r for r in run_specs if r.run_id not in all_results]

        if not run_specs:
            logger.warning("Batch %d: no runs to execute (all cancelled or missing)", batch_idx)
            continue

        logger.info(
            "Batch %d/%d: launching %s in parallel",
            batch_idx + 1, len(plan.batches),
            [r.run_id for r in run_specs],
        )

        async def cancel_callback(completed: RunResult) -> list[str]:
            # Result-based cancellation disabled for now — every run runs to completion.
            # The LLM reviews all results afterward and decides what to explore next.
            # Hard timeout (--timeout-minutes) still applies as a safety net.
            return []

        batch_results = await run_batch(
            executor=executor,
            run_specs=run_specs,
            script_paths=script_paths,
            exp_number=plan.exp_number,
            cancel_callback=cancel_callback,
            timeout_s=timeout_s,
            max_projected_s=max_projected_s,
        )

        for result in batch_results:
            all_results[result.run_id] = result
            run_spec = plan.run_by_id(result.run_id)
            if run_spec:
                append_run_result(plan, run_spec, result, is_first_result=not is_first_written)
                is_first_written = True
            budget.finish_arm(killed=result.cancelled)
            budget.save()

    return all_results


async def main_loop(
    budget: Budget,
    client: LLMClient,
    max_parallel: int,
    timeout_s: float,
    max_projected_s: float,
    dry_run: bool,
) -> None:
    async with Executor(max_parallel=max_parallel) as executor:
        while not budget.exhausted:
            exp_number = budget.state.next_exp_number
            logger.info("=" * 60)
            logger.info("Planning Exp%d  |  %s", exp_number, budget.state.summary())

            # 1. Plan — adaptive, reviews full RESULTS.md each time
            try:
                plan, usage = await plan_next_experiment(
                    client,
                    exp_number,
                    state_summary=budget.state.summary(),
                )
            except Exception as exc:
                logger.error("Planning failed: %s — retrying in 30s", exc)
                await asyncio.sleep(30)
                continue

            budget.record(usage)
            budget.save()

            logger.info("Hypothesis: %s", plan.hypothesis)
            logger.info("Rationale: %s", plan.rationale)
            logger.info("Decision tree: %s", plan.decision_tree)
            logger.info(
                "Runs: %s",
                [(r.run_id, r.description[:60]) for r in plan.runs],
            )
            logger.info("Batches: %s", [b.run_ids for b in plan.batches])

            # 2. Execute
            try:
                all_results = await run_experiment(
                    plan, client, budget, executor, timeout_s, max_projected_s, dry_run
                )
            except Exception as exc:
                logger.error("Experiment execution failed: %s", exc, exc_info=True)
                budget.finish_experiment()
                budget.save()
                continue

            if dry_run:
                budget.finish_experiment()
                budget.save()
                logger.info("Dry run complete. Exiting.")
                break

            if not all_results:
                logger.warning("No results from Exp%d — skipping interpretation", exp_number)
                budget.finish_experiment()
                budget.save()
                continue

            # 3. Interpret — appends finding to RESULTS.md
            try:
                interpretation, usage = await interpret_experiment(client, plan, all_results, exp_number)
                budget.record(usage)
                budget.save()
                append_group_summary(plan, all_results, interpretation)
            except Exception as exc:
                logger.error("Interpretation failed: %s", exc)

            budget.finish_experiment()
            budget.save()

            logger.info("Exp%d complete. %s", exp_number, budget.state.summary())

            await asyncio.sleep(5)

    logger.info("Experimenter stopped. Final state: %s", budget.state.summary())


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Autonomous robotaxi sim experimenter",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--budget-usd", type=float, default=10.0, help="Hard LLM spend cap in USD")
    p.add_argument("--max-experiments", type=int, default=50, help="Max experiment groups")
    p.add_argument("--max-parallel", type=int, default=2, help="Max concurrent sim arms")
    p.add_argument(
        "--timeout-minutes", type=float, default=20.0, help="Per-arm hard timeout in minutes"
    )
    p.add_argument(
        "--max-run-minutes", type=float, default=20.0,
        help="Kill run early if tqdm projects runtime beyond this many minutes",
    )
    p.add_argument(
        "--model",
        default="minimax/minimax-m2.7",
        help="LLM model via OpenRouter (e.g. minimax/minimax-m2, anthropic/claude-sonnet-4-5)",
    )
    p.add_argument(
        "--api-key",
        default=None,
        help="OpenRouter API key (or set OPENROUTER_API_KEY env var)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan and generate scripts but do not run sims",
    )
    p.add_argument(
        "--reset-state",
        action="store_true",
        help="Delete state.json and start fresh from Exp 76",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    state_path = Path(__file__).parent / "state.json"
    if args.reset_state and state_path.exists():
        state_path.unlink()
        logger.info("Deleted state.json — starting fresh")

    GENERATED_DIR.mkdir(parents=True, exist_ok=True)

    state = State.load(state_path)
    budget = Budget(
        max_usd=args.budget_usd,
        max_experiments=args.max_experiments,
        state=state,
    )

    client = LLMClient(
        api_key=args.api_key or os.environ.get("OPENROUTER_API_KEY"),
        model=args.model,
    )

    logger.info(
        "Starting experimenter | model=%s | budget=$%.2f | max_exp=%d | max_parallel=%d%s",
        args.model,
        args.budget_usd,
        args.max_experiments,
        args.max_parallel,
        " [DRY RUN]" if args.dry_run else "",
    )
    logger.info("Current state: %s", state.summary())

    asyncio.run(
        main_loop(
            budget=budget,
            client=client,
            max_parallel=args.max_parallel,
            timeout_s=args.timeout_minutes * 60,
            max_projected_s=args.max_run_minutes * 60,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
