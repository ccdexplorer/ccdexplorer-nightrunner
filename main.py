from __future__ import annotations

from asyncio import run

from nightrunner import NightRunner

from apscheduler import AsyncScheduler
from apscheduler.triggers.interval import IntervalTrigger

from ccdexplorer_fundamentals.GRPCClient import GRPCClient

from ccdexplorer_fundamentals.GRPCClient.CCD_Types import *  # noqa: F403
from ccdexplorer_fundamentals.tooter import Tooter
from ccdexplorer_fundamentals.mongodb import (
    MongoDB,
    MongoMotor,
)
from env import *  # noqa: F403
from rich.console import Console

console = Console()

grpcclient = GRPCClient()
tooter = Tooter()
mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter)


async def main():
    night_runner = NightRunner(grpcclient, tooter, mongodb, motormongo)
    async with AsyncScheduler() as scheduler:
        await scheduler.add_schedule(
            night_runner.repo_pull, IntervalTrigger(seconds=60)
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_daily_holders,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_daily_limits,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_network,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_classified_pools,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_microccd,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_ccd_classified,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_exchange_wallets,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_release_amounts,
            IntervalTrigger(seconds=5 * 60),
        )
        # fmt: off
        await scheduler.add_schedule(
            night_runner.perform_statistics_mongo_transactions, IntervalTrigger(seconds=5 * 60))

        await scheduler.add_schedule(
            night_runner.perform_statistics_ccd_volume, IntervalTrigger(seconds=5 * 60)
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_transaction_fees,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_historical_exchange_rates,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_bridges_and_dexes,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.add_schedule(
            night_runner.perform_statistics_transaction_types,
            IntervalTrigger(seconds=5 * 60),
        )
        await scheduler.run_until_stopped()
        pass


if __name__ == "__main__":
    run(main())
