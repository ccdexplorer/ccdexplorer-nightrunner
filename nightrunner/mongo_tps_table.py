from git import Repo
from rich.console import Console
from .utils import Utils, PreRender
from pymongo import ReplaceOne
from ccdexplorer_fundamentals.mongodb import Collections
from pymongo.collection import Collection
import datetime as dt

console = Console()


class MongoTPSTable(Utils):

    def perform_statistics_mongo_tps_table(self):
        self.repo: Repo
        self.mainnet: dict[Collections, Collection]
        prerender = PreRender.tps_table
        console.log(f"Start {prerender.value}...")
        now = dt.datetime.now().astimezone(dt.timezone.utc)
        # console.log(f"{dt.datetime.now()} | STATE now | {now}")
        start_of_today = dt.datetime(
            year=now.year, month=now.month, day=now.day, hour=0, minute=0, second=0
        )
        minus_1h = now - dt.timedelta(hours=1)
        minus_24h = now - dt.timedelta(hours=24)
        minus_1w = now - dt.timedelta(days=7)
        minus_1m = now - dt.timedelta(days=30)
        minus_1y = now - dt.timedelta(days=365)
        chain_start = dt.datetime(2021, 6, 9, 9, 0, 0)

        state_response = {}
        # self.mainnet: Dict[Collections, AsyncIOMotorCollection]

        state_response["hour_count_at"] = self.mainnet[
            Collections.transactions
        ].count_documents({"block_info.slot_time": {"$gte": minus_1h}})
        state_response["day_count_at"] = self.mainnet[
            Collections.transactions
        ].count_documents({"block_info.slot_time": {"$gte": minus_24h}})
        console.log(f"Start {prerender.value}...2 done.")
        state_response["week_count_at"] = self.mainnet[
            Collections.transactions
        ].count_documents({"block_info.slot_time": {"$gte": minus_1w}})
        state_response["month_count_at"] = self.mainnet[
            Collections.transactions
        ].count_documents({"block_info.slot_time": {"$gte": minus_1m}})
        console.log(f"Start {prerender.value}...4 done.")
        state_response["year_count_at"] = self.mainnet[
            Collections.transactions
        ].count_documents({"block_info.slot_time": {"$gte": minus_1y}})
        console.log(f"Start {prerender.value}...all done.")
        state_response["timestamp"] = now

        state_response["hour_tps"] = (
            f'{(state_response["hour_count_at"]   / (now - minus_1h).total_seconds()):,.3f}'
        )
        state_response["day_tps"] = (
            f'{(state_response["day_count_at"]    / (now - minus_24h).total_seconds()):,.3f}'
        )
        state_response["week_tps"] = (
            f'{(state_response["week_count_at"]    / (now - minus_1w).total_seconds()):,.3f}'
        )
        state_response["month_tps"] = (
            f'{(state_response["month_count_at"]    / (now - minus_1m).total_seconds()):,.3f}'
        )
        state_response["year_tps"] = (
            f'{(state_response["year_count_at"]    / (now - minus_1y).total_seconds()):,.3f}'
        )

        try:
            hour_factor = (
                state_response["hour_count_at"]
                * (now - minus_24h).total_seconds()
                / (now - minus_1h).total_seconds()
                / state_response["day_count_at"]
            )
        except:
            hour_factor = 0

        try:
            day_factor = (
                state_response["day_count_at"]
                * (now - minus_1w).total_seconds()
                / (now - minus_24h).total_seconds()
                / state_response["week_count_at"]
            )
        except:
            day_factor = 0

        try:
            week_factor = (
                state_response["week_count_at"]
                * (now - minus_1m).total_seconds()
                / (now - minus_1w).total_seconds()
                / state_response["month_count_at"]
            )
        except:
            week_factor = 0

        try:
            month_factor = (
                state_response["month_count_at"]
                * (now - minus_1y).total_seconds()
                / (now - minus_1m).total_seconds()
                / state_response["year_count_at"]
            )
        except:
            month_factor = 0

        state_response["hour_count_at"] = f"{state_response['hour_count_at']:,.0f}"
        state_response["day_count_at"] = f"{state_response['day_count_at']:,.0f}"
        state_response["week_count_at"] = f"{state_response['week_count_at']:,.0f}"
        state_response["month_count_at"] = f"{state_response['month_count_at']:,.0f}"
        state_response["year_count_at"] = f"{state_response['year_count_at']:,.0f}"

        state_response["hour_f_at"] = f"{(hour_factor-1)*100:.0f}%"
        state_response["day_f_at"] = f"{(day_factor-1)*100:.0f}%"
        state_response["week_f_at"] = f"{(week_factor-1)*100:.0f}%"
        state_response["month_f_at"] = f"{(month_factor-1)*100:.0f}%"

        # state_response["all_count_at"] = f"{state_response['all_count_at']:,.0f}"
        state_response["updated"] = True
        state_response["type"] = prerender.value

        queue = []
        queue.append(
            ReplaceOne(
                {"_id": prerender.value},
                replacement=state_response,
                upsert=True,
            )
        )
        console.log(f"Start {prerender.value}...done.")
        self.write_queue_to_prerender_collection(queue, prerender)
