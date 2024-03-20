from ccdefundamentals.mongodb import (
    MongoDB,
    Collections,
    CollectionsUtilities,
    MongoMotor,
)
from pymongo.collection import Collection
from enum import Enum
import dateutil.parser
from git import Commit
import io
from pymongo import ReplaceOne
import pandas as pd
from datetime import timedelta
import datetime as dt
from pandas import DataFrame
from io import StringIO


class PreRender(Enum):
    """
    PreRender are slow queries that are being processed on a regular schedule
    and displayed in a frontend.
    """

    tps_table = "tps_table"
    accounts_table = "accounts_table"


class AnalysisType(Enum):
    """
    Analyses are performed on a nightly schedule and are displyaed
    on the statistics page.
    """

    statistics_daily_holders = "statistics_daily_holders"
    statistics_daily_limits = "statistics_daily_limits"
    statistics_network_summary = "statistics_network_summary"
    statistics_classified_pools = "statistics_classified_pools"
    statistics_microccd = "statistics_microccd"
    statistics_ccd_classified = "statistics_ccd_classified"
    statistics_exchange_wallets = "statistics_exchange_wallets"
    statistics_ccd_volume = "statistics_ccd_volume"
    statistics_release_amounts = "statistics_release_amounts"
    statistics_mongo_transactions = "statistics_mongo_transactions"
    statistics_network_activity = "statistics_network_activity"


class Utils:
    def get_exchanges(self):
        self.utilities: dict[CollectionsUtilities, Collection]
        result = self.utilities[CollectionsUtilities.labeled_accounts].find(
            {"label_group": "exchanges"}
        )

        exchanges = {}
        for r in result:
            key = r["label"].lower().split(" ")[0]
            current_list: list[str] = exchanges.get(key, [])
            current_list.append(r["_id"][:29])
            exchanges[key] = current_list
        return exchanges

    def get_exchanges_as_list(self):
        self.utilities: dict[CollectionsUtilities, Collection]
        result = self.utilities[CollectionsUtilities.labeled_accounts].find(
            {"label_group": "exchanges"}
        )

        exchanges = []
        for r in result:
            exchanges.append(r["_id"][:29])
        return exchanges

    def get_usecases(self):
        self.utilities: dict[CollectionsUtilities, Collection]
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        usecase_addresses = {}
        result = self.utilities[CollectionsUtilities.usecases].find({})
        for usecase in list(result):
            mainnet_usecase_addresses = self.mainnet[Collections.usecases].find(
                {"usecase_id": usecase["usecase_id"]}
            )
            for address in mainnet_usecase_addresses:
                if address["type"] == "account_address":
                    usecase_addresses[address["account_address"]] = usecase[
                        "display_name"
                    ]

        return usecase_addresses

    def write_queue_to_collection(
        self, queue: list[ReplaceOne], analysis: AnalysisType
    ):
        self.mainnet: dict[Collections, Collection]

        if len(queue) > 0:
            _ = self.mainnet[Collections.statistics].bulk_write(queue)

        result = self.mainnet[Collections.helpers].find_one({"_id": "statistics_rerun"})
        # set rerun to False
        result[analysis.value] = False
        _ = self.mainnet[Collections.helpers].bulk_write(
            [
                ReplaceOne(
                    {"_id": "statistics_rerun"},
                    replacement=result,
                    upsert=True,
                )
            ]
        )

    def write_queue_to_prerender_collection(
        self, queue: list[ReplaceOne], prerender: PreRender
    ):
        self.mainnet: dict[Collections, Collection]

        if len(queue) > 0:
            _ = self.mainnet[Collections.pre_render].bulk_write(queue)

        result = self.mainnet[Collections.helpers].find_one({"_id": "prerender_runs"})
        # set rerun to False
        if not result:
            result = {}

        result[prerender.value] = dt.datetime.now().astimezone(dt.timezone.utc)
        _ = self.mainnet[Collections.helpers].bulk_write(
            [
                ReplaceOne(
                    {"_id": "prerender_runs"},
                    replacement=result,
                    upsert=True,
                )
            ]
        )

    def get_all_dates(self) -> list[str]:
        return [x["date"] for x in self.mainnet[Collections.blocks_per_day].find({})]

    def get_start_end_block_from_date(self, date: str) -> str:
        self.mainnet: dict[Collections, Collection]
        result = self.mainnet[Collections.blocks_per_day].find_one({"date": date})
        return result["height_for_first_block"], result["height_for_last_block"]

    def get_hash_from_date(self, date: str) -> str:
        self.mainnet: dict[Collections, Collection]
        result = self.mainnet[Collections.blocks_per_day].find_one({"date": date})
        return result["hash_for_last_block"]

    def generate_dates_from_start_until_date(self, date: str):
        start_date = dateutil.parser.parse("2021-06-09")
        end_date = dateutil.parser.parse(date)
        date_range = []

        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        return date_range

    def get_all_dates_for_analysis(self, analysis: AnalysisType) -> list[str]:
        return [
            x["date"]
            for x in self.mainnet[Collections.statistics].find({"type": analysis.value})
        ]

    def get_analysis_rerun_state(self, analysis: AnalysisType) -> bool:
        result = self.mainnet[Collections.helpers].find_one({"_id": "statistics_rerun"})
        if result:
            if analysis.value in result:
                return result[analysis.value]
            else:
                return True
        else:
            return True

    def get_total_amount_from_summary_for_date(self, date: str) -> list[str]:
        result = self.mainnet[Collections.statistics].find_one(
            {"$and": [{"type": "statistics_network_summary"}, {"date": date}]}
        )
        return result["total_amount"]

    def get_unprocessed_day(self) -> str:
        result = self.mainnet[Collections.helpers].find_one(
            {"_id": "last_known_nightly_accounts"}
        )
        return result["date"]

    def find_dates_to_process(self, analysis: AnalysisType) -> list[str]:
        all_dates = self.get_all_dates()
        rerun_state = self.get_analysis_rerun_state(analysis)
        all_dates_for_analysis = self.get_all_dates_for_analysis(analysis)

        # this is the new day to be processed
        unprocessed_day = self.get_unprocessed_day()

        # first check whether we need re rerun all dates
        if rerun_state:
            dates_to_process = all_dates
        # check if we have already done the unprocessed day
        else:
            dates_to_process = (
                [] if unprocessed_day in all_dates_for_analysis else [unprocessed_day]
            )

        return dates_to_process

    def get_df_from_git(self, commit: Commit) -> DataFrame:
        targetfile = commit.tree / "accounts.csv"
        with io.BytesIO(targetfile.data_stream.read()) as f:
            my_file = f.read().decode("utf-8")
        data = StringIO(my_file)
        df = pd.read_csv(data, low_memory=False)

        return df

    def get_date_from_git(self, commit: Commit) -> str:
        timestamp = dateutil.parser.parse(commit.message)
        d_date = f"{timestamp:%Y-%m-%d}"
        return d_date
