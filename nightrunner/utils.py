import datetime as dt
import io
from datetime import timedelta
from enum import Enum
from io import StringIO

import dateutil.parser
import pandas as pd
from ccdexplorer_fundamentals.cis import MongoTypeTokensTag, MongoTypeTokenAddress
from ccdexplorer_fundamentals.mongodb import (
    Collections,
    CollectionsUtilities,
)
from git import Commit
from pandas import DataFrame
from pymongo import ReplaceOne
from pymongo.collection import Collection
from ccdexplorer_fundamentals.tooter import Tooter, TooterChannel, TooterType


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
    statistics_transaction_fees = "statistics_transaction_fees"
    statistics_bridges_and_dexes = "statistics_bridges_and_dexes"
    statistics_historical_exchange_rates = "statistics_historical_exchange_rates"
    statistics_transaction_types = "statistics_transaction_types"


class Utils:
    def have_we_missed_commits(
        self, analysis: AnalysisType, dates_to_process_count_down: dict
    ):
        if len(dates_to_process_count_down.keys()) > 0:
            self.tooter: Tooter
            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"Nightrunner: Analysis {analysis.value} could not find commit(s) for {list(dates_to_process_count_down.keys())}.",
                notifier_type=TooterType.REQUESTS_ERROR,
            )

    def get_contracts_with_tag_info(self):
        db_to_use = self.mainnet
        contracts_with_tag_info = {
            x["contract"]: MongoTypeTokensTag(**x)
            for x in db_to_use[Collections.pre_render].find(
                {"recurring_type": "contracts_to_tokens"}
            )
        }

        return contracts_with_tag_info

    def get_contracts_for_fungible_tokens(self):
        db: dict[Collections, Collection] = self.mainnet
        contract_to_fungible_token = {}
        token_tags_list = [
            MongoTypeTokensTag(**x)
            for x in db[Collections.tokens_tags].find({"token_type": "fungible"})
        ]
        for token_tag in token_tags_list:
            for contract in token_tag.contracts:
                contract_to_fungible_token[contract] = token_tag

        return contract_to_fungible_token

    def get_fungible_tokens_with_markup(self):
        contracts_with_tag_info = self.get_contracts_with_tag_info()
        db_to_use = self.mainnet
        # get exchange rates
        coll = self.utilities[CollectionsUtilities.exchange_rates]

        exchange_rates = {x["token"]: x for x in coll.find({})}
        contract_to_fungible_token = self.get_contracts_for_fungible_tokens()
        # find fungible tokens
        fungible_contracts = [
            contract
            for contract, token_tag in contracts_with_tag_info.items()
            if token_tag.token_type == "fungible"
        ]
        # now onto the token_addresses
        fungible_tokens_with_markup = {
            x["_id"]: MongoTypeTokenAddress(**x)
            for x in db_to_use[Collections.tokens_token_addresses_v2].find(
                {"contract": {"$in": fungible_contracts}}
            )
        }

        for address in fungible_tokens_with_markup.keys():
            token_address_as_class = fungible_tokens_with_markup[address]
            if not isinstance(token_address_as_class, MongoTypeTokenAddress):
                token_address_as_class = MongoTypeTokenAddress(**MongoTypeTokenAddress)
            corresponding_tokens_tag: MongoTypeTokensTag = contract_to_fungible_token[
                token_address_as_class.contract
            ]
            if not isinstance(corresponding_tokens_tag, MongoTypeTokensTag):
                corresponding_tokens_tag = MongoTypeTokensTag(
                    **corresponding_tokens_tag
                )
            if token_address_as_class.contract in contracts_with_tag_info.keys():
                token_address_as_class.tag_information = contracts_with_tag_info[
                    token_address_as_class.contract
                ]
            get_price_from = corresponding_tokens_tag.get_price_from

            if get_price_from in exchange_rates:
                token_address_as_class.exchange_rate = exchange_rates[get_price_from][
                    "rate"
                ]
            else:
                token_address_as_class.exchange_rate = 0

            fungible_tokens_with_markup[address] = token_address_as_class

        return fungible_tokens_with_markup

    def get_historical_rates(self):

        exchange_rates_by_currency = dict({})
        result = self.utilities[CollectionsUtilities.exchange_rates_historical].find({})

        for x in result:
            if not exchange_rates_by_currency.get(x["token"]):
                exchange_rates_by_currency[x["token"]] = {}
                exchange_rates_by_currency[x["token"]][x["date"]] = x["rate"]
            else:
                exchange_rates_by_currency[x["token"]][x["date"]] = x["rate"]

        return exchange_rates_by_currency

    def get_all_blocks_last_height(self):
        result = list(
            self.mainnet[Collections.blocks_per_day].find(
                filter={},
                projection={
                    "_id": 0,
                    "date": 1,
                    "height_for_last_block": 1,
                    "slot_time_for_last_block": 1,
                },
            )
        )
        block_end_of_day_dict = {x["height_for_last_block"]: x["date"] for x in result}
        heights = list(block_end_of_day_dict.keys())
        return heights, block_end_of_day_dict

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

    def get_usecases_complete(self):
        self.utilities: dict[CollectionsUtilities, Collection]
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]

        usecases_dict = {}
        result = self.utilities[CollectionsUtilities.usecases].find({})
        for usecase in list(result):
            usecase_addresses = list(
                self.mainnet[Collections.usecases].find(
                    {"usecase_id": usecase["usecase_id"]}
                )
            )
            mainnet_usecase_addresses = [
                x["account_address"]
                for x in usecase_addresses
                if "account_address" in x
            ]

            mainnet_usecase_addresses.extend(
                [
                    x["contract_address"]
                    for x in usecase_addresses
                    if "contract_address" in x
                ]
            )

            # testnet_usecase_addresses = [
            #     x["account_address"]
            #     for x in self.testnet[Collections.usecases].find(
            #         {"usecase_id": usecase["usecase_id"]}
            #     )
            # ]
            usecases_dict[usecase["usecase_id"]] = {
                "mainnet_addresses": mainnet_usecase_addresses,
                # "testnet_addresses": testnet_usecase_addresses,
            }
        usecases_dict["all"] = {}
        return usecases_dict

    def get_projects_complete(self):
        self.utilities: dict[CollectionsUtilities, Collection]
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]

        projects_dict = {}
        result = self.utilities[CollectionsUtilities.projects].find({})
        for project in list(result):
            project_addresses = list(
                self.mainnet[Collections.projects].find(
                    {"project_id": project["project_id"]}
                )
            )
            mainnet_project_addresses = [
                x["account_address"]
                for x in project_addresses
                if "account_address" in x
            ]

            mainnet_project_addresses.extend(
                [
                    x["contract_address"]
                    for x in project_addresses
                    if "contract_address" in x
                ]
            )

            # testnet_usecase_addresses = [
            #     x["account_address"]
            #     for x in self.testnet[Collections.usecases].find(
            #         {"usecase_id": usecase["usecase_id"]}
            #     )
            # ]
            projects_dict[project["project_id"]] = {
                "mainnet_addresses": mainnet_project_addresses,
                # "testnet_addresses": testnet_usecase_addresses,
            }
        projects_dict["all"] = {}
        return projects_dict

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

    def get_all_dates_with_info(self) -> list[str]:
        return {x["date"]: x for x in self.mainnet[Collections.blocks_per_day].find({})}

    def get_start_end_block_from_date(self, date: str) -> str:
        self.mainnet: dict[Collections, Collection]
        result = self.mainnet[Collections.blocks_per_day].find_one({"date": date})
        if result:
            return result["height_for_first_block"], result["height_for_last_block"]
        else:
            # if it's today...
            # set last block to the last block we can find
            height_result = self.mainnet[Collections.blocks].find_one(
                {}, sort=[("height", -1)]
            )
            end_height = height_result["height"]

            # find yesterday's date and find the last block there
            yesterday_date = (
                f"{(dateutil.parser.parse(date) - timedelta(days=1)):%Y-%m-%d}"
            )
            result = self.mainnet[Collections.blocks_per_day].find_one(
                {"date": yesterday_date}
            )
            start_height = result["height_for_last_block"] + 1

            return start_height, end_height

    def get_hash_from_date(self, date: str) -> str:
        self.mainnet: dict[Collections, Collection]
        result = self.mainnet[Collections.blocks_per_day].find_one({"date": date})
        return result["hash_for_last_block"]

    def generate_dates_from_start_date_until_end_date(self, start: str, end: str):
        start_date = dateutil.parser.parse(start)
        end_date = dateutil.parser.parse(end)
        date_range = []

        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        return date_range

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
            for x in self.mainnet[Collections.statistics]
            .find({"type": analysis.value})
            .sort({"date": -1})
        ]

    def get_all_dates_for_usecase(
        self, analysis: AnalysisType, usecase_id: str
    ) -> list[str]:
        pipeline = [
            {"$match": {"type": analysis.value}},
            {"$match": {"usecase": usecase_id}},
            {"$project": {"_id": 0, "date": 1}},
        ]
        return [
            x["date"] for x in self.mainnet[Collections.statistics].aggregate(pipeline)
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
        if not result:
            exit(f"No statistics_network_summary for {date}")
        else:
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
        # unprocessed_day = "2024-05-07"
        # first check whether we need re rerun all dates
        if rerun_state:
            dates_to_process = all_dates
        # check if we have already done the unprocessed day
        else:
            dates_to_process = (
                [] if unprocessed_day in all_dates_for_analysis else [unprocessed_day]
            )

        return dates_to_process

    def find_dates_to_process_for_nightly_statistics(
        self, analysis: AnalysisType
    ) -> list[str]:
        # all days, including current day 1 sec after midnight
        all_dates = self.get_all_dates()
        # from a Mongo helper
        rerun_state = self.get_analysis_rerun_state(analysis)
        # all dates present for this analysis
        all_dates_for_analysis = self.get_all_dates_for_analysis(analysis)

        # this is the new day to be processed
        # for normal days, we should only proceed to process this day
        # when the unprocessed day is the last day in all_dates
        # as that indicates that all_accounts has finished.
        unprocessed_day = self.get_unprocessed_day()
        # first check whether we need re rerun all dates
        if rerun_state:
            dates_to_process = all_dates
        # check if we have already done the unprocessed day
        else:
            dates_to_process = list(set(all_dates) - set(all_dates_for_analysis))
            if unprocessed_day != all_dates[-1]:
                dates_to_process = list(
                    set(dates_to_process) - set(list(unprocessed_day))
                )

        return dates_to_process

    def find_dates_to_process_for_project(
        self, analysis: AnalysisType, usecase_id: str
    ) -> list[str]:
        all_dates = self.get_all_dates()
        dates_to_process = all_dates
        dates_to_process.append(
            f"{dt.datetime.now().astimezone(dt.timezone.utc):%Y-%m-%d}"
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
