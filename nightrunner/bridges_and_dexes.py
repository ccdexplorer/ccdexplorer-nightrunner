import datetime as dt
import math
from bisect import bisect_right
from enum import Enum
from typing import Optional
import dateutil.parser
from datetime import timedelta
import pandas as pd
from ccdexplorer_fundamentals.cis import MongoTypeLoggedEvent, MongoTypeTokenAddress
from ccdexplorer_fundamentals.mongodb import Collections
from pydantic import BaseModel, ConfigDict
from pymongo import ASCENDING, ReplaceOne
from rich.console import Console
from rich import print

from .utils import AnalysisType, Utils

console = Console()


class ReportingActionType(str, Enum):
    deposit = "Deposit"
    swap = "Swap"
    withdraw = "Withdraw"
    mint = "Mint"
    burn = "Burn"
    none = "None"


class ReportingSubject(str, Enum):
    Tricorn = "Tricorn"
    Concordex = "Concordex"
    Arabella = "Arabella"


class ClassifiedTransaction(BaseModel):
    tx_hash: str
    logged_events: list[MongoTypeLoggedEvent]
    block_height: int
    addresses: Optional[set] = None
    date: Optional[str] = None
    action_type: Optional[ReportingActionType] = None
    logged_event_index_for_action: Optional[int] = None


class ReportingUnit(BaseModel):
    tx_hash: str
    date: str
    fungible_token: str
    amount_in_local_currency: float
    amount_in_usd: float
    action_type: str


class ReportingAddresses(BaseModel):
    tx_hash: str
    date: str
    addresses: str


class ReportingOutput(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    txs_by_action_type: dict  # [ReportingActionType: list[ClassifiedTransaction]]
    output: list
    df_accounts: dict
    df_raw: pd.DataFrame
    df_output_action_types: dict  # [str:pd.DataFrame]
    df_output_fungible_token: dict  # [str:pd.DataFrame]


class ReportingOutputV2(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    action_types_for_day: list[dict]
    fungible_tokens_for_day: list[dict]
    addresses: list[str]


class BridgesAndDexes(Utils):

    def get_txs_for_impacted_address_cdex(self, d_date: str):
        pipeline = [
            {"$match": {"impacted_address_canonical": "<9363,0>", "date": d_date}},
            {"$project": {"_id": 0, "tx_hash": 1}},
        ]

        result = self.mainnet[Collections.impacted_addresses].aggregate(pipeline)
        return [x["tx_hash"] for x in result]

    def get_txs_for_impacted_address_arabella(self, d_date: str):
        pipeline = [
            {"$match": {"impacted_address_canonical": "<9337,0>", "date": d_date}},
            {"$project": {"_id": 0, "tx_hash": 1}},
        ]

        result = self.mainnet[Collections.impacted_addresses].aggregate(pipeline)
        return [x["tx_hash"] for x in result]

    def get_txs_for_impacted_address_tricorn(self, d_date: str):
        pipeline = [
            {"$match": {"impacted_address_canonical": "<9427,0>", "date": d_date}},
            {"$project": {"_id": 0, "tx_hash": 1}},
        ]

        result = self.mainnet[Collections.impacted_addresses].aggregate(pipeline)
        bridge_txs = set(x["tx_hash"] for x in result)

        pipeline = [
            {
                "$match": {
                    "impacted_address_canonical": {
                        "$in": ["<9428,0>", "<9429,0>", "<9430,0>"],
                    },
                    "date": d_date,
                }
            },
            {"$project": {"_id": 0, "tx_hash": 1}},
        ]

        result = self.mainnet[Collections.impacted_addresses].aggregate(pipeline)
        wrong_txs = set(x["tx_hash"] for x in result)
        return list(bridge_txs - wrong_txs)

    def address_exists_and_is_account(self, address: str | None):
        if not address:
            return False
        else:
            if len(address) > 20:
                return True
            else:
                return False

    def classify_tx_as_swap_or_withdraw(self, classified_tx: ClassifiedTransaction):
        log_0_to = classified_tx.logged_events[0].result.get("to_address")
        log_0_from = classified_tx.logged_events[0].result.get("from_address")
        log_1_to = classified_tx.logged_events[1].result.get("to_address")

        if self.address_exists_and_is_account(
            log_0_to
        ) and self.address_exists_and_is_account(log_1_to):
            classified_tx.action_type = ReportingActionType.withdraw
        else:
            if (
                classified_tx.logged_events[0].event_type == "mint_event"
                and self.address_exists_and_is_account(log_1_to)
                or self.address_exists_and_is_account(log_0_from)
                and self.address_exists_and_is_account(log_1_to)
                or self.address_exists_and_is_account(log_0_from)
                and classified_tx.logged_events[1].event_type == "burn_event"
            ):
                classified_tx.action_type = ReportingActionType.swap
        return classified_tx

    def find_date_for_height(heights, block_end_of_day_dict, height):
        found_index = bisect_right(heights, height)
        # meaning it's today...
        if found_index == len(heights):
            return f"{dt.datetime.now():%Y-%m-%d}"
        else:
            return block_end_of_day_dict[heights[found_index]]

    def find_date_and_date_str_for_height(self, height):
        found_index = bisect_right(self.heights, height)
        # meaning it's today...
        if found_index == len(self.heights):
            return f"{dt.datetime.now().astimezone(dt.timezone.utc):%Y-%m-%d}"
        else:
            return self.block_end_of_day_dict[self.heights[found_index]]

    def add_date_to_tx(self, tx: ClassifiedTransaction):
        date = self.find_date_and_date_str_for_height(tx.block_height)
        tx.date = date
        return tx

    def process_txs_for_analytics(self, txs_by_action_type):
        output = []
        accounts = []

        for action_type in ReportingActionType:
            txs_per_action_type = txs_by_action_type[action_type]
            for tx in txs_per_action_type:
                tx: ClassifiedTransaction
                if tx.logged_event_index_for_action:
                    r = tx.logged_events[tx.logged_event_index_for_action]
                else:
                    r = tx.logged_events[0]
                output, accounts = self.append_logged_event(
                    output,
                    accounts,
                    action_type,
                    tx,
                    r,
                )

                if tx.action_type == ReportingActionType.withdraw:
                    r = tx.logged_events[1]
                    output, accounts = self.append_logged_event(
                        output,
                        accounts,
                        action_type,
                        tx,
                        r,
                    )

        if len(output) > 0:
            df = pd.DataFrame([x.model_dump() for x in output])
            df["date"] = pd.to_datetime(df["date"])

            df_accounts = {}
            action_types_for_day = (
                df.groupby([pd.Grouper(key="date", axis=0, freq="D"), "action_type"])
                .sum()
                .reset_index()
            )[["action_type", "amount_in_usd"]].to_dict("records")
            fungible_tokens_for_day = (
                df.groupby([pd.Grouper(key="date", axis=0, freq="D"), "fungible_token"])
                .sum()
                .reset_index()
            )[["fungible_token", "amount_in_usd"]].to_dict("records")
        else:
            action_types_for_day = []
            fungible_tokens_for_day = []

        if len(accounts) > 0:
            df_a = pd.DataFrame([x.model_dump() for x in accounts])
            df_a["date"] = pd.to_datetime(df_a["date"])

            df_accounts = (
                df_a.groupby([pd.Grouper(key="date", axis=0, freq="D")])["addresses"]
                .agg(",".join)
                .reset_index()
            )
            df_accounts["addresses"] = df_accounts["addresses"].str.split(",")
            df_accounts["addresses"] = df_accounts["addresses"].apply(set)
            addresses = df_accounts.to_dict("records")[0]["addresses"]
        else:
            addresses = []
            # df.groupby('col')['val'].agg('-'.join)
        return ReportingOutputV2(
            action_types_for_day=action_types_for_day,
            fungible_tokens_for_day=fungible_tokens_for_day,
            addresses=addresses,
        )

    def append_logged_event(
        self,
        output: list[ReportingUnit],
        accounts: list[ReportingAddresses],
        action_type: ReportingActionType,
        tx: ClassifiedTransaction,
        r: MongoTypeLoggedEvent,
    ):
        token_address_with_markup: MongoTypeTokenAddress = (
            self.fungible_tokens_with_markup.get(r.token_address)
        )
        if not token_address_with_markup:
            console.log(
                f"Can't find {r.token_address} in token_addresses_with_markup!!!"
            )
            return output, accounts

        get_price_from = token_address_with_markup.tag_information.get_price_from
        fungible_token = token_address_with_markup.tag_information.token_tag_id
        if int(r.result["token_amount"]) > 0:
            real_token_amount = int(r.result["token_amount"]) * (
                math.pow(10, -token_address_with_markup.tag_information.decimals)
            )

            if self.historical_exchange_rates.get(get_price_from):
                if tx.date in self.historical_exchange_rates[get_price_from]:
                    exchange_rate_for_day = self.historical_exchange_rates[
                        get_price_from
                    ][tx.date]
                    dd = {
                        "tx_hash": tx.tx_hash,
                        "date": tx.date,
                        "fungible_token": fungible_token,
                        "amount_in_local_currency": real_token_amount,
                        "amount_in_usd": real_token_amount * exchange_rate_for_day,
                        "action_type": action_type.value,
                    }
                    # print (dd)
                    output.append(ReportingUnit(**dd))
                    if len(tx.addresses) > 0:
                        accounts.append(
                            ReportingAddresses(
                                **{
                                    "tx_hash": tx.tx_hash,
                                    "date": tx.date,
                                    "addresses": ", ".join(tx.addresses),
                                }
                            )
                        )
        return output, accounts

    def add_tx_addresses(self, event: MongoTypeLoggedEvent, addresses: set):
        _to = event.result.get("to_address")
        if _to:
            if len(_to) > 20:
                addresses.add(_to)

        _from = event.result.get("from_address")
        if _from:
            if len(_from) > 20:
                addresses.add(_from)

        return addresses

    def process_txs_for_action_type_classification(
        self,
        tx_hashes: list[str],
        reporting_subject: ReportingSubject,
    ):
        validated_txs = {}
        txs_by_action_type: dict[ReportingActionType : list[ClassifiedTransaction]] = {}
        for action_type in ReportingActionType:
            txs_by_action_type[action_type]: list[ClassifiedTransaction] = []  # type: ignore

        # get all logged events from transactions on impacted address <9363,0>
        # note: transactions without logged events are not retrieved.
        all_logged_events_for_tx_hashes = [
            MongoTypeLoggedEvent(**x)
            for x in self.mainnet[Collections.tokens_logged_events].aggregate(
                [
                    {"$match": {"tx_hash": {"$in": tx_hashes}}},
                    {"$sort": {"ordering": ASCENDING}},
                ]
            )
        ]

        tx_hashes_from_events = []

        # groupby transaction hash, output in dict classified_txs
        for event in all_logged_events_for_tx_hashes:
            addresses = self.add_tx_addresses(event, set())
            tx_hashes_from_events.append(event.tx_hash)
            classified_tx = validated_txs.get(event.tx_hash)
            if not classified_tx:
                validated_txs[event.tx_hash] = ClassifiedTransaction(
                    tx_hash=event.tx_hash,
                    logged_events=[event],
                    block_height=event.block_height,
                    addresses=addresses,
                )
            else:
                classified_tx: ClassifiedTransaction
                addresses = self.add_tx_addresses(event, classified_tx.addresses)
                classified_tx.addresses = addresses
                classified_tx.logged_events.append(event)
                validated_txs[event.tx_hash] = classified_tx

        # now all transactions have been been validated, time to classify to action type
        for classified_tx in validated_txs.values():
            classified_tx.action_type = ReportingActionType.none
            classified_tx = self.add_date_to_tx(classified_tx)

            if reporting_subject == ReportingSubject.Concordex:
                if len(classified_tx.logged_events) == 1:
                    classified_tx.action_type = ReportingActionType.deposit

                elif len(classified_tx.logged_events) > 1:
                    classified_tx = self.classify_tx_as_swap_or_withdraw(classified_tx)

            elif reporting_subject == ReportingSubject.Arabella:

                if classified_tx.logged_events[0].event_type == "mint_event":
                    classified_tx.action_type = ReportingActionType.mint

                if classified_tx.logged_events[0].event_type == "burn_event":
                    classified_tx.action_type = ReportingActionType.burn

            elif reporting_subject == ReportingSubject.Tricorn:

                if classified_tx.logged_events[0].event_type == "mint_event":
                    classified_tx.action_type = ReportingActionType.mint
                # Tricorn burn txs seem to have a fee transfer as first logged event,
                # hence we need to take the second logged event to classify correctly.
                if len(classified_tx.logged_events) > 1:
                    if classified_tx.logged_events[1].event_type == "burn_event":
                        classified_tx.action_type = ReportingActionType.burn
                        classified_tx.logged_event_index_for_action = 1

            txs_by_action_type[classified_tx.action_type].append(classified_tx)
        return txs_by_action_type, tx_hashes_from_events

    def get_analytics_for_platform(
        self, reporting_subject: ReportingSubject, d_date: str
    ):

        if reporting_subject == ReportingSubject.Concordex:
            tx_hashes = self.get_txs_for_impacted_address_cdex(d_date)
        elif reporting_subject == ReportingSubject.Arabella:
            tx_hashes = self.get_txs_for_impacted_address_arabella(d_date)
        elif reporting_subject == ReportingSubject.Tricorn:
            tx_hashes = self.get_txs_for_impacted_address_tricorn(d_date)

        if len(tx_hashes) > 0:
            (
                txs_by_action_type,
                tx_hashes_from_events,
            ) = self.process_txs_for_action_type_classification(
                tx_hashes, reporting_subject
            )
            reporting_output = self.process_txs_for_analytics(txs_by_action_type)

            return reporting_output
        else:
            return None

    def calculate_tvl(
        self,
        reporting_subject: ReportingSubject,
        analysis: AnalysisType,
        dates_from_start_until_date: list[str],
    ):
        pipeline = [
            {
                "$match": {
                    "date": {"$in": dates_from_start_until_date},
                    "type": analysis.value,
                    "reporting_subject": reporting_subject.value.lower(),
                }
            },
            {"$sort": {"date": 1}},
            {"$project": {"_id": 0, "date": 1, "action_types_for_day": 1}},
        ]

        result = list(self.mainnet[Collections.statistics].aggregate(pipeline))
        tvl = 0
        for day_dict in result:
            day_result = day_dict["action_types_for_day"]
            if reporting_subject == ReportingSubject.Arabella:
                for action in day_result:
                    if action["action_type"] == "Mint":
                        tvl += action["amount_in_usd"]
                    if action["action_type"] == "Withdraw":
                        tvl -= action["amount_in_usd"]

            if reporting_subject == ReportingSubject.Concordex:
                for action in day_result:
                    if action["action_type"] == "Deposit":
                        tvl += action["amount_in_usd"]
                    if action["action_type"] == "Withdraw":
                        tvl -= action["amount_in_usd"]
        return tvl

    def perform_statistics_bridges_and_dexes(self):
        """
        Calculate statistics to feed the Bridges and Dexes view.
        """
        analysis = AnalysisType.statistics_bridges_and_dexes
        self.fungible_tokens_with_markup = self.get_fungible_tokens_with_markup()
        self.historical_exchange_rates = self.get_historical_rates()
        self.heights, self.block_end_of_day_dict = self.get_all_blocks_last_height()
        dates_to_process = self.find_dates_to_process(analysis)
        if len(dates_to_process) == 0:
            # run today's date
            dates_to_process = [
                f"{dt.datetime.now().astimezone(dt.timezone.utc):%Y-%m-%d}"
            ]
        # check to see if the day we want to process has historical exchange rates informartion
        # already available. If not, quit and wait for next run.
        # Note only check this if we do 1 day, not for a full rerun.
        if len(dates_to_process) == 1:
            test_exchange_rate = self.historical_exchange_rates["ETH"].get(
                dates_to_process[0]
            )
            do_your_thing = test_exchange_rate is not None
            console.log(
                f"To process: {dates_to_process[0]}, ETH for this date: {test_exchange_rate}, can we start? {do_your_thing}."
            )
        elif len(dates_to_process) > 1:
            do_your_thing = True
            console.log(f"To process: {len(dates_to_process)=}")
        else:
            do_your_thing = False

        if do_your_thing:
            # these dates are when both Arabella and Concordex contracts did not exist yet
            # so no need to search for txs.
            dates_to_remove = self.generate_dates_from_start_date_until_end_date(
                "2021-06-09", "2023-05-03"
            )
            dates_to_process = sorted(
                list(set(dates_to_process) - set(dates_to_remove))
            )

            for reporting_subject in ReportingSubject:

                queue = []
                for d_date in dates_to_process:
                    _id = f"{d_date}-{analysis.value}-{reporting_subject.value.lower()}"
                    console.log(_id)
                    reporting_output = self.get_analytics_for_platform(
                        reporting_subject, d_date
                    )
                    # we will now request all values (including d_date itself) and calculate the
                    # cumulative TVL
                    # dates_from_start_until_date = (
                    #     self.generate_dates_from_start_date_until_end_date(
                    #         "2023-05-03", d_date
                    #     )
                    # )
                    # tvl = self.calculate_tvl(
                    #     reporting_subject, analysis, dates_from
                    # _start_until_date
                    # )
                    dct = {
                        "_id": _id,
                        "date": d_date,
                        "type": analysis.value,
                        "reporting_subject": reporting_subject.value.lower(),
                        "action_types_for_day": (
                            reporting_output.action_types_for_day
                            if reporting_output
                            else []
                        ),
                        "fungible_tokens_for_day": (
                            reporting_output.fungible_tokens_for_day
                            if reporting_output
                            else []
                        ),
                        "unique_addresses_for_day": (
                            reporting_output.addresses if reporting_output else []
                        ),
                        # "tvl_in_usd": tvl,
                    }

                    queue.append(
                        ReplaceOne(
                            {"_id": _id},
                            replacement=dct,
                            upsert=True,
                        )
                    )
                    _ = self.mainnet[Collections.statistics].bulk_write(queue)
                    queue = []

            if len(dates_to_process) > 0:
                self.write_queue_to_collection(queue, analysis)
