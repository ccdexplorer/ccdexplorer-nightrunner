from ccdexplorer_fundamentals.mongodb import Collections
from pymongo import ReplaceOne
from rich.console import Console
import datetime as dt
from .utils import AnalysisType, Utils
from enum import Enum

console = Console()


class Grouping(Enum):
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"


class UniqueAddressesV2(Utils):

    def dates_to_consecutive_weeks(self, date_list):
        # Sort the list of dates
        sorted_dates = sorted(date_list)

        # Initialize a list to store consecutive weeks
        consecutive_weeks = []

        # Initialize variables to track the start and end of the current week
        start_of_week = None
        end_of_week = None

        # Iterate through the sorted dates
        for date_str in sorted_dates:
            date = dt.datetime.strptime(date_str, "%Y-%m-%d")

            # If start_of_week is None, set it to the current date
            if start_of_week is None:
                start_of_week = date

            # If end_of_week is None or if the current date is within the same week
            if end_of_week is None or date <= end_of_week + dt.timedelta(
                days=6 - end_of_week.weekday()
            ):
                end_of_week = date

            # If the current date is not within the same week or it's the last date in the list
            if (
                date > end_of_week + dt.timedelta(days=6 - end_of_week.weekday())
                or date == sorted_dates[-1]
            ):
                consecutive_weeks.append(
                    (
                        start_of_week.strftime("%Y-%m-%d"),
                        end_of_week.strftime("%Y-%m-%d"),
                    )
                )
                start_of_week = None
                end_of_week = None

        return consecutive_weeks

    def perform_statistics_unique_addresses_v2(self):
        self.perform_statistics_unique_addresses_daily_v2()
        self.perform_statistics_unique_addresses_weekly_v2()
        self.perform_statistics_unique_addresses_monthly_v2()

    def perform_statistics_unique_addresses_weekly_v2(self):
        """
        Calculate count of unique addresses
        """
        # is_complete, missing = self.check_date_completeness()
        # if not is_complete:
        #     print(f"Missing {len(missing)} dates:")
        #     print("\n".join(missing))
        analysis = AnalysisType.statistics_unique_addresses_v2_weekly
        weeks_to_process = self.find_dates_to_process_for_weekly_unique_addresses(
            analysis, complete=True
        )
        for date_tuple in weeks_to_process:
            d_date = date_tuple[0]
            e_date = date_tuple[1]
            complete = date_tuple[2] == "complete"
            height_for_first_block = self.get_start_block_from_date_for_unique(d_date)
            height_for_last_block = self.get_end_block_from_date_for_unique(e_date)
            self.calculate_unique_address_stats(
                analysis,
                d_date,
                height_for_first_block,
                height_for_last_block,
                complete=complete,
            )

    def perform_statistics_unique_addresses_monthly_v2(self):
        """
        Calculate count of unique addresses
        """
        analysis = AnalysisType.statistics_unique_addresses_v2_monthly
        months_to_process = self.find_dates_to_process_for_monthly_unique_addresses(
            analysis, complete=True
        )
        for date_tuple in months_to_process:
            d_date = date_tuple[0]
            e_date = date_tuple[1]
            complete = date_tuple[2] == "complete"
            height_for_first_block = self.get_start_block_from_date_for_unique(d_date)
            height_for_last_block = self.get_end_block_from_date_for_unique(e_date)
            self.calculate_unique_address_stats(
                analysis,
                d_date,
                height_for_first_block,
                height_for_last_block,
                complete=complete,
            )

    def perform_statistics_unique_addresses_daily_v2(self):
        """
        Calculate count of unique addresses
        """
        analysis = AnalysisType.statistics_unique_addresses_v2_daily
        dates_to_process = self.find_dates_to_process_for_nightly_statistics(analysis)

        for d_date in dates_to_process:

            height_for_first_block, height_for_last_block = (
                self.get_start_end_block_from_date(d_date)
            )
            self.calculate_unique_address_stats(
                analysis,
                d_date,
                height_for_first_block,
                height_for_last_block,
            )

    def calculate_unique_address_stats(
        self,
        analysis: AnalysisType,
        d_date,
        height_for_first_block,
        height_for_last_block,
        grouping=Grouping.daily,
        complete: bool = True,
    ):
        _id = f"{d_date}-{analysis.value}"
        console.log(_id)
        pipeline = [
            {
                "$match": {
                    "block_height": {
                        "$gte": height_for_first_block,
                        "$lte": height_for_last_block,
                    },
                    "effect_type": {"$ne": "Account Reward"},
                }
            },
            {
                "$project": {
                    "address_length": {"$strLenCP": "$impacted_address_canonical"},
                    "impacted_address_canonical": 1,
                }
            },
            {
                "$group": {
                    "_id": {
                        "category": {
                            "$switch": {
                                "branches": [
                                    {
                                        "case": {"$lt": ["$address_length", 29]},
                                        "then": "contract",
                                    },
                                    {
                                        "case": {
                                            "$and": [
                                                {"$gte": ["$address_length", 29]},
                                                {"$lt": ["$address_length", 64]},
                                            ]
                                        },
                                        "then": "address",
                                    },
                                    {
                                        "case": {"$eq": ["$address_length", 64]},
                                        "then": "public_key",
                                    },
                                ],
                                "default": "other",
                            }
                        }
                    },
                    "unique_addresses": {"$addToSet": "$impacted_address_canonical"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "category": "$_id.category",
                    "count": {"$size": "$unique_addresses"},
                }
            },
        ]
        result = self.mainnet[Collections.impacted_addresses].aggregate(pipeline)
        ll = list(result)

        if len(ll) > 0:
            counts_by_category = {doc["category"]: doc["count"] for doc in ll}

        else:
            counts_by_category = {
                "address": 0,
                "contract": 0,
                "public_key": 0,
            }
        dct = {
            "_id": _id,
            "date": d_date,
            "type": analysis.value,
            "unique_impacted_address_count": counts_by_category,
        }

        if grouping.weekly or grouping.monthly:
            dct.update({"complete": complete})

        queue = [
            ReplaceOne(
                {"_id": _id},
                replacement=dct,
                upsert=True,
            )
        ]
        self.write_queue_to_collection(queue, analysis)
