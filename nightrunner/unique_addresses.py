from ccdexplorer_fundamentals.mongodb import Collections
from pymongo import ReplaceOne
from rich.console import Console
import datetime as dt
from .utils import AnalysisType, Utils

console = Console()


class UniqueAddresses(Utils):

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

    def perform_statistics_unique_addresses(self):
        self.perform_statistics_unique_addresses_daily()
        self.perform_statistics_unique_addresses_weekly()

    def perform_statistics_unique_addresses_weekly(self):
        """
        Calculate count of unique addresses
        """
        analysis = AnalysisType.statistics_unique_addresses_weekly
        # dates_to_process = self.find_dates_to_process_for_nightly_statistics(analysis)
        weeks_to_process = self.find_dates_to_process_for_weekly_unique_addresses(
            analysis
        )
        queue = []

        for start_date, end_date in weeks_to_process:
            _id = f"{start_date}-{end_date}-{analysis.value}"
            console.log(_id)
            try:
                height_for_first_block, _ = self.get_start_end_block_from_date(
                    start_date
                )
            except (
                TypeError
            ):  # occurs because the chain start is not on the Monday of the week.
                height_for_first_block = 0

            try:
                _, height_for_last_block = self.get_start_end_block_from_date(end_date)
            except TypeError:  # occurs because the chain end is before the end_date.
                height_for_last_block = 1_000_000_000

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
                    "$group": {
                        "_id": "unique_impacted_addresses",
                        "unique_impacted_addresses": {
                            "$addToSet": "$impacted_address_canonical"
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "unique_impacted_address_count": {
                            "$size": "$unique_impacted_addresses"
                        },
                    }
                },
            ]
            result = self.mainnet[Collections.impacted_addresses].aggregate(pipeline)
            ll = list(result)
            if len(ll) > 0:
                unique_impacted_address_count = ll[0]["unique_impacted_address_count"]
            else:
                unique_impacted_address_count = 0
            dct = {
                "_id": _id,
                "date": end_date,
                "type": analysis.value,
                "unique_impacted_address_count": unique_impacted_address_count,
            }

            queue.append(
                ReplaceOne(
                    {"_id": _id},
                    replacement=dct,
                    upsert=True,
                )
            )
            self.write_queue_to_collection(queue, analysis)
            queue = []

    def perform_statistics_unique_addresses_daily(self):
        """
        Calculate count of unique addresses
        """
        analysis = AnalysisType.statistics_unique_addresses_daily
        dates_to_process = self.find_dates_to_process_for_nightly_statistics(analysis)
        queue = []

        for d_date in dates_to_process:
            _id = f"{d_date}-{analysis.value}"
            console.log(_id)
            height_for_first_block, height_for_last_block = (
                self.get_start_end_block_from_date(d_date)
            )
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
                    "$group": {
                        "_id": "$date",
                        "unique_impacted_addresses": {
                            "$addToSet": "$impacted_address_canonical"
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "unique_impacted_address_count": {
                            "$size": "$unique_impacted_addresses"
                        },
                    }
                },
            ]
            result = self.mainnet[Collections.impacted_addresses].aggregate(pipeline)
            ll = list(result)
            if len(ll) > 0:
                unique_impacted_address_count = ll[0]["unique_impacted_address_count"]
            else:
                unique_impacted_address_count = 0
            dct = {
                "_id": _id,
                "date": d_date,
                "type": analysis.value,
                "unique_impacted_address_count": unique_impacted_address_count,
            }

            queue.append(
                ReplaceOne(
                    {"_id": _id},
                    replacement=dct,
                    upsert=True,
                )
            )
        self.write_queue_to_collection(queue, analysis)
