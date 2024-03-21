from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from pymongo import ReplaceOne
from ccdexplorer_fundamentals.mongodb import (
    Collections,
    CollectionsUtilities,
)

from pymongo.collection import Collection

console = Console()


class ExchangeWallets(Utils):

    def get_wallets_for_exchange(self, exchange_addresses: list[str], dates: list[str]):
        self.mainnet: dict[Collections, Collection]
        pipeline = [
            {"$match": {"impacted_address_canonical": {"$in": exchange_addresses}}},
            {"$match": {"date": {"$in": dates}}},
            {"$project": {"impacted_address": 1, "_id": 0}},
        ]
        addresses = [
            x["impacted_address"]
            for x in self.mainnet[Collections.impacted_addresses].aggregate(pipeline)
        ]
        return len(set(addresses))

    def perform_statistics_exchange_wallets(self):
        self.repo: Repo
        exchanges = self.get_exchanges()

        analysis = AnalysisType.statistics_exchange_wallets
        dates_to_process = self.find_dates_to_process(analysis)
        # all_dates_for_analysis = self.get_all_dates_for_analysis(analysis)
        queue = []
        # commits = reversed(list(self.repo.iter_commits("main")))

        for d_date in dates_to_process:

            if d_date in dates_to_process:
                _id = f"{d_date}-{analysis.value}"
                console.log(_id)

                dates_from_start_until_date = self.generate_dates_from_start_until_date(
                    d_date
                )
                exchange_wallet_count = {}
                for key, address_list in exchanges.items():
                    exchange_wallet_count[key] = self.get_wallets_for_exchange(
                        address_list, dates_from_start_until_date
                    )

                dct = {
                    "_id": _id,
                    "type": analysis.value,
                    "date": d_date,
                }

                for key in exchanges:
                    dct.update({key: exchange_wallet_count[key]})

                queue.append(
                    ReplaceOne(
                        {"_id": _id},
                        replacement=dct,
                        upsert=True,
                    )
                )
        self.write_queue_to_collection(queue, analysis)
