from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from pymongo import ReplaceOne
from ccdefundamentals.mongodb import (
    CollectionsUtilities,
)

from pymongo.collection import Collection

console = Console()


class Classified(Utils):
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

    def perform_statistics_ccd_classified(self):
        self.repo: Repo
        exchanges = self.get_exchanges()
        analysis = AnalysisType.statistics_ccd_classified
        dates_to_process = self.find_dates_to_process(analysis)
        queue = []
        commits = reversed(list(self.repo.iter_commits("main")))

        for commit in commits:
            d_date = self.get_date_from_git(commit)

            if d_date in dates_to_process:
                df = self.get_df_from_git(commit)
                _id = f"{d_date}-{analysis.value}"
                console.log(_id)

                df["account_29"] = df["account"].str[:29]
                f_staked = df["staked_amount"] > 0.0
                # this is only available with the Sirius protocol and later
                if "delegation_target" in df.columns:
                    f_no_delegators = df["delegation_target"].isna()
                    pool_total_delegated = df[~f_no_delegators]["staked_amount"].sum()
                else:
                    pool_total_delegated = 0

                filters = {}
                for key, address_list in exchanges.items():
                    filters[key] = df["account_29"].isin(address_list)

                tradeable = {}
                for key, address_list in exchanges.items():
                    tradeable[key] = df[filters[key]]["total_balance"].sum()
                tradeable_sum = sum(tradeable.values())

                staked = df[f_staked]["staked_amount"].sum() - pool_total_delegated
                total_supply = df["total_balance"].sum()

                unstaked = total_supply - staked - tradeable_sum - pool_total_delegated

                dct = {
                    "_id": _id,
                    "type": analysis.value,
                    "date": d_date,
                    "total_supply": total_supply,
                    "staked": staked,
                    "unstaked": unstaked,
                    "delegated": pool_total_delegated,
                }

                for key in exchanges:
                    dct.update({key: tradeable[key]})

                queue.append(
                    ReplaceOne(
                        {"_id": _id},
                        replacement=dct,
                        upsert=True,
                    )
                )
        self.write_queue_to_collection(queue, analysis)
