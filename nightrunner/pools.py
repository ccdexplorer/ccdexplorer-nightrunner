from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from pymongo import ReplaceOne


console = Console()


class Pools(Utils):

    def perform_statistics_classified_pools(self):
        self.repo: Repo
        analysis = AnalysisType.statistics_classified_pools
        dates_to_process = self.find_dates_to_process(analysis)
        queue = []
        commits = reversed(list(self.repo.iter_commits("main")))
        process_pools = False
        for commit in commits:
            d_date = self.get_date_from_git(commit)

            # pools started at 2022-06-23
            if d_date == "2022-06-23":
                process_pools = True

            if d_date in dates_to_process and process_pools:
                df = self.get_df_from_git(commit)
                _id = f"{d_date}-{analysis.value}"
                console.log(_id)

                if len(df[df["pool_status"] == "openForAll"]) > 0:
                    open_pool_count = len(df[df["pool_status"] == "openForAll"])
                    closed_pool_count = len(df[df["pool_status"] == "closedForAll"])
                    closed_new_pool_count = len(df[df["pool_status"] == "closedForNew"])
                else:
                    open_pool_count = len(df[df["pool_status"] == "open_for_all"])
                    closed_pool_count = len(df[df["pool_status"] == "closed_for_all"])
                    closed_new_pool_count = len(
                        df[df["pool_status"] == "closed_for_new"]
                    )

                f_no_delegators = df["delegation_target"].isna()
                delegator_count = len(df[~f_no_delegators])
                delegator_avg_stake = df[~f_no_delegators]["staked_amount"].mean()
                if open_pool_count > 0:
                    delegator_avg_count_per_pool = delegator_count / open_pool_count
                else:
                    delegator_avg_count_per_pool = 0
                pool_total_delegated = df[~f_no_delegators]["staked_amount"].sum()
                pass
                dct = {
                    "_id": _id,
                    "type": analysis.value,
                    "date": d_date,
                    "open_pool_count": open_pool_count,
                    "closed_pool_count": closed_pool_count,
                    "closed_new_pool_count": closed_new_pool_count,
                    "delegator_count": delegator_count,
                    "delegator_avg_stake": delegator_avg_stake,
                    "delegator_avg_count_per_pool": delegator_avg_count_per_pool,
                    "pool_total_delegated": pool_total_delegated,
                }

                queue.append(
                    ReplaceOne(
                        {"_id": _id},
                        replacement=dct,
                        upsert=True,
                    )
                )
        self.write_queue_to_collection(queue, analysis)
