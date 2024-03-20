from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from pymongo import ReplaceOne


console = Console()


class Holders(Utils):

    def perform_statistics_daily_holders(self):
        self.repo: Repo
        analysis = AnalysisType.statistics_daily_holders
        dates_to_process = self.find_dates_to_process(analysis)
        queue = []
        commits = reversed(list(self.repo.iter_commits("main")))
        for commit in commits:
            d_date = self.get_date_from_git(commit)
            if d_date in dates_to_process:
                df = self.get_df_from_git(commit)
                _id = f"{d_date}-{analysis.value}"
                console.log(_id)

                # the actual analysis
                f = df["total_balance"] > 1_000_000
                count_accounts = int(df[f].count()["account"])

                dct = {
                    "_id": _id,
                    "type": analysis.value,
                    "date": d_date,
                    "count_above_1M": count_accounts,
                }

                queue.append(
                    ReplaceOne(
                        {"_id": _id},
                        replacement=dct,
                        upsert=True,
                    )
                )
        self.write_queue_to_collection(queue, analysis)
