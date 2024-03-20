from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from pymongo import ReplaceOne


console = Console()


class Limits(Utils):

    def perform_statistics_daily_limits(self):
        self.repo: Repo
        analysis = AnalysisType.statistics_daily_limits
        dates_to_process = self.find_dates_to_process(analysis)
        queue = []
        commits = reversed(list(self.repo.iter_commits("main")))
        for commit in commits:
            d_date = self.get_date_from_git(commit)
            if d_date in dates_to_process:
                df = self.get_df_from_git(commit)
                _id = f"{d_date}-{analysis.value}"
                console.log(_id)

                df.sort_values(by="total_balance", inplace=True, ascending=False)
                df.reset_index(inplace=True)

                dct = {
                    "_id": _id,
                    "date": d_date,
                    "type": analysis.value,
                    "amount_to_make_top_100": (
                        df.iloc[100]["total_balance"] if len(df) > 100 else 0.0
                    ),
                    "amount_to_make_top_250": (
                        df.iloc[250]["total_balance"] if len(df) > 250 else 0.0
                    ),
                }

                queue.append(
                    ReplaceOne(
                        {"_id": _id},
                        replacement=dct,
                        upsert=True,
                    )
                )
        self.write_queue_to_collection(queue, analysis)
