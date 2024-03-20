from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from pymongo import ReplaceOne


console = Console()


class NetworkActivity(Utils):
    pass
    # def calculate_activity(
    #     self,
    #     d_date_today,
    #     df_accounts_for_day,
    #     df_accounts_for_yesterday,
    #     d_date_yesterday,
    # ):

    #     df_accounts_for_day.set_index("account", inplace=True)
    #     d_today = df_accounts_for_day.to_dict("index")

    #     df_accounts_for_yesterday.set_index("account", inplace=True)
    #     d_yesterday = df_accounts_for_yesterday.to_dict("index")

    #     movement = 0
    #     for acc, row in d_today.items():
    #         today_value = row["total_balance"]
    #         yesterday_value = d_yesterday.get(acc, {"total_balance": 0.0})[
    #             "total_balance"
    #         ]
    #         movement += abs(today_value - yesterday_value)

    #     # df_totals = pd.read_csv(f"{dir}/network-summary.csv")
    #     # f_yesterday = df_totals["date"] == d_date_yesterday
    #     # dd = df_totals[f_yesterday].reset_index()
    #     # total_balance_yesterday = dd["total_amount"][0]
    #     total_balance_yesterday = self.get_total_amount_from_summary_for_date(
    #         d_date_yesterday
    #     )
    #     # f_today = df_totals["date"] == d_date_today
    #     # dd = df_totals[f_today].reset_index()
    #     # total_balance_today = dd["total_amount"][0]
    #     total_balance_today = self.get_total_amount_from_summary_for_date(d_date_today)

    #     inflation = total_balance_today - total_balance_yesterday

    #     network_activity = max(0, (movement - inflation) / 2)

    #     return network_activity

    # def perform_statistics_network_activity(self):
    #     self.repo: Repo
    #     analysis = AnalysisType.statistics_network_activity
    #     dates_to_process = self.find_dates_to_process(analysis)
    #     queue = []
    #     commits = reversed(list(self.repo.iter_commits("main")))
    #     previous_commit = None
    #     for commit_index, commit in enumerate(commits):

    #         d_date = self.get_date_from_git(commit)
    #         if d_date in dates_to_process and previous_commit:

    #             df_accounts_for_day = self.get_df_from_git(commit)
    #             df_accounts_for_yesterday = self.get_df_from_git(previous_commit)
    #             d_date_yesterday = self.get_date_from_git(previous_commit)

    #             _id = f"{d_date}-{analysis.value}"
    #             console.log(_id)

    #             network_activity = self.calculate_activity(
    #                 d_date,
    #                 df_accounts_for_day,
    #                 df_accounts_for_yesterday,
    #                 d_date_yesterday,
    #             )

    #             dct = {
    #                 "_id": _id,
    #                 "date": d_date,
    #                 "type": analysis.value,
    #                 "network_activity": network_activity,
    #             }

    #             queue.append(
    #                 ReplaceOne(
    #                     {"_id": _id},
    #                     replacement=dct,
    #                     upsert=True,
    #                 )
    #             )
    #         previous_commit = commit
    #         if len(queue) > 0:
    #             self.write_queue_to_collection(queue, analysis)
    #         queue = []
