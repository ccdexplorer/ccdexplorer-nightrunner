from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from pymongo import ReplaceOne
from ccdexplorer_fundamentals.GRPCClient.CCD_Types import ProtocolVersions


console = Console()


class NetworkSummary(Utils):

    def perform_statistics_network(self):
        self.perform_statistics_network_summary()
        self.perform_statistics_network_activity()

    def perform_statistics_network_summary(self):
        self.grpcclient: GRPCClient
        self.repo: Repo
        analysis = AnalysisType.statistics_network_summary
        dates_to_process = self.find_dates_to_process_for_nightly_statistics(analysis)
        queue = []
        commits = reversed(list(self.repo.iter_commits("main")))
        for commit in commits:
            d_date = self.get_date_from_git(commit)
            if d_date in dates_to_process:
                df = self.get_df_from_git(commit)
                _id = f"{d_date}-{analysis.value}"
                console.log(_id)
                block_hash = self.get_hash_from_date(d_date)
                s = self.grpcclient.get_tokenomics_info(block_hash)
                if s.v0:
                    versioned_object = s.v0
                if s.v1:
                    versioned_object = s.v1

                protocol_version = ProtocolVersions(
                    versioned_object.protocol_version
                ).name
                total_amount = int(versioned_object.total_amount) / 1_000_000
                total_encrypted_amount = (
                    int(versioned_object.total_encrypted_amount) / 1_000_000
                )
                baking_reward_account = (
                    int(versioned_object.baking_reward_account) / 1_000_000
                )
                finalization_reward_account = (
                    int(versioned_object.finalization_reward_account) / 1_000_000
                )
                gas_account = int(versioned_object.gas_account) / 1_000_000

                self.accounts_count = len(df)
                f_no_bakers = df["baker_id"].isna()
                self.bakers_count = len(df[~f_no_bakers])
                # New row to add, as a dictionary.
                dct = {
                    "_id": _id,
                    "type": analysis.value,
                    "date": d_date,
                    "protocol_version": protocol_version,
                    "total_amount": total_amount,
                    "total_encrypted_amount": total_encrypted_amount,
                    "baking_reward_account": baking_reward_account,
                    "finalization_reward_account": finalization_reward_account,
                    "gas_account": gas_account,
                    "account_count": self.accounts_count,
                    "validator_count": self.bakers_count,
                }

                queue.append(
                    ReplaceOne(
                        {"_id": _id},
                        replacement=dct,
                        upsert=True,
                    )
                )

        self.write_queue_to_collection(queue, analysis)

    def calculate_activity(
        self,
        d_date_today,
        df_accounts_for_day,
        df_accounts_for_yesterday,
        d_date_yesterday,
    ):

        df_accounts_for_day.set_index("account", inplace=True)
        d_today = df_accounts_for_day.to_dict("index")

        df_accounts_for_yesterday.set_index("account", inplace=True)
        d_yesterday = df_accounts_for_yesterday.to_dict("index")

        movement = 0
        for acc, row in d_today.items():
            today_value = row["total_balance"]
            yesterday_value = d_yesterday.get(acc, {"total_balance": 0.0})[
                "total_balance"
            ]
            movement += abs(today_value - yesterday_value)

        # df_totals = pd.read_csv(f"{dir}/network-summary.csv")
        # f_yesterday = df_totals["date"] == d_date_yesterday
        # dd = df_totals[f_yesterday].reset_index()
        # total_balance_yesterday = dd["total_amount"][0]
        total_balance_yesterday = self.get_total_amount_from_summary_for_date(
            d_date_yesterday
        )
        # f_today = df_totals["date"] == d_date_today
        # dd = df_totals[f_today].reset_index()
        # total_balance_today = dd["total_amount"][0]
        total_balance_today = self.get_total_amount_from_summary_for_date(d_date_today)

        inflation = total_balance_today - total_balance_yesterday

        network_activity = max(0, (movement - inflation) / 2)

        return network_activity

    def perform_statistics_network_activity(self):
        self.repo: Repo
        analysis = AnalysisType.statistics_network_activity
        dates_to_process = self.find_dates_to_process_for_nightly_statistics(analysis)
        dates_to_process_count_down = {x: x for x in dates_to_process}
        queue = []
        commits = reversed(list(self.repo.iter_commits("main")))
        previous_commit = None
        for commit_index, commit in enumerate(commits):

            d_date = self.get_date_from_git(commit)
            if d_date in dates_to_process and previous_commit:
                del dates_to_process_count_down[d_date]
                df_accounts_for_day = self.get_df_from_git(commit)
                df_accounts_for_yesterday = self.get_df_from_git(previous_commit)
                d_date_yesterday = self.get_date_from_git(previous_commit)

                _id = f"{d_date}-{analysis.value}"
                console.log(_id)

                network_activity = self.calculate_activity(
                    d_date,
                    df_accounts_for_day,
                    df_accounts_for_yesterday,
                    d_date_yesterday,
                )

                dct = {
                    "_id": _id,
                    "date": d_date,
                    "type": analysis.value,
                    "network_activity": network_activity,
                }

                queue.append(
                    ReplaceOne(
                        {"_id": _id},
                        replacement=dct,
                        upsert=True,
                    )
                )
            previous_commit = commit
            if len(queue) > 0:
                self.write_queue_to_collection(queue, analysis)
            queue = []
        self.have_we_missed_commits(analysis, dates_to_process_count_down)
