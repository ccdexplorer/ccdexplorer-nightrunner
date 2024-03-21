from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from pymongo import ReplaceOne
from ccdexplorer_fundamentals.ccdscan import CCDScan


console = Console()


class ReleaseAmounts(Utils):

    def perform_statistics_release_amounts(self):
        self.repo: Repo
        analysis = AnalysisType.statistics_release_amounts
        ccdscan = CCDScan(tooter=self.tooter)
        dates_to_process = self.find_dates_to_process(analysis)
        queue = []
        commits = reversed(list(self.repo.iter_commits("main")))
        for commit in commits:
            d_date = self.get_date_from_git(commit)
            if d_date in dates_to_process:
                _id = f"{d_date}-{analysis.value}"
                console.log(_id)
                block_hash = self.get_hash_from_date(d_date)
                result = ccdscan.ql_request_block_for_release(block_hash)
                dct = {
                    "_id": _id,
                    "date": d_date,
                    "type": analysis.value,
                    "block_hash": block_hash,
                    "block_height": int(result["blockHeight"]),
                    "total_amount": result["balanceStatistics"]["totalAmount"],
                    "total_amount_released": result["balanceStatistics"][
                        "totalAmountReleased"
                    ],
                }

                queue.append(
                    ReplaceOne(
                        {"_id": _id},
                        replacement=dct,
                        upsert=True,
                    )
                )
        self.write_queue_to_collection(queue, analysis)
