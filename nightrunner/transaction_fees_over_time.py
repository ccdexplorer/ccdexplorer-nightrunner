from ccdexplorer_fundamentals.mongodb import Collections
from pymongo import ReplaceOne
from rich.console import Console

from .utils import AnalysisType, Utils

console = Console()


class TransactionFees(Utils):

    def perform_statistics_transaction_fees(self):
        """
        Calculate transaction fees per day.
        """
        analysis = AnalysisType.statistics_transaction_fees
        dates_to_process = self.find_dates_to_process(analysis)
        queue = []
        for d_date in dates_to_process:
            _id = f"{d_date}-{analysis.value}"
            console.log(_id)
            height_for_first_block, height_for_last_block = (
                self.get_start_end_block_from_date(d_date)
            )
            pipeline = [
                {"$match": {"account_transaction": {"$exists": True}}},
                {
                    "$match": {
                        "block_info.height": {
                            "$gte": height_for_first_block,
                            "$lte": height_for_last_block,
                        }
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "fee_for_day": {"$sum": "$account_transaction.cost"},
                    }
                },
            ]
            result = self.mainnet[Collections.transactions].aggregate(pipeline)
            ll = list(result)
            if len(ll) > 0:
                fee_for_day = ll[0]["fee_for_day"]
            else:
                fee_for_day = 0
            dct = {
                "_id": _id,
                "date": d_date,
                "type": analysis.value,
                "fee_for_day": fee_for_day,
            }

            queue.append(
                ReplaceOne(
                    {"_id": _id},
                    replacement=dct,
                    upsert=True,
                )
            )
        self.write_queue_to_collection(queue, analysis)
