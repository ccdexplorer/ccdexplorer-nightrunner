from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from pymongo import ReplaceOne
from ccdefundamentals.mongodb import Collections
from pymongo.collection import Collection

console = Console()


class MongoTransactions(Utils):

    def perform_statistics_mongo_transactions(self):
        self.repo: Repo
        self.mainnet: dict[Collections, Collection]
        analysis = AnalysisType.statistics_mongo_transactions
        dates_to_process = self.find_dates_to_process(analysis)
        queue = []
        commits = reversed(list(self.repo.iter_commits("main")))
        for commit in commits:
            d_date = self.get_date_from_git(commit)
            s, e = self.get_start_end_block_from_date(d_date)
            if d_date in dates_to_process:
                _id = f"{d_date}-{analysis.value}"
                console.log(_id)

                contents = {}

                pipeline = [
                    {
                        "$match": {
                            "block_info.height": {
                                "$gte": s,
                                "$lte": e,
                            }
                        }
                    },
                    {"$sortByCount": "$type.contents"},
                ]
                dd = self.mainnet[Collections.transactions].aggregate(pipeline)
                contents_day_list = list(dd)
                # print(
                #     f"{sum([int(x['count']) for x in contents_day_list]):6,.0f}",
                #     end=" | ",
                # )
                contents = {
                    "_id": _id,
                    "date": d_date,
                    "type": analysis.value,
                }
                contents_update = {x["_id"]: int(x["count"]) for x in contents_day_list}
                contents.update(contents_update)

                pipeline = [
                    {
                        "$match": {
                            "block_info.height": {
                                "$gte": s,
                                "$lte": e,
                            }
                        }
                    },
                    {"$sortByCount": "$type.type"},
                ]
                dd = self.mainnet[Collections.transactions].aggregate(pipeline)
                type_day_list = list(dd)
                contents_update = {x["_id"]: int(x["count"]) for x in type_day_list}
                contents.update(contents_update)

                queue.append(
                    ReplaceOne(
                        {"_id": _id},
                        replacement=contents,
                        upsert=True,
                    )
                )
        self.write_queue_to_collection(queue, analysis)
