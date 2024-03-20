from git import Repo
from rich.console import Console
from .utils import Utils, PreRender
from pymongo import ReplaceOne, DESCENDING, ASCENDING
from ccdefundamentals.mongodb import Collections
from pymongo.collection import Collection
import datetime as dt
from datetime import timedelta
from ccdefundamentals.GRPCClient.CCD_Types import CCD_BlockItemSummary

console = Console()


class MongoAccountsTable(Utils):

    def perform_statistics_mongo_accounts_table(self):
        self.repo: Repo
        self.mainnet: dict[Collections, Collection]
        prerender = PreRender.accounts_table
        console.log(f"Start {prerender.value}...")
        accounts_response = {}
        exchange_accounts = self.get_exchanges_as_list()
        now = dt.datetime.now().astimezone(dt.timezone.utc)
        pipeline_regular = [
            {
                "$match": {
                    "account_transaction.effects.account_transfer": {"$exists": True}
                }
            },
            {"$match": {"block_info.slot_time": {"$gt": now - timedelta(days=7)}}},
            {
                "$set": {
                    "canonical_sender": {
                        "$substr": [
                            "$account_transaction.sender",
                            0,
                            29,
                        ]
                    }
                }
            },
            {
                "$set": {
                    "canonical_receiver": {
                        "$substr": [
                            "$account_transaction.effects.account_transfer.receiver",
                            0,
                            29,
                        ]
                    }
                }
            },
            {
                "$match": {
                    "$or": [
                        {"canonical_sender": {"$nin": exchange_accounts}},
                        {"canonical_receiver": {"$nin": exchange_accounts}},
                    ]
                }
            },
            {
                "$sort": {
                    "account_transaction.effects.account_transfer.amount": DESCENDING
                }
            },
            {"$limit": 10},
        ]

        result = self.mainnet[Collections.transactions].aggregate(pipeline_regular)

        largest_regular = [CCD_BlockItemSummary(**x) for x in result]
        console.log(f"Start {prerender.value}...largest_regular done")
        pipeline_scheduled = [
            {
                "$match": {
                    "account_transaction.effects.transferred_with_schedule": {
                        "$exists": True
                    }
                }
            },
            {"$match": {"block_info.slot_time": {"$gt": now - timedelta(days=7)}}},
            {
                "$set": {
                    "canonical_sender": {
                        "$substr": [
                            "$account_transaction.sender",
                            0,
                            29,
                        ]
                    }
                }
            },
            {
                "$set": {
                    "canonical_receiver": {
                        "$substr": [
                            "$account_transaction.effects.transferred_with_schedule.receiver",
                            0,
                            29,
                        ]
                    }
                }
            },
            {
                "$match": {
                    "$or": [
                        {"canonical_sender": {"$nin": exchange_accounts}},
                        {"canonical_receiver": {"$nin": exchange_accounts}},
                    ]
                }
            },
            {
                "$set": {
                    "amount_ccd": {
                        "$sum": {
                            "$first": {
                                "$map": {
                                    "input": "$effects.transferred_with_schedule.amount",
                                    "as": "event",
                                    "in": {
                                        "$map": {
                                            "input": "$$event.amount",
                                            "as": "amount",
                                            "in": {
                                                "$divide": [
                                                    {
                                                        "$toDouble": {
                                                            "$last": "$$amount"
                                                        }
                                                    },
                                                    1000000,
                                                ]
                                            },
                                        }
                                    },
                                }
                            }
                        }
                    }
                }
            },
            {"$sort": {"amount_ccd": DESCENDING}},
            {"$limit": 10},
        ]

        result = self.mainnet[Collections.transactions].aggregate(pipeline_scheduled)

        largest_scheduled = [CCD_BlockItemSummary(**x) for x in result]
        console.log(f"Start {prerender.value}...largest_scheduled done")
        largest_regular_dict = {
            x.hash: x.account_transaction.effects.account_transfer.amount
            for x in largest_regular
        }

        largest_scheduled_dict = {
            x.hash: sum(
                [
                    y.amount
                    for y in x.account_transaction.effects.transferred_with_schedule.amount
                ]
            )
            for x in largest_scheduled
        }

        largest_dict = largest_regular_dict
        largest_dict.update(largest_scheduled_dict)
        sorted_dict = {
            k: v
            for k, v in sorted(
                largest_dict.items(), key=lambda item: item[1], reverse=True
            )
        }

        # now redo the dicts, to make it key=hash and value is original CCD_BlockItemSummary
        largest_regular_dict = {x.hash: x for x in largest_regular}
        largest_scheduled_dict = {x.hash: x for x in largest_scheduled}

        for index, (hash, v) in enumerate(sorted_dict.items()):
            if hash in largest_regular_dict.keys():
                item = largest_regular_dict[hash]
                amount = (
                    item.account_transaction.effects.account_transfer.amount / 1_000_000
                ) / 1_000_000
            else:
                item = largest_scheduled_dict[hash]
                # note the different division, in the pipeline for scheduled
                # we have already made it into CCD.
                amount = (
                    sum(
                        [
                            y.amount
                            for y in item.account_transaction.effects.transferred_with_schedule.amount
                        ]
                    )
                    / 1_000_000
                    / 1_000_000
                )

            accounts_response[f"largest-{index}"] = item.account_transaction.sender
            accounts_response[f"largest-{index}-amount"] = {
                "amount": amount,
                "tx_hash": item.hash,
            }

        # Most transactions
        pipeline = [
            {"$match": {"account_transaction": {"$exists": True}}},
            {"$match": {"block_info.slot_time": {"$gt": now - timedelta(days=7)}}},
            {
                "$set": {
                    "canonical_sender": {
                        "$substr": [
                            "$account_transaction.sender",
                            0,
                            29,
                        ]
                    }
                }
            },
            {"$match": {"canonical_sender": {"$nin": exchange_accounts}}},
            {
                "$group": {
                    "_id": "$account_transaction.sender",
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"count": DESCENDING}},
            {"$limit": 100},
        ]
        usecases = self.get_usecases()
        most_active = self.mainnet[Collections.transactions].aggregate(pipeline)
        console.log(f"Start {prerender.value}...most_active done")

        merged = {}
        leftover = []
        for index, ele in enumerate(most_active):
            if ele["_id"] in usecases:
                use_case_display_name = usecases[ele["_id"]]
                if merged.get(use_case_display_name):
                    merged[use_case_display_name] += ele["count"]
                else:
                    merged[use_case_display_name] = ele["count"]
            else:
                leftover.append(ele)

        merged_list = [{"_id": k, "count": v} for k, v in merged.items()]
        merged_list.extend(leftover)
        merged_list = sorted(merged_list, key=lambda x: x["count"], reverse=True)
        for index, ele in enumerate(merged_list):
            tx_count = ele["count"]
            accounts_response[f"most_active-{index}"] = ele["_id"]
            accounts_response[f"most_active-{index}-tx"] = (
                f'<span class="small ccd">{tx_count:,.0f}</span>'
            )

        accounts_response["timestamp"] = dt.datetime.now().astimezone(dt.timezone.utc)
        accounts_response["updated"] = True
        accounts_response["type"] = prerender.value

        queue = []
        queue.append(
            ReplaceOne(
                {"_id": prerender.value},
                replacement=accounts_response,
                upsert=True,
            )
        )
        console.log(f"Start {prerender.value}...done")
        self.write_queue_to_prerender_collection(queue, prerender)
