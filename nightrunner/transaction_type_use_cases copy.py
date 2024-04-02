# from ccdexplorer_fundamentals.mongodb import Collections
# from pymongo import ReplaceOne
# from rich.console import Console
# import datetime as dt
# from .utils import AnalysisType, Utils
# import more_itertools
# from collections import Counter

# console = Console()


# class TransactionTypes(Utils):

#     def perform_statistics_transaction_types(self):
#         """
#         Calculate transaction type counts
#         """
#         analysis = AnalysisType.statistics_transaction_types

#         usecases_dict = self.get_usecases_complete()

#         # out loop is usecases
#         for usecase_id, usecase in usecases_dict.items():
#             # find previously done items
#             pipeline = [
#                 {"$match": {"type": analysis.value, "usecase": usecase_id}},
#             ]
#             already_done_for_usecase = {
#                 x["_id"]: x
#                 for x in self.mainnet[Collections.statistics].aggregate(pipeline)
#             }

#             dates_to_process = self.find_dates_to_process_for_usecase(
#                 analysis, usecase_id
#             )
#             if len(dates_to_process) == 0:
#                 # run today's date
#                 dates_to_process = [
#                     f"{dt.datetime.now().astimezone(dt.timezone.utc):%Y-%m-%d}"
#                 ]

#             # loop through all days for this usecase that we need to perform
#             for d_date in dates_to_process:
#                 queue = []
#                 height_for_first_block, height_for_last_block = (
#                     self.get_start_end_block_from_date(d_date)
#                 )

#                 # if it's an actual usecase ('all' == the chain)
#                 if usecase_id != "all":
#                     # we need to loop through all addresses separately
#                     for address in usecase["mainnet_addresses"]:
#                         _id = f"{d_date}-{analysis.value}-{usecase_id}-{address[:29]}"
#                         already_done = already_done_for_usecase.get(_id)

#                         if not already_done:
#                             dct = self.perform_actions_for_address(
#                                 analysis, usecase_id, d_date, address, _id
#                             )

#                             if dct:
#                                 queue.append(
#                                     ReplaceOne(
#                                         {"_id": _id},
#                                         replacement=dct,
#                                         upsert=True,
#                                     )
#                                 )
#                 # so usecase 'all', ie the chain.
#                 else:
#                     _id = f"{d_date}-{analysis.value}-{usecase_id}"
#                     console.log(_id)

#                     pipeline = [
#                         {
#                             "$match": {
#                                 "block_info.height": {
#                                     "$gte": height_for_first_block,
#                                     "$lte": height_for_last_block,
#                                 }
#                             }
#                         },
#                         {"$sortByCount": "$type.contents"},
#                     ]
#                     dd = list(
#                         self.mainnet[Collections.transactions].aggregate(pipeline)
#                     )
#                     if len(dd) > 0:
#                         tx_types = {x["_id"]: x["count"] for x in dd}

#                     dct = {
#                         "_id": _id,
#                         "date": d_date,
#                         "type": analysis.value,
#                         "usecase": usecase_id,
#                         # note no address here
#                         "tx_type_counts": tx_types,
#                     }

#                     queue.append(
#                         ReplaceOne(
#                             {"_id": _id},
#                             replacement=dct,
#                             upsert=True,
#                         )
#                     )

#                 if len(queue) > 0:
#                     _ = self.mainnet[Collections.statistics].bulk_write(queue)

#         # when done...
#         self.write_queue_to_collection(queue, analysis)

#     def perform_actions_for_address(
#         self,
#         analysis: AnalysisType,
#         usecase_id: str,
#         d_date: str,
#         address: str,
#         _id: str,
#     ):

#         console.log(_id)
#         s = dt.datetime.now().astimezone(dt.timezone.utc)
#         tx_types = {}
#         pipeline = [
#             {"$match": {"date": d_date}},
#             {"$match": {"impacted_address_canonical": address[:29]}},
#             # {"$match": {"tx_hash": {"$exists": True}}},
#             {"$project": {"_id": 0, "tx_hash": 1}},
#         ]
#         tx_hashes = [
#             x["tx_hash"]
#             for x in self.mainnet[Collections.impacted_addresses].aggregate(pipeline)
#             if "tx_hash" in x
#         ]
#         if len(tx_hashes):
#             tx_types = Counter()
#             for tx_hash_batch in more_itertools.chunked(tx_hashes, 1_000):
#                 e = dt.datetime.now().astimezone(dt.timezone.utc)
#                 console.log(
#                     f"{d_date} - {address[:4]}: # = {len(tx_hash_batch):,.0f} / {len(tx_hashes):,.0f} in {(e-s).total_seconds():,.4}s"
#                 )
#                 s = dt.datetime.now().astimezone(dt.timezone.utc)
#                 # now we will do a count of transaction types from these hashes
#                 pipeline = [
#                     {"$match": {"_id": {"$in": tx_hash_batch}}},
#                     {"$sortByCount": "$type.contents"},
#                 ]
#                 dd = list(self.mainnet[Collections.transactions].aggregate(pipeline))
#                 if len(dd) > 0:
#                     tx_types += Counter({x["_id"]: x["count"] for x in dd})

#                 e = dt.datetime.now().astimezone(dt.timezone.utc)
#                 console.log(
#                     f"{d_date} - {address[:4]}: sortByCount in {(e-s).total_seconds():,.4}s"
#                 )
#             dct = {
#                 "_id": _id,
#                 "date": d_date,
#                 "type": analysis.value,
#                 "usecase": usecase_id,
#                 "address": address,
#                 "tx_type_counts": tx_types,
#             }

#             return dct
#         else:
#             return None
