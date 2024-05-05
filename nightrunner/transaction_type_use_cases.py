from ccdexplorer_fundamentals.mongodb import Collections
from pymongo import ReplaceOne
from rich.console import Console
import datetime as dt
from .utils import AnalysisType, Utils
import more_itertools
from collections import Counter

console = Console()


class TransactionTypes(Utils):

    def perform_statistics_transaction_types(self):
        """
        Calculate transaction type counts.
        This method read usecases from the usecase collections (and adds a last one called `all`
        which is the chain). For every time this method runs, for all usecases (except all),
        we will run through all dates from the blockchain.
        For every day we check (in `determine_if_day_needs_to_be_done`) whether:
        - this day already exists
        - whether new addresses have been added or addresses have been removed
        - if we have covered the full day

        Depending on the outcome, we either:
        - process this day as normal
        - re-run this day (ie don't look at the last_block_processed, but instead read the entire day)
        - only update the `based_on_addresses` property for a day, if a new address was added,
        which did not have txs on that day, so no need to redo calculations.

        Finally, if we need to (re-)do calculations, we call `perform_actions_for_usecase`. This method
        first takes the list of addresses that are relevant and looks in the collection `impacted_addresses` for
        any txs on this day for these addresses. As we do this as a group of addresses and not individually,
        we also capture transfers between addresses as 1 tx, so no duplication.
        This list of tx_hashes is then fed (in batches) to the collection `transactions` for a `sortByCount` pipeline.
        """
        analysis = AnalysisType.statistics_transaction_types
        # usecases_dict = self.get_usecases_complete()
        projects_dict = self.get_projects_complete()
        self.all_days: dict = self.get_all_dates_with_info()

        # out loop is usecases
        for project_id, project in projects_dict.items():
            dates_to_process = self.find_dates_to_process_for_project(
                analysis, project_id
            )

            # project 'all', ie the chain.
            if project_id == "all":
                for d_date in dates_to_process[-2:]:
                    queue = []
                    _id = f"{d_date}-{analysis.value}-{project_id}"
                    console.log(_id)
                    height_for_first_block, height_for_last_block = (
                        self.get_start_end_block_from_date(d_date)
                    )
                    pipeline = [
                        {
                            "$match": {
                                "block_info.height": {
                                    "$gte": height_for_first_block,
                                    "$lte": height_for_last_block,
                                }
                            }
                        },
                        {"$sortByCount": "$type.contents"},
                    ]
                    dd = list(
                        self.mainnet[Collections.transactions].aggregate(pipeline)
                    )
                    if len(dd) > 0:
                        tx_types = {x["_id"]: x["count"] for x in dd}

                    dct = {
                        "_id": _id,
                        "date": d_date,
                        "type": analysis.value,
                        "project": project_id,
                        # note no address here
                        "tx_type_counts": tx_types,
                    }

                    queue.append(
                        ReplaceOne(
                            {"_id": _id},
                            replacement=dct,
                            upsert=True,
                        )
                    )
            else:
                # we are going to loop through all days from dates_to_process
                # if the day isn't present already, we need to create it.
                # if the day is present, we need to check whether our current
                # list of addresses for this use case is still the same.
                # if it's not the same, we need to re-do this day.
                # if it's the same, we need to compare the last block processed against
                # the last block for this day and adjust query.

                # find previously done items
                self.already_done_for_project = self.find_previous_entries_for_project(
                    analysis, project_id
                )

                # loop through all days for this usecase that we need to perform
                for d_date in dates_to_process:
                    dct = None
                    queue = []
                    (
                        do_this_day,
                        redo_this_day,
                        addresses_are_different,
                        addresses_from_project_collection,
                        last_block_processed,
                    ) = self.determine_if_day_needs_to_be_done(project, d_date)

                    _id = f"{d_date}-{analysis.value}-{project_id}"
                    if do_this_day or redo_this_day:
                        # we need to loop through all addresses separately

                        dct = self.perform_actions_for_project(
                            analysis,
                            project_id,
                            project,
                            d_date,
                            _id,
                            addresses_from_project_collection,
                            last_block_processed,
                            redo_this_day,
                        )

                    if addresses_are_different:
                        # new address(es) with no txs on this day, so only update based_on_addresses.
                        dct = {
                            "_id": _id,
                            "date": d_date,
                            "type": analysis.value,
                            "project": project_id,
                            "based_on_addresses": project["mainnet_addresses"],
                            "tx_type_counts": self.already_done_for_project[d_date][
                                "tx_type_counts"
                            ],
                            "last_block_processed": self.already_done_for_project[
                                d_date
                            ]["last_block_processed"],
                        }

                    if dct:
                        queue.append(
                            ReplaceOne(
                                {"_id": _id},
                                replacement=dct,
                                upsert=True,
                            )
                        )

                    if len(queue) > 0:
                        _ = self.mainnet[Collections.statistics].bulk_write(queue)
                    queue = []
        # when done...
        self.write_queue_to_collection(queue, analysis)

    def determine_if_day_needs_to_be_done(
        self,
        usecase,
        d_date,
    ):
        do_this_day = False
        redo_this_day = False

        addresses_are_different = False
        addresses_from_project_collection = usecase["mainnet_addresses"]
        last_block_processed = -1
        if not (self.already_done_for_project.get(d_date)):
            # this day is not present in the collection
            do_this_day = True
        else:
            # so we have previously done this day, we should only redo this
            # if something has changed in the addresses for this usecase.
            addresses_in_done_day = self.already_done_for_project[d_date][
                "based_on_addresses"
            ]

            if set(addresses_in_done_day) != set(addresses_from_project_collection):
                # if we have removed an address
                # rerun this day

                removed_addresses = list(
                    set(addresses_in_done_day) - set(addresses_from_project_collection)
                )
                redo_this_day = len(removed_addresses) > 0

                # if we have a new address:
                # check if txs for new address, then rerun this day if true, else just update based_on_addresses

                new_addresses = list(
                    set(addresses_from_project_collection) - set(addresses_in_done_day)
                )

                pipeline = [
                    {"$match": {"date": d_date}},
                    {"$match": {"impacted_address_canonical": {"$in": new_addresses}}},
                    {"$project": {"_id": 0, "tx_hash": 1}},
                    {"$limit": 1},
                ]
                result = list(
                    self.mainnet[Collections.impacted_addresses].aggregate(pipeline)
                )
                if len(result) > 0:
                    # new address(es) have txs on this day, so redo.
                    redo_this_day = True
                    addresses_are_different = False
                else:
                    # new address(es) have no new txs on this day, so only update the based_on_addresses.
                    redo_this_day = False
                    addresses_are_different = True
            else:
                # finally, check to see if we have gone through all blocks for the day
                # or if it's today, just run again.
                last_block_processed_collection = self.already_done_for_project[d_date][
                    "last_block_processed"
                ]

                if self.all_days.get(d_date):
                    last_block_for_day = self.all_days[d_date]["height_for_last_block"]
                    if last_block_processed_collection < last_block_for_day:
                        do_this_day = True
                        redo_this_day = False
                        last_block_processed = last_block_processed_collection
                else:
                    # it's today, so still running.
                    do_this_day = True
                    redo_this_day = False
                    last_block_processed = last_block_processed_collection

        return (
            do_this_day,
            redo_this_day,
            addresses_are_different,
            addresses_from_project_collection,
            last_block_processed,
        )

    def find_previous_entries_for_project(self, analysis, project_id):
        pipeline = [
            {"$match": {"type": analysis.value, "project": project_id}},
        ]
        already_done_for_usecase = {
            x["date"]: x
            for x in self.mainnet[Collections.statistics].aggregate(pipeline)
        }

        return already_done_for_usecase

    def perform_actions_for_project(
        self,
        analysis: AnalysisType,
        project_id: str,
        project: dict,
        d_date: str,
        _id: str,
        addresses_from_usecase_collection: list[str],
        last_block_processed: int,
        redo_this_day: bool,
    ):

        console.log(_id)
        addresses_from_usecase_collection_29 = [
            x[:29] for x in addresses_from_usecase_collection
        ]
        s = dt.datetime.now().astimezone(dt.timezone.utc)
        if redo_this_day:
            last_block_processed = -1
        tx_types = {}
        pipeline = [
            {"$match": {"date": d_date}},
            {
                "$match": {
                    "impacted_address_canonical": {
                        "$in": addresses_from_usecase_collection_29
                    }
                }
            },
            {"$match": {"block_height": {"$gt": last_block_processed}}},
            {"$project": {"_id": 0, "tx_hash": 1, "block_height": 1}},
        ]
        result = list(self.mainnet[Collections.impacted_addresses].aggregate(pipeline))
        tx_hashes = list(set([x["tx_hash"] for x in result if "tx_hash" in x]))
        if d_date not in self.all_days:
            # it's today, so we need to calculate the max_height
            last_block_processed = (
                max([x["block_height"] for x in result]) if len(result) > 0 else 0
            )
            # if last_block_processed == 0:
            #     last_block_processed = self.all_days[-1]["height_for_last_block"]

        else:
            last_block_processed = self.all_days[d_date]["height_for_last_block"]

        if self.already_done_for_project.get(d_date) and not redo_this_day:
            tx_types = Counter(self.already_done_for_project[d_date]["tx_type_counts"])
        else:
            tx_types = Counter()
        if len(tx_hashes):
            for index, tx_hash_batch in enumerate(
                more_itertools.chunked(tx_hashes, 10_000)
            ):
                e = dt.datetime.now().astimezone(dt.timezone.utc)
                console.log(
                    f"{d_date}: # = {len(tx_hash_batch):,.0f} / {len(tx_hashes):,.0f} in {(e-s).total_seconds():,.4}s"
                )
                s = dt.datetime.now().astimezone(dt.timezone.utc)
                # now we will do a count of transaction types from these hashes
                pipeline = [
                    {"$match": {"_id": {"$in": tx_hash_batch}}},
                    {"$sortByCount": "$type.contents"},
                ]
                dd = list(self.mainnet[Collections.transactions].aggregate(pipeline))
                if len(dd) > 0:
                    tx_types += Counter({x["_id"]: x["count"] for x in dd})

                e = dt.datetime.now().astimezone(dt.timezone.utc)
                console.log(
                    f"{d_date}: sortByCount batch {index+1} (cum. {((index+1)*10_000):,.0f}) in {(e-s).total_seconds():,.4}s"
                )

        dct = {
            "_id": _id,
            "date": d_date,
            "type": analysis.value,
            "project": project_id,
            "based_on_addresses": project["mainnet_addresses"],
            "tx_type_counts": dict(tx_types),
            "last_block_processed": last_block_processed,
        }

        return dct
