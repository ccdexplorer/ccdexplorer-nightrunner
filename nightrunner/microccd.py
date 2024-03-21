from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from pymongo import ReplaceOne


console = Console()


class MicroCCD(Utils):

    def perform_statistics_microccd(self):
        self.repo: Repo
        self.grpcclient: GRPCClient
        analysis = AnalysisType.statistics_microccd
        dates_to_process = self.find_dates_to_process(analysis)
        queue = []
        commits = reversed(list(self.repo.iter_commits("main")))
        for commit in commits:
            d_date = self.get_date_from_git(commit)
            if d_date in dates_to_process:
                _id = f"{d_date}-{analysis.value}"
                console.log(_id)
                block_hash = self.get_hash_from_date(d_date)
                cp = self.grpcclient.get_block_chain_parameters(block_hash)
                if cp.v0:
                    version = cp.v0
                if cp.v1:
                    version = cp.v1
                if cp.v2:
                    version = cp.v2

                # these are retrieved as string and stored as string in MongoDB,
                # as they are larger than 8-bit.
                GTU_denominator = version.micro_ccd_per_euro.denominator
                GTU_numerator = version.micro_ccd_per_euro.numerator

                NRG_denominator = version.euro_per_energy.denominator
                NRG_numerator = version.euro_per_energy.numerator

                dct = {
                    "_id": _id,
                    "type": analysis.value,
                    "date": d_date,
                    "GTU_denominator": GTU_denominator,
                    "GTU_numerator": GTU_numerator,
                    "NRG_denominator": NRG_denominator,
                    "NRG_numerator": NRG_numerator,
                }

                queue.append(
                    ReplaceOne(
                        {"_id": _id},
                        replacement=dct,
                        upsert=True,
                    )
                )
        self.write_queue_to_collection(queue, analysis)
