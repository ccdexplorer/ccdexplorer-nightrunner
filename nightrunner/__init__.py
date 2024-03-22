# ruff: noqa: F403, F405, E402, E501, E722
from git import Repo
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from ccdexplorer_fundamentals.GRPCClient.CCD_Types import *
from ccdexplorer_fundamentals.tooter import Tooter
from ccdexplorer_fundamentals.mongodb import (
    MongoDB,
    Collections,
    CollectionsUtilities,
    MongoMotor,
)

from pymongo.collection import Collection
from ccdexplorer_fundamentals.tooter import TooterChannel, TooterType
from env import *
from rich.console import Console
import urllib3

from .holders import Holders as _holders
from .limits import Limits as _limits
from .network_summary import NetworkSummary as _network_summary
from .pools import Pools as _pools
from .microccd import MicroCCD as _microccd
from .ccd_classified import Classified as _classified
from .exchange_wallets import ExchangeWallets as _exchange_wallets
from .ccd_exchange_volume import CCDExchangeVolume as _ccd_exchange_volume
from .release_amounts import ReleaseAmounts as _release_amounts
from .mongo_transactions import MongoTransactions as _mongo_transactions
from .mongo_tps_table import MongoTPSTable as _mongo_tps_table
from .mongo_accounts_table import MongoAccountsTable as _mongo_accounts_table
from .network_activity import NetworkActivity as _network_activity
from .transaction_fees_over_time import TransactionFees as _transaction_fees

urllib3.disable_warnings()
console = Console()


class NightRunner(
    _holders,
    _limits,
    _network_summary,
    _pools,
    _microccd,
    _classified,
    _exchange_wallets,
    _ccd_exchange_volume,
    _release_amounts,
    _mongo_transactions,
    _network_activity,
    _mongo_tps_table,
    _mongo_accounts_table,
    _transaction_fees,
):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        mongodb: MongoDB,
        motormongo: MongoMotor,
    ):
        self.grpcclient = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.motormongo = motormongo
        self.mainnet: dict[Collections, Collection] = self.mongodb.mainnet
        self.testnet: dict[Collections, Collection] = self.mongodb.testnet
        self.utilities: dict[CollectionsUtilities, Collection] = self.mongodb.utilities

        self.motor_mainnet: dict[Collections, Collection] = self.motormongo.mainnet
        self.motor_testnet: dict[Collections, Collection] = self.motormongo.testnet
        self.find_repo()
        self.repo_pull()

    def find_repo(self):
        ON_SERVER = os.environ.get("ON_SERVER", False)

        print(f"{ON_SERVER=}.")
        repo_dir = REPO_DIR
        if ON_SERVER:
            repo_dir = "/home/git_dir"

        self.repo = Repo(repo_dir)

    def repo_pull(self):
        origin = self.repo.remote(name="origin")
        _ = origin.pull()

    def inform(self, message):
        self.tooter.send(
            channel=TooterChannel.NOTIFIER,
            message=f"Pre-renderer: {message}",
            notifier_type=TooterType.REQUESTS_ERROR,
        )
