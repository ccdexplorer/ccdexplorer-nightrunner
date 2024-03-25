# ruff: noqa: F403, F405, E402, E501, E722
import httpx
import urllib3
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from ccdexplorer_fundamentals.GRPCClient.CCD_Types import *
from ccdexplorer_fundamentals.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
    MongoMotor,
)
from ccdexplorer_fundamentals.tooter import Tooter, TooterChannel, TooterType
from git import Repo
from pymongo.collection import Collection
from rich.console import Console

from env import *

from .bridges_and_dexes import BridgesAndDexes as _bridges_and_dexes
from .ccd_classified import Classified as _classified
from .ccd_exchange_volume import CCDExchangeVolume as _ccd_exchange_volume
from .exchange_wallets import ExchangeWallets as _exchange_wallets
from .historical_exchange_rates import HistoricalExchangeRates as _exchange_rates
from .holders import Holders as _holders
from .limits import Limits as _limits
from .microccd import MicroCCD as _microccd
from .mongo_accounts_table import MongoAccountsTable as _mongo_accounts_table
from .mongo_tps_table import MongoTPSTable as _mongo_tps_table
from .mongo_transactions import MongoTransactions as _mongo_transactions
from .network_activity import NetworkActivity as _network_activity
from .network_summary import NetworkSummary as _network_summary
from .pools import Pools as _pools
from .release_amounts import ReleaseAmounts as _release_amounts
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
    _bridges_and_dexes,
    _exchange_rates,
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
        self.client = httpx.Client()
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
            message=f"Nightrunner: {message}",
            notifier_type=TooterType.REQUESTS_ERROR,
        )
