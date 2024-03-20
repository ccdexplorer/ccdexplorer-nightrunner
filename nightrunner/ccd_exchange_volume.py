from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from ccdefundamentals.GRPCClient import GRPCClient
import dateutil
import requests
import datetime as dt
from pymongo import ReplaceOne
from ccdefundamentals.mongodb import (
    MongoDB,
    Collections,
    MongoMotor,
)
from pymongo.collection import Collection

console = Console()


class CCDExchangeVolume(Utils):

    def get_dates_with_vol(self) -> list[str]:
        return [
            x["date"]
            for x in self.mainnet[Collections.statistics].find(
                {"type": AnalysisType.statistics_ccd_volume.value}
            )
            if int(x["vol_ccd"]) > 0
        ]

    def perform_statistics_ccd_volume(self):
        self.repo: Repo
        self.grpcclient: GRPCClient
        analysis = AnalysisType.statistics_ccd_volume
        dates_to_process = self.find_dates_to_process(analysis)
        dated_already_done = self.get_dates_with_vol()
        dates_to_do = sorted(list(set(dates_to_process) - set(dated_already_done)))
        listing_day = dt.datetime(2022, 2, 10)
        dates_in_datetime_format = [dateutil.parser.parse(x) for x in dates_to_do]
        dates_in_coingecko_format = [
            f"{x:%d-%m-%Y}" for x in dates_in_datetime_format if x >= listing_day
        ]
        queue = []

        for day in dates_in_coingecko_format:
            date_in_my_format = (
                f"{dateutil.parser.parse(day, yearfirst=True, dayfirst=True):%Y-%m-%d}"
            )

            a = requests.get(
                f"https://api.coingecko.com/api/v3/coins/concordium/history?date={day}&localization=false"
            )
            if a.status_code == 200:
                ccd_usd = a.json()["market_data"]["current_price"]["usd"]
                vol_usd = a.json()["market_data"]["total_volume"]["usd"]
                ccd_eur = a.json()["market_data"]["current_price"]["eur"]
                vol_eur = a.json()["market_data"]["total_volume"]["eur"]
                vol_ccd = vol_usd / ccd_usd

            # fix for coingecko not having the right data.
            if date_in_my_format == "2022-02-11":
                vol_ccd = 16_827_000

            _id = f"{date_in_my_format}-{analysis.value}"
            console.log(_id)

            dct = {
                "_id": _id,
                "type": analysis.value,
                "date": date_in_my_format,
                "ccd_usd": f"{ccd_usd:.6f}",
                "ccd_eur": f"{ccd_eur:.6f}",
                "vol_usd": f"{vol_usd:.0f}",
                "vol_eur": f"{vol_eur:.0f}",
                "vol_ccd": f"{vol_ccd:.0f}",
                "label": "Total Volume",
            }
            # print(dct)

            queue.append(
                ReplaceOne(
                    {"_id": _id},
                    replacement=dct,
                    upsert=True,
                )
            )
        self.write_queue_to_collection(queue, analysis)
