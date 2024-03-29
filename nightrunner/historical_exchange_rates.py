from git import Repo
from rich.console import Console
from .utils import Utils, AnalysisType
from pymongo import ReplaceOne
from ccdexplorer_fundamentals.mongodb import Collections, CollectionsUtilities
from ccdexplorer_fundamentals.tooter import Tooter, TooterChannel, TooterType
from env import COIN_GECKO_API_KEY
import datetime as dt
from datetime import timezone
import httpx
import time

console = Console()


class HistoricalExchangeRates(Utils):
    def get_token_translations_from_mongo(self):
        result = self.utilities[CollectionsUtilities.token_api_translations].find(
            {"service": "coingecko"}
        )
        self.coingecko_token_translation = {
            x["token"]: x["translation"] for x in list(result)
        }

    def coingecko_historical(self, token: str):
        """
        This is the implementation of the CoinGecko historical API.
        """
        token_to_request = self.coingecko_token_translation.get(token)

        return_list_for_token = []
        # only for tokens that we know we can request
        if token_to_request:
            url = f"https://api.coingecko.com/api/v3/coins/{token_to_request}/market_chart?vs_currency=usd&days=3&interval=daily&precision=full&x_cg_demo_api_key={COIN_GECKO_API_KEY}"
            with httpx.Client(
                # headers={"x-cg-demo-api-key": COIN_GECKO_API_KEY}
            ) as client:
                response = client.get(url)
                if response.status_code == 200:
                    result = response.json()
                    result = result["prices"]
                    console.log(
                        f"Historic: {token} | {token_to_request} | {response.status_code} | {len(result)} days"
                    )
                    for timestamp, price in result:
                        formatted_date = f"{dt.datetime.fromtimestamp(timestamp/1000, tz=timezone.utc):%Y-%m-%d}"
                        return_dict = {
                            "_id": f"USD/{token}-{formatted_date}",
                            "token": token,
                            "timestamp": timestamp,
                            "date": formatted_date,
                            "rate": price,
                            "source": "CoinGecko",
                        }
                        return_list_for_token.append(
                            ReplaceOne(
                                {"_id": f"USD/{token}-{formatted_date}"},
                                return_dict,
                                upsert=True,
                            )
                        )
                    # sleep to prevent from being rate-limited.
                    time.sleep(30)
                else:
                    console.log(
                        f"{token} | {token_to_request} | {response.status_code}"
                    )
                    return_dict = None

        return return_list_for_token

    def perform_statistics_historical_exchange_rates(self):
        self.repo: Repo
        analysis = AnalysisType.statistics_historical_exchange_rates
        now = dt.datetime.now().astimezone(dt.timezone.utc)
        hour = now.hour
        minute = now.minute

        is_between_0050_0059 = (hour == 0) and (50 <= minute <= 59)
        # is_between_0050_0059 = True
        if is_between_0050_0059:
            self.get_token_translations_from_mongo()

            try:
                token_list = [
                    x["_id"].replace("w", "")
                    for x in self.mainnet[Collections.tokens_tags].find(
                        {"token_type": "fungible"}
                    )
                ]

                queue = []
                for token in reversed(token_list):
                    _log = f"{token}-{analysis.value}"
                    console.log(_log)

                    queue = self.coingecko_historical(token)

                    if len(queue) > 0:
                        _ = self.utilities[
                            CollectionsUtilities.exchange_rates_historical
                        ].bulk_write(queue)

                        # update exchange rates retrieval
                        query = {
                            "_id": "heartbeat_last_timestamp_exchange_rates_historical"
                        }
                        self.mainnet[Collections.helpers].replace_one(
                            query,
                            {
                                "_id": "heartbeat_last_timestamp_exchange_rates_historical",
                                "timestamp": dt.datetime.now().astimezone(
                                    dt.timezone.utc
                                ),
                            },
                            upsert=True,
                        )

            except Exception as e:
                self.tooter: Tooter
                self.tooter.send(
                    channel=TooterChannel.NOTIFIER,
                    message=f"Recurring: Failed to get exchange rates historical. Error: {e}",
                    notifier_type=TooterType.REQUESTS_ERROR,
                )
