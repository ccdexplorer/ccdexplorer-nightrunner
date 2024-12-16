from ccdexplorer_fundamentals.mongodb import Collections
from pymongo import ReplaceOne
from rich.console import Console
from rich import print
import datetime as dt
from ccdexplorer_fundamentals.cis import (
    MongoTypeLoggedEvent,
    MongoTypeTokenAddress,
    mintEvent,
    burnEvent,
)
from .utils import AnalysisType, Utils
import math

console = Console()


class TVLForTokens(Utils):

    def perform_tvl_for_tokens(self):
        """
        Calculate TVL for all fungible tokens for each day
        We are only going to calculate and store the daily effect,
        ie the total mint and burn for the day.
        The graph method will do a cumsum on these to get to
        tvl per day.
        """
        analysis = AnalysisType.statistics_tvl_for_tokens
        self.fungible_tokens_with_markup = self.get_fungible_tokens_with_markup()
        self.historical_exchange_rates = self.get_historical_rates()

        # these dates are when logged events did not exist yet.
        # so no need to search for them.

        queue = []
        no_fx = []
        for token_address in self.fungible_tokens_with_markup.keys():
            dates_to_process = (
                self.find_dates_to_process_for_nightly_statistics_for_tvl(
                    analysis, token_address
                )
            )

            for d_date in dates_to_process:
                _id = f"{d_date}-{analysis.value}-{token_address}"
                console.log(_id)
                token_address_with_markup: MongoTypeTokenAddress = (
                    self.fungible_tokens_with_markup.get(token_address)
                )
                get_price_from = (
                    token_address_with_markup.tag_information.get_price_from
                )
                fungible_token = token_address_with_markup.tag_information.token_tag_id

                height_for_first_block, height_for_last_block = (
                    self.get_start_end_block_from_date(d_date)
                )
                pipeline = [
                    {"$match": {"token_address": token_address}},
                    {"$match": {"tag": {"$in": [253, 254]}}},
                    {
                        "$match": {
                            "block_height": {
                                "$gte": height_for_first_block,
                                "$lte": height_for_last_block,
                            }
                        }
                    },
                ]
                mongo_result = self.mainnet[Collections.tokens_logged_events].aggregate(
                    pipeline
                )
                event_list = [MongoTypeLoggedEvent(**x) for x in mongo_result]
                tvl_contribution_for_day_in_usd = 0
                if len(event_list) > 0:
                    for event in event_list:
                        if event.tag == 254:
                            result = mintEvent(**event.result)
                        elif event.tag == 253:
                            result = burnEvent(**event.result)
                        else:
                            exit("huh")
                        if (token_amount := int(result.token_amount)) > 0:
                            real_token_amount = token_amount * (
                                math.pow(
                                    10,
                                    -token_address_with_markup.tag_information.decimals,
                                )
                            )

                            if self.historical_exchange_rates.get(get_price_from):
                                exchange_rate_for_day = self.historical_exchange_rates[
                                    get_price_from
                                ].get(d_date)
                                if not exchange_rate_for_day:
                                    no_fx.append(
                                        {"date": d_date, "token": fungible_token}
                                    )
                                    console.log(
                                        f"{d_date}-{fungible_token} - No exchange rate found."
                                    )
                                else:
                                    if event.tag == 253:  # burn
                                        tvl_contribution_for_day_in_usd -= (
                                            real_token_amount * exchange_rate_for_day
                                        )
                                    elif event.tag == 254:  # mint
                                        tvl_contribution_for_day_in_usd += (
                                            real_token_amount * exchange_rate_for_day
                                        )

                dct = {
                    "_id": _id,
                    "date": d_date,
                    "type": analysis.value,
                    "fungible_token": fungible_token,
                    "token_address": token_address,
                    "tvl_contribution_for_day_in_usd": tvl_contribution_for_day_in_usd,
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
        print(no_fx)
