import logging
from datetime import datetime, timedelta, timezone

import ccxt

from freqtrade.constants import BuySell
from freqtrade.enums import CandleType, MarginMode, PriceType, TradingMode
from freqtrade.exceptions import DDosProtection, ExchangeError, OperationalException, TemporaryError
from freqtrade.exchange import Exchange
from freqtrade.exchange.common import retrier
from freqtrade.exchange.exchange_types import FtHas, OHLCVResponse


logger = logging.getLogger(__name__)


class Bitget(Exchange):
    """
    Bitget 交易所类。包含 Freqtrade 与此交易所配合使用所需的调整。

    请注意，此交易所不在 Freqtrade 开发团队正式支持的交易所列表中。
    因此某些功能可能仍无法按预期工作。
    """

    _ft_has: FtHas = {
        "ohlcv_candle_limit": 1000,
        "ohlcv_has_history": True,
        "order_time_in_force": ["GTC", "FOK", "IOC"],
        "trades_has_history": True,
    }
    _ft_has_futures: FtHas = {
        "ohlcv_has_history": True,
        "mark_ohlcv_timeframe": "4h",
        "funding_fee_timeframe": "8h",
        "stoploss_on_exchange": True,
        "stoploss_order_types": {"limit": "limit", "market": "market"},
        "stop_price_prop": "stopPrice",
        "stop_price_type_field": "triggerBy",
        "stop_price_type_value_mapping": {
            PriceType.LAST: "last_price",
            PriceType.MARK: "mark_price",
            PriceType.INDEX: "index_price",
        },
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.FUTURES, MarginMode.ISOLATED)
    ]



    @property
    def _ccxt_config(self) -> dict:
        # 获取 ccxt 配置
        config = {}
        if self.trading_mode == TradingMode.FUTURES:
            config.update({"options": {"defaultType": "swap"}})
        config.update(super()._ccxt_config)
        return config

    @retrier
    def additional_exchange_init(self) -> None:
        # 初始化 Bitget 交易所的额外设置，并处理可能的异常
        try:
            if not self._config["dry_run"]:
                if self.trading_mode == TradingMode.FUTURES:
                    # 设置仓位模式为单向（hedged = False）
                    self._api.set_position_mode(False, None, {"productType": "USDT-FUTURES"})
                    logger.info("Bitget: Position mode set to one-way.")

        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Error in additional_exchange_init due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def _get_params(
        self,
        side: BuySell,
        ordertype: str,
        leverage: float,
        reduceOnly: bool,
        time_in_force: str = "GTC",
    ) -> dict:
        # 获取订单参数，包括杠杆和保证金模式等
        params = super()._get_params(
            side=side,
            ordertype=ordertype,
            leverage=leverage,
            reduceOnly=reduceOnly,
            time_in_force=time_in_force,
        )
        if self.trading_mode == TradingMode.FUTURES and self.margin_mode:
            params["marginMode"] = self.margin_mode.lower()
        return params

    def timeframe_to_milliseconds(self, timeframe: str) -> int:
        # 将时间框架转换为毫秒
        return ccxt.Exchange.parse_timeframe(timeframe) * 1000

    async def _async_get_historic_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since_ms: int,
        candle_type: CandleType,
        raise_: bool = False,
        until_ms: int | None = None,
    ) -> OHLCVResponse:
        # 异步获取历史 OHLCV 数据
        try:
            pair_data = await super()._async_get_historic_ohlcv(
                pair, timeframe, since_ms, candle_type, raise_, until_ms
            )

            pair, _, candle_type, data, partial_candle = pair_data

            if candle_type not in (CandleType.FUNDING_RATE):
                current_time = int(datetime.now(timezone.utc).timestamp() * 1000)
                timeframe_ms = self.timeframe_to_milliseconds(timeframe)
                last_candle_time = data[-1][0] if data else 0

                if current_time - last_candle_time >= timeframe_ms:
                    newest_candle_start = current_time - (current_time % timeframe_ms)

                    params = {}
                    if candle_type == CandleType.MARK:
                        params["price"] = "mark"
                    elif candle_type == CandleType.INDEX:
                        params["price"] = "index"

                    latest_candles = await self._api_async.fetch_ohlcv(
                        pair, timeframe, newest_candle_start, 1, params
                    )

                    if latest_candles:
                        data.append(latest_candles[0])
                    else:
                        estimated_candle = await self._estimate_current_candle(
                            pair, timeframe, newest_candle_start
                        )
                        if estimated_candle:
                            data.append(estimated_candle)
                        else:
                            logger.warning(f"can't for {pair} get candle of {timeframe}")

            return pair, timeframe, candle_type, data, partial_candle

        except Exception as e:
            logger.error(
                f"params：{pair}, {timeframe}, {since_ms}, {candle_type}, "
                f"can't get_historic_ohlcv: {e}"
            )
            raise

    async def _estimate_current_candle(
        self, pair: str, timeframe: str, start_time: int
    ) -> list[float]:
        # 估算当前蜡烛数据
        timeframe_map: dict[str, tuple[str, int]] = {
            "5m": ("1m", 5),
            "15m": ("3m", 5),
            "30m": ("5m", 6),
            "1h": ("15m", 4),
            "4h": ("1h", 4),
            "6h": ("1h", 6),
            "12h": ("3h", 4),
            "1d": ("6h", 4),
            "1w": ("1d", 7),
            "1m": ("1w", 4),
        }

        if timeframe not in timeframe_map:
            raise Exception(f"{timeframe} not in timeframe list")

        smaller_tf, max_candles = timeframe_map[timeframe]

        try:
            smaller_candles = await self._api_async.fetch_ohlcv(
                pair, smaller_tf, start_time, max_candles
            )

            if not smaller_candles:
                logger.warning(f"can't get  {smaller_tf} candle data for {pair}")
                return []
            open_price = smaller_candles[0][1]
            high_price = max(candle[2] for candle in smaller_candles)
            low_price = min(candle[3] for candle in smaller_candles)
            close_price = smaller_candles[-1][4]
            volume = sum(candle[5] for candle in smaller_candles)
            return [start_time, open_price, high_price, low_price, close_price, volume]

        except Exception as e:
            raise Exception(f"can't for {pair} get candle of {timeframe}: {e}")

    def ohlcv_candle_limit(
        self, timeframe: str, candle_type: CandleType, since_ms: int | None = None
    ) -> int:
        # 获取 OHLCV 蜡烛数据的限制
        if candle_type == CandleType.FUNDING_RATE:
            return 100  # Bitget 似乎将资金费率数据限制为 100 条
        if candle_type == CandleType.MARK:
            return 200
        # 对于其他蜡烛类型，使用默认或先前定义的限制
        return super().ohlcv_candle_limit(timeframe, candle_type, since_ms)

    def get_funding_fees(
        self, pair: str, amount: float, is_short: bool, open_date: datetime
    ) -> float:
        # 获取资金费用
        if self.trading_mode == TradingMode.FUTURES:
            try:
                return self._fetch_and_calculate_funding_fees(pair, amount, is_short, open_date)
            except ExchangeError:
                logger.warning(f"Could not update funding fees for {pair}.")
        return 0.0

    def get_max_pair_stake_amount(self, pair: str, price: float, leverage: float = 1.0) -> float:
        # 获取交易对的最大持仓金额
        if self.trading_mode == TradingMode.SPOT:
            return float("inf")  # 实际上不是无穷大，但这可能对现货交易无关紧要

        if pair not in self._leverage_tiers:
            return float("inf")

        pair_tiers = self._leverage_tiers[pair]
        return pair_tiers[-1]["maxNotional"] / leverage


    def bitget_get_trades_for_order(
        self, order_id: str, pair: str, since: datetime, params: dict | None = None
    ) -> list:
        """
        获取指定订单的交易记录，并确保 since 参数在 95 天之内
        :param order_id: 订单 ID
        :param pair: 交易对
        :param since: 订单创建时间的 datetime 对象，假定为 UTC 时间
        :param params: 额外的参数
        :return: 交易记录列表
        """
        # 确保 since 参数在 95 天之内
        max_days = 95
        max_since = datetime.now(timezone.utc) - timedelta(days=max_days)
        if since < max_since:
            since = max_since

        # 检查交易对格式
        if '/' not in pair:
            raise ValueError(f"Invalid pair format: {pair}. Expected format 'BASE/QUOTE'.")

        # 调用父类的 get_trades_for_order 方法
        logger.info(
            f"Calling get_trades_for_order with order_id={order_id}, pair={pair}, "
            f"since={since}, params={params}"
        )
        trades = super().get_trades_for_order(order_id, pair, since, params)

        # 记录获取到的交易数量
        logger.info(f"2222Fetched {len(trades)} trades for pair: {pair}")

        return trades
