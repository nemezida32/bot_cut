"""
Фаза 1: Глобальный анализ всех монет
Цель: Отобрать топ-50 монет из 250 для дальнейшего анализа
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict

from ..core.data_manager import DataManager, MarketDataAggregator


class Phase1GlobalAnalyzer:
    """
    Глобальный анализатор для быстрого скрининга всех монет
    Выполняется каждые 5 минут, должен работать < 5 секунд
    """
    
    def __init__(self, config: dict, data_manager: DataManager):
        self.config = config
        self.data_manager = data_manager
        self.market_aggregator = MarketDataAggregator(data_manager)
        self.logger = logging.getLogger(__name__)
        
        # Настройки фильтров
        self.filters = config['filters']
        
        # Кеш для оптимизации
        self.sector_cache = {}
        self.market_type_cache = None
        self.last_market_classification = None
        
    async def analyze_batch(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Анализ батча символов параллельно
        
        Args:
            symbols: Список символов для анализа
            
        Returns:
            Dict с результатами анализа
        """
        try:
            # Получаем базовые данные для всех символов
            tickers = await self.data_manager.get_tickers_24h_batch(symbols)
            
            if not tickers:
                return {'symbol_scores': [], 'batch_stats': {}}
                
            # Фильтруем по базовым критериям
            filtered_symbols = self._apply_basic_filters(tickers)
            
            # Анализируем каждый символ
            tasks = []
            for ticker in filtered_symbols:
                task = asyncio.create_task(
                    self._analyze_symbol(ticker['symbol'], ticker)
                )
                tasks.append(task)
                
            # Ждем результаты
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Собираем валидные результаты
            symbol_scores = []
            for result in results:
                if isinstance(result, dict) and result.get('score', 0) > 0:
                    symbol_scores.append(result)
                    
            # Сортируем по score
            symbol_scores.sort(key=lambda x: x['score'], reverse=True)
            
            return {
                'symbol_scores': symbol_scores,
                'batch_stats': {
                    'total_symbols': len(symbols),
                    'filtered_symbols': len(filtered_symbols),
                    'analyzed_symbols': len(symbol_scores)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Batch analysis error: {e}")
            return {'symbol_scores': [], 'batch_stats': {}}
            
    def _apply_basic_filters(self, tickers: List[dict]) -> List[dict]:
        """Применение базовых фильтров"""
        filtered = []
        
        for ticker in tickers:
            try:
                # Объемный фильтр
                volume_24h = float(ticker['quoteVolume'])
                if volume_24h < self.filters['volume']['min_24h_volume_usdt']:
                    continue
                    
                # Ценовой фильтр
                price = float(ticker['lastPrice'])
                if (price < self.filters['price']['min_price'] or 
                    price > self.filters['price']['max_price']):
                    continue
                    
                # Фильтр изменения цены
                price_change = float(ticker['priceChangePercent'])
                if (price_change < self.filters['price']['min_price_change_24h'] or
                    price_change > self.filters['price']['max_price_change_24h']):
                    continue
                    
                # Исключенные символы
                if ticker['symbol'] in self.config['scanner']['excluded_symbols']:
                    continue
                    
                filtered.append(ticker)
                
            except Exception as e:
                self.logger.debug(f"Filter error for {ticker.get('symbol')}: {e}")
                continue
                
        return filtered
        
    async def _analyze_symbol(self, symbol: str, ticker_data: dict) -> Dict[str, Any]:
        """
        Анализ одного символа
        
        Returns:
            Dict с score и метриками
        """
        try:
            # Базовые метрики из тикера
            volume_24h = float(ticker_data['quoteVolume'])
            price_change = float(ticker_data['priceChangePercent'])
            count_24h = int(ticker_data['count'])
            
            # Получаем дополнительные данные параллельно
            tasks = [
                self.data_manager.get_klines(symbol, '1h', limit=24),
                self.data_manager.get_klines(symbol, '4h', limit=12),
                self._get_volume_profile(symbol)
            ]
            
            klines_1h, klines_4h, volume_profile = await asyncio.gather(*tasks)
            
            if not klines_1h or not klines_4h:
                return {'symbol': symbol, 'score': 0}
                
            # Расчет метрик
            metrics = {
                'volume_score': self._calculate_volume_score(
                    volume_24h, volume_profile, count_24h
                ),
                'momentum_score': self._calculate_momentum_score(
                    klines_1h, klines_4h, price_change
                ),
                'liquidity_score': await self._calculate_liquidity_score(symbol),
                'trend_score': self._calculate_trend_score(klines_4h),
                'volatility_score': self._calculate_volatility_score(klines_1h),
                'sector_bonus': self._get_sector_bonus(symbol)
            }
            
            # Итоговый score
            total_score = self._calculate_total_score(metrics)
            
            return {
                'symbol': symbol,
                'score': total_score,
                'price_change_24h': price_change,
                'volume_24h': volume_24h,
                'metrics': metrics,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.debug(f"Symbol analysis error for {symbol}: {e}")
            return {'symbol': symbol, 'score': 0}
            
    def _calculate_volume_score(self, volume_24h: float, volume_profile: dict, 
                               count_24h: int) -> float:
        """Оценка объема и активности"""
        score = 0.0
        
        # Базовый объем (логарифмическая шкала)
        if volume_24h > 10_000_000:
            score += 30
        elif volume_24h > 5_000_000:
            score += 20
        elif volume_24h > 1_000_000:
            score += 10
            
        # Активность торгов
        if count_24h > 100_000:
            score += 10
        elif count_24h > 50_000:
            score += 5
            
        # Профиль объема (увеличение)
        if volume_profile.get('trend') == 'increasing':
            score += 10
        elif volume_profile.get('trend') == 'spike':
            score += 15
            
        return min(score, 40)  # Максимум 40 баллов
        
    def _calculate_momentum_score(self, klines_1h: List, klines_4h: List, 
                                 price_change_24h: float) -> float:
        """Оценка моментума"""
        score = 0.0
        
        # Краткосрочный моментум (последние 6 часов)
        recent_movement = self._calculate_recent_movement(klines_1h[-6:])
        if recent_movement > 2:
            score += 15
        elif recent_movement > 1:
            score += 10
        elif recent_movement > 0.5:
            score += 5
            
        # Направление движения
        trend_strength = self._calculate_trend_strength(klines_4h)
        if trend_strength > 0.7:
            score += 10
        elif trend_strength > 0.5:
            score += 5
            
        # Ускорение
        if self._is_accelerating(klines_1h):
            score += 10
            
        # Бонус за умеренное 24h изменение (не перекупленность)
        if 0 < price_change_24h < 10:
            score += 5
            
        return min(score, 30)  # Максимум 30 баллов
        
    async def _calculate_liquidity_score(self, symbol: str) -> float:
        """Оценка ликвидности"""
        try:
            orderbook = await self.data_manager.get_orderbook(symbol, limit=20)
            
            if not orderbook:
                return 0
                
            # Глубина стакана
            bid_depth = sum(float(bid[1]) * float(bid[0]) for bid in orderbook['bids'])
            ask_depth = sum(float(ask[1]) * float(ask[0]) for ask in orderbook['asks'])
            total_depth = bid_depth + ask_depth
            
            score = 0.0
            
            # Оценка глубины
            if total_depth > 100_000:
                score += 10
            elif total_depth > 50_000:
                score += 5
                
            # Баланс стакана
            balance = abs(bid_depth - ask_depth) / total_depth if total_depth > 0 else 1
            if balance < 0.2:  # Хорошо сбалансирован
                score += 5
                
            # Спред
            best_bid = float(orderbook['bids'][0][0])
            best_ask = float(orderbook['asks'][0][0])
            spread_percent = (best_ask - best_bid) / best_bid * 100
            
            if spread_percent < 0.05:
                score += 5
            elif spread_percent < 0.1:
                score += 3
                
            return min(score, 20)  # Максимум 20 баллов
            
        except Exception as e:
            self.logger.debug(f"Liquidity calculation error: {e}")
            return 0
            
    def _calculate_trend_score(self, klines_4h: List) -> float:
        """Оценка тренда на 4h"""
        if len(klines_4h) < 12:
            return 0
            
        closes = [float(k[4]) for k in klines_4h]
        
        # MA анализ
        ma_short = np.mean(closes[-3:])
        ma_long = np.mean(closes[-12:])
        
        score = 0.0
        
        # Позиция цены относительно MA
        current_price = closes[-1]
        if current_price > ma_short > ma_long:
            score += 5  # Восходящий тренд
            
        # Сила тренда
        trend_strength = (ma_short - ma_long) / ma_long * 100
        if abs(trend_strength) > 5:
            score += 5
            
        return min(score, 10)  # Максимум 10 баллов
        
    def _calculate_volatility_score(self, klines_1h: List) -> float:
        """Оценка волатильности (ищем оптимальную)"""
        if len(klines_1h) < 24:
            return 0
            
        # ATR calculation
        atr_values = []
        for i in range(1, len(klines_1h)):
            high = float(klines_1h[i][2])
            low = float(klines_1h[i][3])
            close_prev = float(klines_1h[i-1][4])
            
            tr = max(high - low, abs(high - close_prev), abs(low - close_prev))
            atr_values.append(tr)
            
        atr = np.mean(atr_values[-14:]) if len(atr_values) >= 14 else 0
        current_price = float(klines_1h[-1][4])
        atr_percent = (atr / current_price * 100) if current_price > 0 else 0
        
        # Оптимальная волатильность 1-3%
        if 1 <= atr_percent <= 3:
            return 10
        elif 0.5 <= atr_percent <= 5:
            return 5
        else:
            return 0
            
    def _get_sector_bonus(self, symbol: str) -> float:
        """Бонус за принадлежность к сильному сектору"""
        # Здесь будет интеграция с анализом секторов
        # Пока упрощенная логика
        strong_sectors = ['AI', 'GAMING', 'DEFI']
        
        symbol_upper = symbol.upper()
        
        # AI сектор
        if any(ai in symbol_upper for ai in ['AI', 'FET', 'AGIX', 'OCEAN', 'NMR']):
            return 5
            
        # Gaming
        if any(game in symbol_upper for game in ['AXS', 'SAND', 'MANA', 'GALA', 'IMX']):
            return 5
            
        # DeFi
        if any(defi in symbol_upper for defi in ['AAVE', 'UNI', 'SUSHI', 'COMP', 'CRV']):
            return 5
            
        return 0
        
    def _calculate_total_score(self, metrics: dict) -> float:
        """Расчет итогового score"""
        weights = {
            'volume_score': 1.0,
            'momentum_score': 1.2,  # Приоритет на моментум
            'liquidity_score': 0.8,
            'trend_score': 0.7,
            'volatility_score': 0.5,
            'sector_bonus': 0.6
        }
        
        total = sum(metrics[key] * weights[key] for key in metrics)
        
        # Нормализация к 100
        max_possible = sum(
            40 * weights['volume_score'],
            30 * weights['momentum_score'],
            20 * weights['liquidity_score'],
            10 * weights['trend_score'],
            10 * weights['volatility_score'],
            5 * weights['sector_bonus']
        )
        
        return round((total / max_possible) * 100, 2)
        
    async def _get_volume_profile(self, symbol: str) -> dict:
        """Анализ профиля объема"""
        try:
            # Получаем часовые свечи за последние 24 часа
            klines = await self.data_manager.get_klines(symbol, '1h', limit=24)
            
            if not klines or len(klines) < 24:
                return {'trend': 'unknown'}
                
            volumes = [float(k[5]) for k in klines]
            
            # Средний объем
            avg_volume = np.mean(volumes[:-6])  # Исключаем последние 6 часов
            recent_avg = np.mean(volumes[-6:])  # Последние 6 часов
            
            # Определяем тренд
            if recent_avg > avg_volume * 2:
                return {'trend': 'spike', 'ratio': recent_avg / avg_volume}
            elif recent_avg > avg_volume * 1.5:
                return {'trend': 'increasing', 'ratio': recent_avg / avg_volume}
            else:
                return {'trend': 'normal', 'ratio': recent_avg / avg_volume}
                
        except Exception as e:
            self.logger.debug(f"Volume profile error: {e}")
            return {'trend': 'unknown'}
            
    def _calculate_recent_movement(self, klines: List) -> float:
        """Расчет недавнего движения в %"""
        if not klines:
            return 0
            
        first_close = float(klines[0][4])
        last_close = float(klines[-1][4])
        
        return abs((last_close - first_close) / first_close * 100)
        
    def _calculate_trend_strength(self, klines: List) -> float:
        """Сила тренда от 0 до 1"""
        if len(klines) < 3:
            return 0
            
        closes = [float(k[4]) for k in klines]
        
        # Считаем последовательные движения в одном направлении
        ups = 0
        downs = 0
        
        for i in range(1, len(closes)):
            if closes[i] > closes[i-1]:
                ups += 1
            else:
                downs += 1
                
        # Сила = доминирование одного направления
        total = ups + downs
        if total == 0:
            return 0
            
        return abs(ups - downs) / total
        
    def _is_accelerating(self, klines: List) -> bool:
        """Проверка ускорения движения"""
        if len(klines) < 6:
            return False
            
        # Сравниваем изменения за последние периоды
        closes = [float(k[4]) for k in klines]
        
        # Изменение за последние 2 часа vs предыдущие 2 часа
        recent_change = abs(closes[-1] - closes[-3]) / closes[-3]
        previous_change = abs(closes[-3] - closes[-5]) / closes[-5]
        
        return recent_change > previous_change * 1.5
        
    async def classify_market(self, batch_results: List[dict]) -> str:
        """
        Классификация текущего состояния рынка
        
        Returns:
            Тип рынка: ACCUMULATION / DISTRIBUTION / TRENDING / RANGING / VOLATILE
        """
        try:
            # Получаем обзор рынка
            market_overview = await self.market_aggregator.get_market_overview()
            
            if not market_overview:
                return "UNKNOWN"
                
            # Анализируем метрики
            total_volume = market_overview['total_volume_24h']
            avg_price_change = market_overview['average_price_change']
            positive_ratio = market_overview['positive_symbols'] / market_overview['total_symbols']
            
            # Получаем данные по BTC
            btc_data = await self._get_btc_analysis()
            
            # Классификация
            if btc_data['volatility'] > 3:
                return "VOLATILE"
                
            if positive_ratio > 0.7 and avg_price_change > 2:
                if btc_data['trend'] == 'strong_up':
                    return "TRENDING_UP"
                else:
                    return "DISTRIBUTION"  # Альты растут, BTC слабый
                    
            if positive_ratio < 0.3 and avg_price_change < -2:
                if total_volume < btc_data['avg_volume'] * 0.7:
                    return "ACCUMULATION"  # Низкий объем, падение
                else:
                    return "TRENDING_DOWN"
                    
            if 0.4 <= positive_ratio <= 0.6 and abs(avg_price_change) < 1:
                return "RANGING"
                
            # По умолчанию
            return "NORMAL_TRENDING"
            
        except Exception as e:
            self.logger.error(f"Market classification error: {e}")
            return "UNKNOWN"
            
    async def _get_btc_analysis(self) -> dict:
        """Анализ BTC для понимания рынка"""
        try:
            # Получаем данные BTC
            btc_ticker = await self.data_manager.get_ticker_24h('BTCUSDT')
            btc_klines = await self.data_manager.get_klines('BTCUSDT', '4h', limit=24)
            
            if not btc_ticker or not btc_klines:
                return {
                    'trend': 'unknown',
                    'volatility': 0,
                    'avg_volume': 0
                }
                
            # Тренд
            closes = [float(k[4]) for k in btc_klines]
            ma_short = np.mean(closes[-6:])
            ma_long = np.mean(closes)
            
            if ma_short > ma_long * 1.02:
                trend = 'strong_up'
            elif ma_short > ma_long:
                trend = 'up'
            elif ma_short < ma_long * 0.98:
                trend = 'strong_down'
            else:
                trend = 'down'
                
            # Волатильность
            volatility = np.std(closes) / np.mean(closes) * 100
            
            # Средний объем
            volumes = [float(k[5]) for k in btc_klines]
            avg_volume = np.mean(volumes)
            
            return {
                'trend': trend,
                'volatility': volatility,
                'avg_volume': avg_volume,
                'price_change_24h': float(btc_ticker['priceChangePercent'])
            }
            
        except Exception as e:
            self.logger.error(f"BTC analysis error: {e}")
            return {
                'trend': 'unknown',
                'volatility': 0,
                'avg_volume': 0
            }
            
    def update_config(self, new_config: dict):
        """Обновление конфигурации на лету"""
        if 'filters' in new_config:
            self.filters.update(new_config['filters'])
            self.logger.info("Filters updated in Phase1 analyzer")