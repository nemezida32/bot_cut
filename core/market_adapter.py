"""
Модуль адаптации торговых параметров под текущие рыночные условия
Автоматически корректирует настройки в зависимости от волатильности и типа рынка
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import numpy as np

logger = logging.getLogger(__name__)


class MarketType(Enum):
    """Типы рынка"""
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGING = "ranging"
    VOLATILE = "volatile"
    ACCUMULATION = "accumulation"
    DISTRIBUTION = "distribution"


class VolatilityRegime(Enum):
    """Режимы волатильности"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    EXTREME = "extreme"


class MarketAdapter:
    """
    Адаптирует торговые параметры под текущие рыночные условия
    """
    
    def __init__(self, data_manager, config: dict):
        self.data_manager = data_manager
        self.config = config
        
        # Пороги волатильности (ATR based)
        self.volatility_thresholds = {
            'low': 0.5,      # < 0.5% за 15 минут
            'normal': 1.0,   # 0.5-1.0%
            'high': 2.0,     # 1.0-2.0%
            'extreme': None  # > 2.0%
        }
        
        # Множители для разных режимов
        self.mode_adjustments = {
            'conservative': {
                'position_size_multiplier': 1.5,
                'tp_multiplier': 1.2,
                'min_signal_strength': 4,
                'use_momentum_burst': False
            },
            'balanced': {
                'position_size_multiplier': 1.0,
                'tp_multiplier': 1.0,
                'min_signal_strength': 3,
                'use_momentum_burst': True
            },
            'aggressive': {
                'position_size_multiplier': 0.5,
                'tp_multiplier': 0.8,
                'min_signal_strength': 2,
                'use_momentum_burst': True
            }
        }
        
        # Кэш анализа рынка
        self._market_analysis_cache = {}
        self._cache_ttl = 300  # 5 минут
        
    async def analyze_global_market(self) -> Dict:
        """
        Анализирует глобальное состояние рынка
        """
        try:
            # Проверяем кэш
            cache_key = 'global_market'
            if self._is_cache_valid(cache_key):
                return self._market_analysis_cache[cache_key]['data']
            
            # Анализируем BTC как индикатор рынка
            btc_analysis = await self._analyze_btc_market()
            
            # Анализируем топ-10 монет по капитализации
            top_coins_analysis = await self._analyze_top_coins()
            
            # Определяем общий тип рынка
            market_type = self._determine_market_type(btc_analysis, top_coins_analysis)
            
            # Определяем режим волатильности
            volatility_regime = await self._determine_volatility_regime()
            
            # Анализируем силу секторов
            sector_strength = await self._analyze_sector_strength()
            
            result = {
                'market_type': market_type,
                'volatility_regime': volatility_regime,
                'btc_trend': btc_analysis['trend'],
                'btc_strength': btc_analysis['strength'],
                'market_breadth': top_coins_analysis['breadth'],
                'strongest_sectors': sector_strength['top_3'],
                'fear_greed_estimate': self._estimate_fear_greed(btc_analysis, volatility_regime),
                'recommended_mode': self._recommend_trading_mode(market_type, volatility_regime),
                'timestamp': datetime.now()
            }
            
            # Сохраняем в кэш
            self._update_cache(cache_key, result)
            
            logger.info(f"Анализ рынка: {market_type.value}, волатильность: {volatility_regime.value}")
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка анализа глобального рынка: {e}")
            return self._get_default_market_analysis()
    
    async def adapt_trading_parameters(self, symbol: str, base_params: Dict) -> Dict:
        """
        Адаптирует торговые параметры под текущий рынок и конкретную монету
        """
        try:
            # Получаем глобальный анализ рынка
            market_analysis = await self.analyze_global_market()
            
            # Анализируем конкретную монету
            symbol_analysis = await self._analyze_symbol_conditions(symbol)
            
            # Базовые параметры из конфига
            mode = self.config['trading']['mode']
            mode_params = self.mode_adjustments[mode].copy()
            
            # Адаптируем под волатильность
            adapted_params = self._adapt_for_volatility(
                mode_params, 
                market_analysis['volatility_regime'],
                symbol_analysis['volatility']
            )
            
            # Адаптируем под тип рынка
            adapted_params = self._adapt_for_market_type(
                adapted_params,
                market_analysis['market_type'],
                symbol_analysis
            )
            
            # Специальные корректировки для momentum burst
            if base_params.get('signal_type') == 'momentum_burst':
                adapted_params = self._adapt_for_momentum_burst(adapted_params, symbol_analysis)
            
            # Финальные параметры
            final_params = {
                **base_params,
                **adapted_params,
                'market_context': {
                    'global': market_analysis,
                    'symbol': symbol_analysis
                }
            }
            
            logger.debug(f"Адаптированные параметры для {symbol}: {final_params}")
            
            return final_params
            
        except Exception as e:
            logger.error(f"Ошибка адаптации параметров для {symbol}: {e}")
            return base_params
    
    async def get_indicator_set(self, market_type: MarketType, volatility: VolatilityRegime) -> List[str]:
        """
        Возвращает оптимальный набор индикаторов для текущего рынка
        """
        indicator_sets = {
            # Трендовый рынок с нормальной волатильностью
            (MarketType.TRENDING_UP, VolatilityRegime.NORMAL): [
                'macd_trend', 'moving_averages', 'volume_trend', 
                'support_resistance', 'momentum_oscillator'
            ],
            
            # Трендовый рынок с высокой волатильностью
            (MarketType.TRENDING_UP, VolatilityRegime.HIGH): [
                'macd_momentum', 'volume_burst', 'atr_bands',
                'liquidity_pools', 'momentum_divergence'
            ],
            
            # Боковой рынок
            (MarketType.RANGING, VolatilityRegime.NORMAL): [
                'support_resistance_strong', 'rsi_extremes', 'volume_accumulation',
                'liquidity_pools', 'mean_reversion'
            ],
            
            # Волатильный рынок
            (MarketType.VOLATILE, VolatilityRegime.EXTREME): [
                'atr_expansion', 'volume_climax', 'momentum_burst',
                'volatility_breakout', 'panic_detection'
            ],
            
            # Накопление
            (MarketType.ACCUMULATION, VolatilityRegime.LOW): [
                'volume_accumulation', 'support_building', 'smart_money_flow',
                'accumulation_patterns', 'breakout_preparation'
            ]
        }
        
        # Возвращаем подходящий набор или дефолтный
        key = (market_type, volatility)
        return indicator_sets.get(key, self._get_default_indicator_set())
    
    # === Приватные методы анализа ===
    
    async def _analyze_btc_market(self) -> Dict:
        """Анализирует состояние BTC"""
        try:
            # Получаем данные BTC
            btc_data = await self.data_manager.get_symbol_data('BTCUSDT', ['4h', '1h', '15m'])
            
            if not btc_data:
                return {'trend': 'neutral', 'strength': 0}
            
            # Определяем тренд по MA
            ma_analysis = self._analyze_moving_averages(btc_data['4h'])
            
            # Определяем силу тренда
            trend_strength = self._calculate_trend_strength(btc_data['1h'])
            
            # Проверяем momentum
            momentum = self._analyze_momentum(btc_data['15m'])
            
            return {
                'trend': ma_analysis['direction'],
                'strength': trend_strength,
                'momentum': momentum,
                'key_levels': self._find_key_levels(btc_data['4h'])
            }
            
        except Exception as e:
            logger.error(f"Ошибка анализа BTC: {e}")
            return {'trend': 'neutral', 'strength': 0}
    
    async def _analyze_top_coins(self) -> Dict:
        """Анализирует топ монеты для определения ширины рынка"""
        top_symbols = ['ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT']
        
        bullish_count = 0
        bearish_count = 0
        total_strength = 0
        
        for symbol in top_symbols:
            try:
                data = await self.data_manager.get_symbol_data(symbol, ['1h'])
                if data and '1h' in data:
                    trend = self._quick_trend_check(data['1h'])
                    if trend > 0:
                        bullish_count += 1
                    elif trend < 0:
                        bearish_count += 1
                    total_strength += abs(trend)
            except:
                continue
        
        total_coins = len(top_symbols)
        breadth = (bullish_count - bearish_count) / total_coins if total_coins > 0 else 0
        
        return {
            'breadth': breadth,
            'bullish_percent': bullish_count / total_coins * 100,
            'average_strength': total_strength / total_coins if total_coins > 0 else 0
        }
    
    async def _determine_volatility_regime(self) -> VolatilityRegime:
        """Определяет текущий режим волатильности"""
        try:
            # Анализируем волатильность BTC как индикатор
            btc_data = await self.data_manager.get_symbol_data('BTCUSDT', ['15m'])
            
            if not btc_data or '15m' not in btc_data:
                return VolatilityRegime.NORMAL
            
            # Рассчитываем ATR%
            atr_percent = self._calculate_atr_percent(btc_data['15m'])
            
            # Определяем режим
            if atr_percent < self.volatility_thresholds['low']:
                return VolatilityRegime.LOW
            elif atr_percent < self.volatility_thresholds['normal']:
                return VolatilityRegime.NORMAL
            elif atr_percent < self.volatility_thresholds['high']:
                return VolatilityRegime.HIGH
            else:
                return VolatilityRegime.EXTREME
                
        except Exception as e:
            logger.error(f"Ошибка определения волатильности: {e}")
            return VolatilityRegime.NORMAL
    
    async def _analyze_sector_strength(self) -> Dict:
        """Анализирует силу различных секторов"""
        sectors = {
            'AI': ['FETUSDT', 'AGIXUSDT', 'OCEANUSDT'],
            'Gaming': ['AXSUSDT', 'SANDUSDT', 'MANAUSDT'],
            'DeFi': ['UNIUSDT', 'AAVEUSDT', 'SUSHIUSDT'],
            'Layer1': ['SOLUSDT', 'AVAXUSDT', 'NEARUSDT'],
            'Layer2': ['MATICUSDT', 'ARBUSDT', 'OPUSDT']
        }
        
        sector_scores = {}
        
        for sector, symbols in sectors.items():
            total_score = 0
            count = 0
            
            for symbol in symbols:
                try:
                    data = await self.data_manager.get_symbol_data(symbol, ['1h'])
                    if data and '1h' in data:
                        score = self._calculate_symbol_strength(data['1h'])
                        total_score += score
                        count += 1
                except:
                    continue
            
            if count > 0:
                sector_scores[sector] = total_score / count
        
        # Сортируем по силе
        sorted_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)
        
        return {
            'scores': sector_scores,
            'top_3': [s[0] for s in sorted_sectors[:3]],
            'bottom_3': [s[0] for s in sorted_sectors[-3:]]
        }
    
    async def _analyze_symbol_conditions(self, symbol: str) -> Dict:
        """Анализирует условия конкретной монеты"""
        try:
            data = await self.data_manager.get_symbol_data(symbol, ['1h', '15m'])
            
            if not data:
                return self._get_default_symbol_analysis()
            
            # Волатильность символа
            volatility = self._calculate_symbol_volatility(data)
            
            # Ликвидность
            liquidity = await self._check_liquidity(symbol)
            
            # Корреляция с BTC
            btc_correlation = await self._calculate_btc_correlation(symbol)
            
            # Техническое состояние
            technical_state = self._analyze_technical_state(data)
            
            return {
                'volatility': volatility,
                'liquidity': liquidity,
                'btc_correlation': btc_correlation,
                'technical_state': technical_state,
                'relative_strength': await self._calculate_relative_strength(symbol)
            }
            
        except Exception as e:
            logger.error(f"Ошибка анализа {symbol}: {e}")
            return self._get_default_symbol_analysis()
    
    # === Методы адаптации параметров ===
    
    def _adapt_for_volatility(self, params: Dict, global_volatility: VolatilityRegime, 
                             symbol_volatility: float) -> Dict:
        """Адаптирует параметры под волатильность"""
        adapted = params.copy()
        
        # При высокой волатильности
        if global_volatility in [VolatilityRegime.HIGH, VolatilityRegime.EXTREME]:
            # Уменьшаем размер позиции
            adapted['position_size_multiplier'] *= 0.7
            # Увеличиваем TP для компенсации
            adapted['tp_multiplier'] *= 1.3
            # Повышаем требования к сигналам
            adapted['min_signal_strength'] += 1
            
        # При низкой волатильности
        elif global_volatility == VolatilityRegime.LOW:
            # Можем увеличить позицию
            adapted['position_size_multiplier'] *= 1.2
            # Уменьшаем TP
            adapted['tp_multiplier'] *= 0.8
            # Можем быть менее строгими
            adapted['min_signal_strength'] = max(2, adapted['min_signal_strength'] - 1)
        
        # Корректировка под волатильность конкретного символа
        if symbol_volatility > 3.0:  # Очень волатильный символ
            adapted['position_size_multiplier'] *= 0.8
            adapted['use_tight_stop'] = True
        
        return adapted
    
    def _adapt_for_market_type(self, params: Dict, market_type: MarketType, 
                               symbol_analysis: Dict) -> Dict:
        """Адаптирует параметры под тип рынка"""
        adapted = params.copy()
        
        if market_type == MarketType.TRENDING_UP:
            # В восходящем тренде
            adapted['prefer_long'] = True
            adapted['momentum_weight'] = 1.5
            
        elif market_type == MarketType.TRENDING_DOWN:
            # В нисходящем тренде
            adapted['prefer_short'] = True
            adapted['bounce_trading'] = True
            
        elif market_type == MarketType.RANGING:
            # В боковике
            adapted['range_trading'] = True
            adapted['support_resistance_weight'] = 2.0
            adapted['momentum_weight'] = 0.5
            
        elif market_type == MarketType.VOLATILE:
            # При высокой волатильности
            adapted['breakout_trading'] = True
            adapted['volume_confirmation_required'] = True
            
        return adapted
    
    def _adapt_for_momentum_burst(self, params: Dict, symbol_analysis: Dict) -> Dict:
        """Специальная адаптация для momentum burst сигналов"""
        adapted = params.copy()
        
        # Для momentum burst всегда уменьшаем позицию
        if self.config['trading']['mode'] == 'aggressive':
            adapted['position_size_multiplier'] *= 0.5
        else:
            adapted['position_size_multiplier'] *= 0.7
        
        # Быстрый TP
        adapted['tp_multiplier'] *= 0.8
        
        # Не используем DCA на momentum burst
        adapted['dca_enabled'] = False
        
        return adapted
    
    # === Вспомогательные методы ===
    
    def _determine_market_type(self, btc_analysis: Dict, top_coins: Dict) -> MarketType:
        """Определяет тип рынка"""
        btc_trend = btc_analysis['trend']
        market_breadth = top_coins['breadth']
        
        # Сильный восходящий тренд
        if btc_trend == 'up' and market_breadth > 0.5:
            return MarketType.TRENDING_UP
        
        # Сильный нисходящий тренд
        elif btc_trend == 'down' and market_breadth < -0.5:
            return MarketType.TRENDING_DOWN
        
        # Расхождение - возможно распределение
        elif btc_trend == 'up' and market_breadth < -0.3:
            return MarketType.DISTRIBUTION
        
        # Накопление
        elif btc_trend == 'neutral' and abs(market_breadth) < 0.2:
            if btc_analysis.get('momentum', 0) > 0:
                return MarketType.ACCUMULATION
        
        # Высокая волатильность
        elif abs(market_breadth) > 0.7:
            return MarketType.VOLATILE
        
        # По умолчанию - боковик
        return MarketType.RANGING
    
    def _estimate_fear_greed(self, btc_analysis: Dict, volatility: VolatilityRegime) -> int:
        """Оценивает индекс страха и жадности"""
        score = 50  # Нейтральное значение
        
        # Корректировка по тренду BTC
        if btc_analysis['trend'] == 'up':
            score += btc_analysis['strength'] * 20
        elif btc_analysis['trend'] == 'down':
            score -= btc_analysis['strength'] * 20
        
        # Корректировка по волатильности
        if volatility == VolatilityRegime.LOW:
            score += 10  # Низкая волатильность = жадность
        elif volatility == VolatilityRegime.EXTREME:
            score -= 20  # Высокая волатильность = страх
        
        return max(0, min(100, int(score)))
    
    def _recommend_trading_mode(self, market_type: MarketType, 
                               volatility: VolatilityRegime) -> str:
        """Рекомендует оптимальный режим торговли"""
        # При экстремальной волатильности - только conservative
        if volatility == VolatilityRegime.EXTREME:
            return 'conservative'
        
        # В трендовом рынке с нормальной волатильностью - balanced
        if market_type in [MarketType.TRENDING_UP, MarketType.TRENDING_DOWN]:
            if volatility == VolatilityRegime.NORMAL:
                return 'balanced'
        
        # В боковике - aggressive для ловли отскоков
        if market_type == MarketType.RANGING and volatility == VolatilityRegime.LOW:
            return 'aggressive'
        
        # По умолчанию - balanced
        return 'balanced'
    
    def _analyze_moving_averages(self, candles: List[Dict]) -> Dict:
        """Анализирует скользящие средние"""
        if len(candles) < 50:
            return {'direction': 'neutral', 'strength': 0}
        
        closes = [float(c['close']) for c in candles]
        
        # Считаем MA
        ma20 = np.mean(closes[-20:])
        ma50 = np.mean(closes[-50:])
        current_price = closes[-1]
        
        # Определяем направление
        if current_price > ma20 > ma50:
            direction = 'up'
            strength = (current_price - ma50) / ma50
        elif current_price < ma20 < ma50:
            direction = 'down'
            strength = (ma50 - current_price) / ma50
        else:
            direction = 'neutral'
            strength = 0
        
        return {
            'direction': direction,
            'strength': min(1.0, strength * 100)  # Нормализуем до 0-1
        }
    
    def _calculate_trend_strength(self, candles: List[Dict]) -> float:
        """Рассчитывает силу тренда"""
        if len(candles) < 20:
            return 0
        
        closes = [float(c['close']) for c in candles[-20:]]
        
        # Считаем наклон линейной регрессии
        x = np.arange(len(closes))
        slope = np.polyfit(x, closes, 1)[0]
        
        # Нормализуем относительно средней цены
        avg_price = np.mean(closes)
        normalized_slope = (slope / avg_price) * 100
        
        return min(1.0, abs(normalized_slope))
    
    def _analyze_momentum(self, candles: List[Dict]) -> float:
        """Анализирует momentum"""
        if len(candles) < 10:
            return 0
        
        closes = [float(c['close']) for c in candles[-10:]]
        
        # Rate of Change
        roc = (closes[-1] - closes[0]) / closes[0]
        
        return roc
    
    def _find_key_levels(self, candles: List[Dict]) -> Dict:
        """Находит ключевые уровни"""
        if len(candles) < 50:
            return {'support': [], 'resistance': []}
        
        highs = [float(c['high']) for c in candles]
        lows = [float(c['low']) for c in candles]
        
        # Простой метод: локальные экстремумы
        resistance_levels = []
        support_levels = []
        
        for i in range(10, len(highs) - 10):
            # Сопротивление
            if highs[i] == max(highs[i-10:i+11]):
                resistance_levels.append(highs[i])
            
            # Поддержка
            if lows[i] == min(lows[i-10:i+11]):
                support_levels.append(lows[i])
        
        return {
            'support': sorted(set(support_levels))[-3:],  # Топ 3
            'resistance': sorted(set(resistance_levels))[:3]  # Топ 3
        }
    
    def _quick_trend_check(self, candles: List[Dict]) -> float:
        """Быстрая проверка тренда"""
        if len(candles) < 20:
            return 0
        
        closes = [float(c['close']) for c in candles[-20:]]
        ma_fast = np.mean(closes[-5:])
        ma_slow = np.mean(closes)
        
        return (ma_fast - ma_slow) / ma_slow
    
    def _calculate_atr_percent(self, candles: List[Dict]) -> float:
        """Рассчитывает ATR в процентах"""
        if len(candles) < 14:
            return 1.0
        
        atr_values = []
        
        for i in range(1, min(15, len(candles))):
            high = float(candles[-i]['high'])
            low = float(candles[-i]['low'])
            prev_close = float(candles[-i-1]['close'])
            
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            atr_values.append(tr)
        
        if not atr_values:
            return 1.0
        
        atr = np.mean(atr_values)
        current_price = float(candles[-1]['close'])
        
        return (atr / current_price) * 100
    
    def _calculate_symbol_strength(self, candles: List[Dict]) -> float:
        """Рассчитывает силу символа"""
        if len(candles) < 20:
            return 0
        
        # Считаем изменение за последние 20 свечей
        start_price = float(candles[-20]['close'])
        end_price = float(candles[-1]['close'])
        
        return ((end_price - start_price) / start_price) * 100
    
    def _calculate_symbol_volatility(self, data: Dict) -> float:
        """Рассчитывает волатильность символа"""
        if '15m' not in data:
            return 1.0
        
        return self._calculate_atr_percent(data['15m'])
    
    async def _check_liquidity(self, symbol: str) -> str:
        """Проверяет ликвидность символа"""
        # Здесь должна быть проверка объемов торгов
        # Пока возвращаем дефолт
        return 'normal'
    
    async def _calculate_btc_correlation(self, symbol: str) -> float:
        """Рассчитывает корреляцию с BTC"""
        # Упрощенная версия
        if symbol == 'BTCUSDT':
            return 1.0
        elif symbol in ['ETHUSDT', 'BNBUSDT']:
            return 0.8
        else:
            return 0.5
    
    def _analyze_technical_state(self, data: Dict) -> str:
        """Анализирует техническое состояние"""
        if '1h' not in data:
            return 'neutral'
        
        trend = self._quick_trend_check(data['1h'])
        
        if trend > 0.01:
            return 'bullish'
        elif trend < -0.01:
            return 'bearish'
        else:
            return 'neutral'
    
    async def _calculate_relative_strength(self, symbol: str) -> float:
        """Рассчитывает относительную силу символа"""
        # Упрощенная версия
        return 0.5
    
    # === Вспомогательные методы для кэша и дефолтов ===
    
    def _is_cache_valid(self, key: str) -> bool:
        """Проверяет валидность кэша"""
        if key not in self._market_analysis_cache:
            return False
        
        cache_time = self._market_analysis_cache[key]['timestamp']
        return (datetime.now() - cache_time).seconds < self._cache_ttl
    
    def _update_cache(self, key: str, data: Dict):
        """Обновляет кэш"""
        self._market_analysis_cache[key] = {
            'data': data,
            'timestamp': datetime.now()
        }
    
    def _get_default_market_analysis(self) -> Dict:
        """Возвращает дефолтный анализ рынка"""
        return {
            'market_type': MarketType.RANGING,
            'volatility_regime': VolatilityRegime.NORMAL,
            'btc_trend': 'neutral',
            'btc_strength': 0,
            'market_breadth': 0,
            'strongest_sectors': [],
            'fear_greed_estimate': 50,
            'recommended_mode': 'balanced',
            'timestamp': datetime.now()
        }
    
    def _get_default_symbol_analysis(self) -> Dict:
        """Возвращает дефолтный анализ символа"""
        return {
            'volatility': 1.0,
            'liquidity': 'normal',
            'btc_correlation': 0.5,
            'technical_state': 'neutral',
            'relative_strength': 0.5
        }
    
    def _get_default_indicator_set(self) -> List[str]:
        """Возвращает дефолтный набор индикаторов"""
        return [
            'macd_standard', 'rsi_standard', 'moving_averages',
            'support_resistance', 'volume_analysis'
        ]