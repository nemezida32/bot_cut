"""
Фаза 2: Средний анализ топ-50 монет
Цель: Детальный анализ и отбор топ-15 для мониторинга точек входа
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
from collections import deque

from ..core.data_manager import DataManager
from ..indicators.classic.macd import MACDAnalyzer
from ..indicators.classic.rsi import RSIAnalyzer
from ..indicators.market_structure.support_resistance import SupportResistanceAnalyzer


class Phase2MediumAnalyzer:
    """
    Средний анализатор для детальной проверки топ-50 монет
    Выполняется каждые 2 минуты на суженном списке
    """
    
    def __init__(self, config: dict, data_manager: DataManager):
        self.config = config
        self.data_manager = data_manager
        self.logger = logging.getLogger(__name__)
        
        # Инициализация индикаторов
        self.macd_analyzer = MACDAnalyzer(config)
        self.rsi_analyzer = RSIAnalyzer(config)
        self.sr_analyzer = SupportResistanceAnalyzer(config)
        
        # Настройки анализа
        self.entry_conditions = config['entry_conditions']['classic']
        
        # История анализов для трекинга
        self.analysis_history = {}
        
    async def analyze_symbol(self, symbol: str, market_type: str) -> Optional[Dict[str, Any]]:
        """
        Детальный анализ одного символа
        
        Args:
            symbol: Символ для анализа
            market_type: Тип рынка из фазы 1
            
        Returns:
            Dict с результатами или None
        """
        try:
            # Получаем необходимые данные параллельно
            tasks = [
                self.data_manager.get_klines(symbol, '15m', limit=100),
                self.data_manager.get_klines(symbol, '1h', limit=100),
                self.data_manager.get_klines(symbol, '4h', limit=100),
                self.data_manager.get_ticker_24h(symbol),
                self.data_manager.get_orderbook(symbol, limit=20),
                self._get_recent_trades_analysis(symbol)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Проверяем результаты
            klines_15m, klines_1h, klines_4h, ticker, orderbook, trades_analysis = results
            
            if any(isinstance(r, Exception) for r in results[:5]):
                self.logger.debug(f"Failed to get data for {symbol}")
                return None
                
            # Базовые проверки
            if not self._passes_basic_checks(ticker, orderbook):
                return None
                
            # Анализ MACD на разных таймфреймах
            macd_results = {
                '15m': await self.macd_analyzer.analyze(klines_15m, '15m'),
                '1h': await self.macd_analyzer.analyze(klines_1h, '1h'),
                '4h': await self.macd_analyzer.analyze(klines_4h, '4h')
            }
            
            # Проверка обязательных условий
            if not self._check_required_conditions(macd_results):
                return None
                
            # RSI анализ
            rsi_results = {
                '15m': await self.rsi_analyzer.analyze(klines_15m),
                '1h': await self.rsi_analyzer.analyze(klines_1h),
                '4h': await self.rsi_analyzer.analyze(klines_4h)
            }
            
            # Анализ уровней
            sr_levels = await self.sr_analyzer.analyze(klines_4h)
            current_price = float(ticker['lastPrice'])
            
            # Расчет расстояния до уровней
            distance_analysis = self._analyze_distance_to_levels(
                current_price, sr_levels, float(ticker['highPrice']), float(ticker['lowPrice'])
            )
            
            # Анализ накопления объема
            volume_accumulation = await self._analyze_volume_accumulation(
                symbol, klines_1h, trades_analysis
            )
            
            # Анализ симметрии движений
            symmetry_analysis = self._analyze_movement_symmetry(klines_4h)
            
            # Оценка потенциала движения
            movement_potential = self._calculate_movement_potential(
                macd_results, rsi_results, distance_analysis, 
                volume_accumulation, symmetry_analysis, market_type
            )
            
            # Проверка дополнительных условий
            optional_signals = self._check_optional_signals(
                rsi_results, volume_accumulation, sr_levels, 
                current_price, macd_results
            )
            
            # Финальная оценка
            potential_score = self._calculate_potential_score(
                movement_potential, optional_signals, distance_analysis
            )
            
            if potential_score < 50:  # Минимальный порог
                return None
                
            return {
                'symbol': symbol,
                'potential_score': potential_score,
                'movement_potential': movement_potential,
                'macd_status': macd_results,
                'rsi_status': rsi_results,
                'distance_to_levels': distance_analysis,
                'volume_accumulation': volume_accumulation,
                'symmetry': symmetry_analysis,
                'optional_signals': optional_signals,
                'estimated_duration': self._estimate_movement_duration(
                    movement_potential, symmetry_analysis
                ),
                'risk_factors': self._identify_risk_factors(
                    distance_analysis, rsi_results, volume_accumulation
                ),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Symbol analysis error for {symbol}: {e}")
            return None
            
    def _passes_basic_checks(self, ticker: dict, orderbook: dict) -> bool:
        """Базовые проверки ликвидности и активности"""
        try:
            # Проверка объема
            volume_24h = float(ticker['quoteVolume'])
            if volume_24h < self.config['filters']['volume']['min_24h_volume_usdt']:
                return False
                
            # Проверка спреда
            if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                best_bid = float(orderbook['bids'][0][0])
                best_ask = float(orderbook['asks'][0][0])
                spread_percent = (best_ask - best_bid) / best_bid * 100
                
                if spread_percent > self.config['filters']['liquidity']['max_spread_percent']:
                    return False
                    
            return True
            
        except Exception:
            return False
            
    def _check_required_conditions(self, macd_results: dict) -> bool:
        """Проверка обязательных условий MACD"""
        # MACD 15m должен быть в режиме 2 (формирование или активный)
        macd_15m = macd_results.get('15m', {})
        if not macd_15m.get('mode2_active'):
            return False
            
        # MACD 4h должен подтверждать направление
        macd_4h = macd_results.get('4h', {})
        if not macd_4h.get('bullish_confirmation'):
            return False
            
        return True
        
    def _analyze_distance_to_levels(self, current_price: float, sr_levels: dict,
                                   high_24h: float, low_24h: float) -> dict:
        """Анализ расстояния до ключевых уровней"""
        result = {
            'to_resistance': float('inf'),
            'to_support': float('inf'),
            'to_24h_high': abs((high_24h - current_price) / current_price * 100),
            'to_24h_low': abs((current_price - low_24h) / current_price * 100),
            'safe_distance': False
        }
        
        # Ближайшее сопротивление
        resistances = sr_levels.get('resistance_levels', [])
        for level in resistances:
            if level > current_price:
                distance = (level - current_price) / current_price * 100
                if distance < result['to_resistance']:
                    result['to_resistance'] = distance
                    
        # Ближайшая поддержка
        supports = sr_levels.get('support_levels', [])
        for level in supports:
            if level < current_price:
                distance = (current_price - level) / current_price * 100
                if distance < result['to_support']:
                    result['to_support'] = distance
                    
        # Проверка безопасного расстояния
        min_distance = self.entry_conditions['distance_to_extremes']
        result['safe_distance'] = (
            result['to_24h_high'] >= min_distance and
            result['to_resistance'] >= min_distance
        )
        
        return result
        
    async def _analyze_volume_accumulation(self, symbol: str, klines_1h: List,
                                         trades_analysis: dict) -> dict:
        """Анализ накопления объема"""
        try:
            if len(klines_1h) < 24:
                return {'accumulating': False, 'score': 0}
                
            # Объемы за последние 24 часа
            volumes = [float(k[5]) for k in klines_1h[-24:]]
            prices = [float(k[4]) for k in klines_1h[-24:]]
            
            # Средний объем
            avg_volume = np.mean(volumes)
            recent_avg = np.mean(volumes[-6:])
            
            # Анализ накопления
            accumulation_score = 0
            
            # 1. Увеличение объема при боковике
            price_range = (max(prices) - min(prices)) / np.mean(prices) * 100
            if price_range < 3 and recent_avg > avg_volume * 1.2:
                accumulation_score += 30
                
            # 2. Объем на понижениях больше чем на повышениях
            down_volume = 0
            up_volume = 0
            
            for i in range(1, len(klines_1h[-24:])):
                if prices[i] < prices[i-1]:
                    down_volume += volumes[i]
                else:
                    up_volume += volumes[i]
                    
            if down_volume > up_volume * 1.2:
                accumulation_score += 20
                
            # 3. Крупные покупки из trades_analysis
            if trades_analysis.get('large_buys_ratio', 0) > 0.6:
                accumulation_score += 20
                
            # 4. Постепенное увеличение объема
            volume_trend = self._calculate_volume_trend(volumes)
            if volume_trend > 0:
                accumulation_score += 10
                
            return {
                'accumulating': accumulation_score >= 40,
                'score': accumulation_score,
                'volume_trend': volume_trend,
                'recent_vs_average': recent_avg / avg_volume if avg_volume > 0 else 1,
                'price_range': price_range,
                'buy_pressure': trades_analysis.get('buy_pressure', 0.5)
            }
            
        except Exception as e:
            self.logger.debug(f"Volume accumulation error: {e}")
            return {'accumulating': False, 'score': 0}
            
    def _analyze_movement_symmetry(self, klines_4h: List) -> dict:
        """Анализ симметрии предыдущих движений"""
        if len(klines_4h) < 50:
            return {'symmetry_found': False, 'expected_move': 0}
            
        try:
            closes = [float(k[4]) for k in klines_4h]
            
            # Находим значимые движения (> 2%)
            movements = []
            i = 0
            while i < len(closes) - 1:
                # Ищем начало движения
                start_idx = i
                start_price = closes[i]
                
                # Определяем направление
                direction = 'up' if closes[i+1] > closes[i] else 'down'
                
                # Ищем конец движения
                end_idx = i + 1
                while end_idx < len(closes) - 1:
                    if direction == 'up' and closes[end_idx+1] < closes[end_idx]:
                        break
                    elif direction == 'down' and closes[end_idx+1] > closes[end_idx]:
                        break
                    end_idx += 1
                    
                # Расчет амплитуды
                end_price = closes[end_idx]
                amplitude = abs((end_price - start_price) / start_price * 100)
                
                if amplitude > 2:  # Значимое движение
                    movements.append({
                        'direction': direction,
                        'amplitude': amplitude,
                        'duration': end_idx - start_idx,
                        'start_idx': start_idx,
                        'end_idx': end_idx
                    })
                    
                i = end_idx
                
            # Анализ симметрии
            if len(movements) < 4:
                return {'symmetry_found': False, 'expected_move': 0}
                
            # Ищем паттерны up-down с похожей амплитудой
            symmetry_pairs = []
            for i in range(len(movements) - 1):
                if movements[i]['direction'] != movements[i+1]['direction']:
                    ratio = movements[i]['amplitude'] / movements[i+1]['amplitude']
                    if 0.7 <= ratio <= 1.3:  # Похожие амплитуды
                        symmetry_pairs.append({
                            'move1': movements[i],
                            'move2': movements[i+1],
                            'ratio': ratio
                        })
                        
            if not symmetry_pairs:
                return {'symmetry_found': False, 'expected_move': 0}
                
            # Средняя амплитуда симметричных движений
            avg_amplitude = np.mean([
                (p['move1']['amplitude'] + p['move2']['amplitude']) / 2 
                for p in symmetry_pairs
            ])
            
            # Проверяем текущую ситуацию
            recent_movement = self._get_recent_movement(closes[-10:])
            
            return {
                'symmetry_found': True,
                'expected_move': avg_amplitude,
                'symmetry_count': len(symmetry_pairs),
                'avg_ratio': np.mean([p['ratio'] for p in symmetry_pairs]),
                'recent_movement': recent_movement,
                'ready_for_reversal': recent_movement['amplitude'] > avg_amplitude * 0.8
            }
            
        except Exception as e:
            self.logger.debug(f"Symmetry analysis error: {e}")
            return {'symmetry_found': False, 'expected_move': 0}
            
    def _calculate_movement_potential(self, macd_results: dict, rsi_results: dict,
                                    distance_analysis: dict, volume_accumulation: dict,
                                    symmetry_analysis: dict, market_type: str) -> dict:
        """Расчет потенциала движения"""
        base_potential = 1.5  # Базовый таргет 1.5%
        
        # Модификаторы потенциала
        multipliers = []
        
        # MACD сила сигнала
        macd_15m = macd_results.get('15m', {})
        if macd_15m.get('histogram_growing') and macd_15m.get('acceleration', 0) > 0:
            multipliers.append(1.2)
            
        # RSI условия
        rsi_1h = rsi_results.get('1h', {})
        if rsi_1h.get('value', 50) < 40:  # Oversold
            multipliers.append(1.3)
        elif 40 <= rsi_1h.get('value', 50) <= 60:  # Neutral zone
            multipliers.append(1.1)
            
        # Расстояние до сопротивления
        if distance_analysis['to_resistance'] > 5:
            multipliers.append(1.4)
        elif distance_analysis['to_resistance'] > 3:
            multipliers.append(1.2)
            
        # Накопление объема
        if volume_accumulation['accumulating']:
            multipliers.append(1.3)
            
        # Симметрия
        if symmetry_analysis['symmetry_found']:
            # Используем ожидаемое движение из симметрии
            if symmetry_analysis['expected_move'] > base_potential:
                base_potential = min(symmetry_analysis['expected_move'], 3.0)
                
        # Тип рынка
        market_multipliers = {
            'TRENDING_UP': 1.2,
            'VOLATILE': 1.1,
            'ACCUMULATION': 1.3,
            'DISTRIBUTION': 0.8,
            'RANGING': 0.9
        }
        
        if market_type in market_multipliers:
            multipliers.append(market_multipliers[market_type])
            
        # Итоговый потенциал
        total_multiplier = np.prod(multipliers) if multipliers else 1.0
        potential = base_potential * total_multiplier
        
        # Ограничения
        max_potential = 3.0  # Максимум 3%
        min_potential = 1.0  # Минимум 1%
        
        return {
            'expected_move_percent': max(min(potential, max_potential), min_potential),
            'base_potential': base_potential,
            'multiplier': total_multiplier,
            'confidence': min(len(multipliers) / 5, 1.0),  # До 5 факторов
            'factors': {
                'macd_strength': 'strong' if any('macd' in str(m) for m in multipliers) else 'normal',
                'rsi_condition': rsi_1h.get('condition', 'neutral'),
                'distance_safe': distance_analysis['safe_distance'],
                'volume_accumulating': volume_accumulation['accumulating'],
                'symmetry_found': symmetry_analysis['symmetry_found']
            }
        }
        
    def _check_optional_signals(self, rsi_results: dict, volume_accumulation: dict,
                               sr_levels: dict, current_price: float, 
                               macd_results: dict) -> dict:
        """Проверка дополнительных сигналов"""
        signals = {
            'rsi_oversold': False,
            'volume_increase': False,
            'support_nearby': False,
            'trend_alignment': False,
            'count': 0
        }
        
        # RSI oversold на любом таймфрейме
        for tf, rsi_data in rsi_results.items():
            if rsi_data.get('value', 50) < 35:
                signals['rsi_oversold'] = True
                break
                
        # Увеличение объема
        if volume_accumulation.get('recent_vs_average', 1) > 1.2:
            signals['volume_increase'] = True
            
        # Близость к поддержке
        nearest_support = float('inf')
        for level in sr_levels.get('support_levels', []):
            if level < current_price:
                distance = (current_price - level) / current_price * 100
                if distance < nearest_support:
                    nearest_support = distance
                    
        if nearest_support < 2:
            signals['support_nearby'] = True
            
        # Выравнивание трендов
        trend_aligned = True
        for tf in ['15m', '1h', '4h']:
            if not macd_results.get(tf, {}).get('bullish_trend'):
                trend_aligned = False
                break
                
        signals['trend_alignment'] = trend_aligned
        
        # Подсчет активных сигналов
        signals['count'] = sum([
            signals['rsi_oversold'],
            signals['volume_increase'],
            signals['support_nearby'],
            signals['trend_alignment']
        ])
        
        return signals
        
    def _calculate_potential_score(self, movement_potential: dict, 
                                 optional_signals: dict, 
                                 distance_analysis: dict) -> float:
        """Расчет итогового score потенциала"""
        score = 0.0
        
        # Базовый score от ожидаемого движения
        expected_move = movement_potential['expected_move_percent']
        if expected_move >= 2:
            score += 40
        elif expected_move >= 1.5:
            score += 30
        elif expected_move >= 1:
            score += 20
            
        # Уверенность в движении
        score += movement_potential['confidence'] * 20
        
        # Дополнительные сигналы (минимум 2 нужно)
        if optional_signals['count'] >= 2:
            score += optional_signals['count'] * 10
            
        # Безопасное расстояние
        if distance_analysis['safe_distance']:
            score += 10
            
        # Штрафы
        if distance_analysis['to_resistance'] < 1.5:
            score -= 20  # Слишком близко к сопротивлению
            
        if not movement_potential['factors']['volume_accumulating']:
            score -= 10  # Нет накопления объема
            
        return min(max(score, 0), 100)
        
    def _estimate_movement_duration(self, movement_potential: dict, 
                                  symmetry_analysis: dict) -> int:
        """Оценка длительности движения в минутах"""
        base_duration = 60  # Базовая длительность 1 час
        
        # Корректировка на основе потенциала
        if movement_potential['expected_move_percent'] > 2:
            base_duration = 120  # 2 часа для больших движений
        elif movement_potential['expected_move_percent'] < 1.5:
            base_duration = 45  # 45 минут для малых
            
        # Корректировка на основе симметрии
        if symmetry_analysis.get('symmetry_found'):
            # Используем среднюю длительность из истории
            # Упрощенно - добавляем 30 минут
            base_duration += 30
            
        return base_duration
        
    def _identify_risk_factors(self, distance_analysis: dict, rsi_results: dict,
                             volume_accumulation: dict) -> List[str]:
        """Идентификация факторов риска"""
        risks = []
        
        # Близость к сопротивлению
        if distance_analysis['to_resistance'] < 2:
            risks.append('close_to_resistance')
            
        # Перекупленность
        if any(rsi.get('value', 50) > 70 for rsi in rsi_results.values()):
            risks.append('overbought')
            
        # Низкая поддержка снизу
        if distance_analysis['to_support'] > 5:
            risks.append('weak_support_below')
            
        # Отсутствие накопления
        if not volume_accumulation['accumulating']:
            risks.append('no_volume_accumulation')
            
        # Слишком узкий диапазон
        if volume_accumulation.get('price_range', 0) < 1:
            risks.append('tight_range')
            
        return risks
        
    async def _get_recent_trades_analysis(self, symbol: str) -> dict:
        """Анализ последних сделок"""
        try:
            # Здесь должен быть запрос к API для получения последних сделок
            # Пока возвращаем заглушку
            return {
                'large_buys_ratio': 0.5,
                'buy_pressure': 0.5,
                'avg_trade_size': 1000
            }
        except Exception as e:
            self.logger.debug(f"Recent trades analysis error: {e}")
            return {
                'large_buys_ratio': 0.5,
                'buy_pressure': 0.5,
                'avg_trade_size': 0
            }
            
    def _calculate_volume_trend(self, volumes: List[float]) -> float:
        """Расчет тренда объема"""
        if len(volumes) < 3:
            return 0
            
        # Линейная регрессия для определения тренда
        x = np.arange(len(volumes))
        y = np.array(volumes)
        
        # Нормализация
        if np.mean(y) > 0:
            y = y / np.mean(y)
        else:
            return 0
            
        # Коэффициент наклона
        slope = np.polyfit(x, y, 1)[0]
        
        return slope
        
    def _get_recent_movement(self, closes: List[float]) -> dict:
        """Получение информации о последнем движении"""
        if len(closes) < 2:
            return {'amplitude': 0, 'direction': 'flat'}
            
        start_price = closes[0]
        end_price = closes[-1]
        amplitude = abs((end_price - start_price) / start_price * 100)
        direction = 'up' if end_price > start_price else 'down'
        
        return {
            'amplitude': amplitude,
            'direction': direction,
            'start_price': start_price,
            'end_price': end_price
        }
        
    def update_config(self, new_config: dict):
        """Обновление конфигурации"""
        if 'entry_conditions' in new_config:
            self.entry_conditions.update(new_config['entry_conditions']['classic'])
            self.logger.info("Entry conditions updated in Phase2 analyzer")