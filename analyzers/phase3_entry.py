"""
Фаза 3: Точечный вход - real-time мониторинг точек входа
Цель: Отслеживание формирования сигналов в реальном времени
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict

from ..core.data_manager import DataManager
from ..core.websocket_manager import WebSocketManager
from ..indicators.classic.macd import MACDAnalyzer
from ..indicators.momentum.burst_detector import MomentumBurstDetector
from ..indicators.volume.cvd import CVDAnalyzer


class Phase3EntryAnalyzer:
    """
    Real-time анализатор для поиска точек входа
    Работает через WebSocket для минимальной задержки
    """
    
    def __init__(self, config: dict, data_manager: DataManager):
        self.config = config
        self.data_manager = data_manager
        self.logger = logging.getLogger(__name__)
        
        # WebSocket менеджер
        self.ws_manager = WebSocketManager(config)
        
        # Инициализация индикаторов
        self.macd_analyzer = MACDAnalyzer(config)
        self.burst_detector = MomentumBurstDetector(config)
        self.cvd_analyzer = CVDAnalyzer(config)
        
        # Настройки входа
        self.entry_conditions = config['entry_conditions']
        
        # Активные мониторы
        self.active_monitors = {}
        self.monitoring_symbols = set()
        
        # Кеш последних данных
        self.last_signals = defaultdict(dict)
        self.formation_tracking = defaultdict(dict)
        
    async def monitor_symbols(self, symbols: List[str], callback: Callable):
        """
        Начать мониторинг списка символов
        
        Args:
            symbols: Список символов для мониторинга
            callback: Функция для вызова при обнаружении сигнала
        """
        self.logger.info(f"Starting Phase 3 monitoring for {len(symbols)} symbols")
        
        # Запускаем WebSocket менеджер если не запущен
        if not self.ws_manager.is_running:
            await self.ws_manager.start()
            
        # Обновляем список мониторинга
        self.monitoring_symbols = set(symbols)
        
        # Подписываемся на необходимые потоки
        await self._setup_subscriptions(symbols)
        
        # Сохраняем callback
        self.signal_callback = callback
        
        # Запускаем мониторы для каждого символа
        for symbol in symbols:
            if symbol not in self.active_monitors:
                monitor_task = asyncio.create_task(
                    self._symbol_monitor(symbol)
                )
                self.active_monitors[symbol] = monitor_task
                
    async def stop_monitoring(self, symbols: List[str] = None):
        """Остановить мониторинг символов"""
        if symbols is None:
            symbols = list(self.monitoring_symbols)
            
        for symbol in symbols:
            if symbol in self.active_monitors:
                self.active_monitors[symbol].cancel()
                del self.active_monitors[symbol]
                
            self.monitoring_symbols.discard(symbol)
            
        self.logger.info(f"Stopped monitoring {len(symbols)} symbols")
        
    async def _setup_subscriptions(self, symbols: List[str]):
        """Настройка подписок WebSocket"""
        # Подписка на тикеры для отслеживания цен
        await self.ws_manager.subscribe_ticker(
            symbols, 
            self._handle_ticker_update
        )
        
        # Подписка на 1m свечи для MACD
        for symbol in symbols:
            await self.ws_manager.subscribe_klines(
                symbol, '1m', 
                self._handle_kline_update
            )
            
        # Подписка на сделки для анализа объема
        await self.ws_manager.subscribe_trades(
            symbols,
            self._handle_trade_update
        )
        
        # Подписка на стакан для анализа ликвидности
        await self.ws_manager.subscribe_depth(
            symbols,
            self._handle_depth_update
        )
        
        # Детектор momentum burst
        for symbol in symbols:
            await self.ws_manager.detect_momentum_spike(
                symbol,
                self._handle_momentum_spike
            )
            
    async def _symbol_monitor(self, symbol: str):
        """Основной цикл мониторинга символа"""
        self.logger.debug(f"Starting monitor for {symbol}")
        
        while symbol in self.monitoring_symbols:
            try:
                # Проверяем формирование сигналов каждые 5 секунд
                await asyncio.sleep(5)
                
                # Анализ классического входа
                classic_signal = await self._check_classic_entry(symbol)
                if classic_signal:
                    await self._emit_signal(symbol, classic_signal)
                    
                # Проверка развития уже обнаруженных формирований
                await self._check_signal_development(symbol)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Monitor error for {symbol}: {e}")
                await asyncio.sleep(10)
                
    async def _check_classic_entry(self, symbol: str) -> Optional[Dict]:
        """Проверка классических условий входа"""
        try:
            # Получаем текущие данные
            klines_15m = await self.data_manager.get_klines(symbol, '15m', limit=50)
            klines_4h = await self.data_manager.get_klines(symbol, '4h', limit=50)
            
            if not klines_15m or not klines_4h:
                return None
                
            # Анализ MACD
            macd_15m = await self.macd_analyzer.analyze(klines_15m, '15m')
            macd_4h = await self.macd_analyzer.analyze(klines_4h, '4h')
            
            # Проверка обязательных условий
            required_conditions = self.entry_conditions['classic']['required_signals']
            
            # MACD 15m режим 2
            if not macd_15m.get('mode2_active'):
                return None
                
            # MACD 4h подтверждение
            if not macd_4h.get('bullish_confirmation'):
                return None
                
            # Проверка времени (запретные часы)
            if not self._check_trading_time():
                return None
                
            # Получаем последние данные из WebSocket
            ticker_data = self.ws_manager.get_latest_price(symbol)
            if not ticker_data:
                return None
                
            current_price = ticker_data['price']
            
            # Проверка расстояния до экстремумов
            distance_check = await self._check_distance_to_extremes(
                symbol, current_price
            )
            if not distance_check['safe']:
                return None
                
            # Анализ объема и CVD
            volume_analysis = await self._analyze_volume_conditions(symbol)
            
            # Проверка дополнительных сигналов
            optional_count = 0
            optional_details = {}
            
            # RSI
            rsi_check = await self._check_rsi_condition(symbol)
            if rsi_check['signal']:
                optional_count += 1
                optional_details['rsi'] = rsi_check
                
            # Объем
            if volume_analysis['increasing']:
                optional_count += 1
                optional_details['volume'] = volume_analysis
                
            # Поддержка
            support_check = await self._check_support_nearby(symbol, current_price)
            if support_check['nearby']:
                optional_count += 1
                optional_details['support'] = support_check
                
            # Выравнивание трендов
            trend_check = self._check_trend_alignment(macd_15m, macd_4h)
            if trend_check['aligned']:
                optional_count += 1
                optional_details['trend'] = trend_check
                
            # Нужно минимум 2 дополнительных сигнала
            if optional_count < 2:
                return None
                
            # Отслеживаем формирование
            formation_id = f"{symbol}_classic_{datetime.now().timestamp()}"
            self.formation_tracking[symbol][formation_id] = {
                'type': 'classic',
                'start_time': datetime.now(),
                'start_price': current_price,
                'macd_15m': macd_15m,
                'conditions_met': optional_count
            }
            
            # Формируем сигнал
            return {
                'type': 'classic_formation',
                'formation_id': formation_id,
                'symbol': symbol,
                'price': current_price,
                'macd_15m': macd_15m,
                'macd_4h': macd_4h,
                'optional_signals': optional_details,
                'optional_count': optional_count,
                'volume_analysis': volume_analysis,
                'distance_check': distance_check,
                'formation_strength': self._calculate_formation_strength(
                    macd_15m, optional_count, volume_analysis
                ),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Classic entry check error for {symbol}: {e}")
            return None
            
    async def _check_signal_development(self, symbol: str):
        """Проверка развития формирующихся сигналов"""
        if symbol not in self.formation_tracking:
            return
            
        current_time = datetime.now()
        formations_to_remove = []
        
        for formation_id, formation in self.formation_tracking[symbol].items():
            # Проверяем таймаут (5 минут на развитие)
            if (current_time - formation['start_time']).seconds > 300:
                formations_to_remove.append(formation_id)
                continue
                
            # Получаем текущую цену
            ticker_data = self.ws_manager.get_latest_price(symbol)
            if not ticker_data:
                continue
                
            current_price = ticker_data['price']
            
            # Проверяем развитие
            if formation['type'] == 'classic':
                # Проверяем усиление MACD
                klines_15m = await self.data_manager.get_klines(symbol, '15m', limit=10)
                if klines_15m:
                    macd_now = await self.macd_analyzer.analyze(klines_15m, '15m')
                    
                    if self._is_macd_strengthening(formation['macd_15m'], macd_now):
                        # Сигнал усиливается - готов к входу
                        signal = {
                            'type': 'classic_ready',
                            'symbol': symbol,
                            'formation_id': formation_id,
                            'entry_price': current_price,
                            'formation_time': (current_time - formation['start_time']).seconds,
                            'price_change': (current_price - formation['start_price']) / formation['start_price'] * 100,
                            'macd_development': macd_now,
                            'timestamp': datetime.now().isoformat()
                        }
                        
                        await self._emit_signal(symbol, signal)
                        formations_to_remove.append(formation_id)
                        
        # Удаляем обработанные формирования
        for formation_id in formations_to_remove:
            del self.formation_tracking[symbol][formation_id]
            
    async def _handle_ticker_update(self, symbol: str, ticker_data: dict):
        """Обработка обновления тикера"""
        # Обновляем последние данные
        if symbol not in self.last_signals:
            self.last_signals[symbol] = {}
            
        self.last_signals[symbol]['last_price'] = ticker_data['price']
        self.last_signals[symbol]['last_update'] = datetime.now()
        
    async def _handle_kline_update(self, symbol: str, interval: str, kline_data: dict):
        """Обработка обновления свечи"""
        if not kline_data['is_closed']:
            return  # Обрабатываем только закрытые свечи
            
        # Обновляем кеш для быстрого анализа
        if symbol not in self.last_signals:
            self.last_signals[symbol] = {}
            
        self.last_signals[symbol][f'kline_{interval}'] = kline_data
        
    async def _handle_trade_update(self, symbol: str, trade_data: dict):
        """Обработка новой сделки"""
        # Используется для анализа CVD и объема
        pass
        
    async def _handle_depth_update(self, symbol: str, depth_data: dict):
        """Обработка обновления стакана"""
        # Анализ дисбаланса для momentum
        if abs(depth_data['imbalance']) > 0.3:  # Сильный дисбаланс
            if symbol not in self.last_signals:
                self.last_signals[symbol] = {}
                
            self.last_signals[symbol]['orderbook_imbalance'] = depth_data['imbalance']
            self.last_signals[symbol]['orderbook_pressure'] = 'buy' if depth_data['imbalance'] > 0 else 'sell'
            
    async def _handle_momentum_spike(self, spike_info: dict):
        """Обработка обнаружения momentum spike"""
        symbol = spike_info['symbol']
        
        # Проверяем дополнительные условия для momentum burst
        burst_conditions = self.entry_conditions['momentum_burst']['required_signals']
        
        # Проверка объемного спайка
        if spike_info['volume_ratio'] < burst_conditions['volume_spike']:
            return
            
        # Проверка минимального движения цены
        if spike_info['price_change'] > burst_conditions['price_movement']:
            return
            
        # Проверка времени с начала
        # (в данном случае spike только что обнаружен)
        
        # Анализ паттернов burst
        burst_patterns = await self._analyze_burst_patterns(symbol)
        
        # Нужно минимум 3 из 4 паттернов
        pattern_count = sum([
            burst_patterns.get('macd_acceleration', False),
            burst_patterns.get('micro_breakout', False),
            burst_patterns.get('volume_acceleration', False),
            burst_patterns.get('orderbook_imbalance', False)
        ])
        
        if pattern_count >= 3:
            # Формируем сигнал momentum burst
            signal = {
                'type': 'momentum_burst',
                'symbol': symbol,
                'entry_price': spike_info['price'],
                'volume_spike': spike_info['volume_ratio'],
                'price_movement': spike_info['price_change'],
                'burst_patterns': burst_patterns,
                'pattern_count': pattern_count,
                'urgency': 'high',  # Momentum burst требует быстрого входа
                'timestamp': spike_info['timestamp']
            }
            
            await self._emit_signal(symbol, signal)
            
    async def _analyze_burst_patterns(self, symbol: str) -> Dict[str, bool]:
        """Анализ паттернов для momentum burst"""
        patterns = {
            'macd_acceleration': False,
            'micro_breakout': False,
            'volume_acceleration': False,
            'orderbook_imbalance': False
        }
        
        try:
            # MACD ускорение
            klines_5m = await self.data_manager.get_klines(symbol, '5m', limit=20)
            if klines_5m:
                macd_5m = await self.macd_analyzer.analyze(klines_5m, '5m')
                if macd_5m.get('acceleration', 0) > 0:
                    patterns['macd_acceleration'] = True
                    
            # Микро-пробой
            recent_high = max(float(k[2]) for k in klines_5m[-5:]) if klines_5m else 0
            recent_low = min(float(k[3]) for k in klines_5m[-5:]) if klines_5m else 0
            current_price = self.ws_manager.get_latest_price(symbol)
            
            if current_price:
                price = current_price['price']
                if price > recent_high or price < recent_low:
                    patterns['micro_breakout'] = True
                    
            # Ускорение объема
            order_flow = self.ws_manager.analyze_order_flow(symbol)
            if order_flow['buy_volume'] > order_flow['sell_volume'] * 1.5:
                patterns['volume_acceleration'] = True
                
            # Дисбаланс стакана
            orderbook = self.ws_manager.get_orderbook_snapshot(symbol)
            if orderbook and abs(orderbook.get('imbalance', 0)) > 0.3:
                patterns['orderbook_imbalance'] = True
                
        except Exception as e:
            self.logger.error(f"Burst pattern analysis error: {e}")
            
        return patterns
        
    async def _emit_signal(self, symbol: str, signal: dict):
        """Отправка сигнала в фазу 4"""
        self.logger.info(f"Phase 3 signal detected: {symbol} - {signal['type']}")
        
        # Добавляем дополнительную информацию
        signal['phase3_confidence'] = self._calculate_signal_confidence(signal)
        signal['recommended_action'] = self._get_recommended_action(signal)
        
        # Вызываем callback
        if hasattr(self, 'signal_callback'):
            await self.signal_callback(signal)
            
    def _check_trading_time(self) -> bool:
        """Проверка разрешенного времени торговли"""
        # Здесь должна быть проверка restricted_hours из конфига
        # Пока возвращаем True
        return True
        
    async def _check_distance_to_extremes(self, symbol: str, current_price: float) -> Dict:
        """Проверка расстояния до экстремумов"""
        try:
            # Получаем 24h данные
            ticker = await self.data_manager.get_ticker_24h(symbol)
            if not ticker:
                return {'safe': False, 'reason': 'no_data'}
                
            high_24h = float(ticker['highPrice'])
            low_24h = float(ticker['lowPrice'])
            
            # Расчет расстояний
            to_high = (high_24h - current_price) / current_price * 100
            to_low = (current_price - low_24h) / current_price * 100
            
            min_distance = self.entry_conditions['classic']['distance_to_extremes']
            
            return {
                'safe': to_high >= min_distance,
                'to_high': to_high,
                'to_low': to_low,
                'min_required': min_distance
            }
            
        except Exception as e:
            self.logger.error(f"Distance check error: {e}")
            return {'safe': False, 'reason': 'error'}
            
    async def _analyze_volume_conditions(self, symbol: str) -> Dict:
        """Анализ объемных условий"""
        try:
            # Анализ потока ордеров
            order_flow = self.ws_manager.analyze_order_flow(symbol)
            
            # CVD анализ
            trades = self.ws_manager.get_recent_trades(symbol)
            cvd_result = await self.cvd_analyzer.analyze_trades(trades) if trades else {}
            
            # VWAP
            vwap = self.ws_manager.calculate_vwap(symbol, period_minutes=15)
            current_price = self.ws_manager.get_latest_price(symbol)
            
            above_vwap = False
            if vwap and current_price:
                above_vwap = current_price['price'] > vwap
                
            return {
                'increasing': order_flow.get('imbalance', 0) > 0.1,
                'buy_pressure': order_flow.get('imbalance', 0),
                'cvd_positive': cvd_result.get('trend', 'neutral') == 'bullish',
                'above_vwap': above_vwap,
                'large_buyers': order_flow.get('large_buy_volume', 0) > order_flow.get('large_sell_volume', 0)
            }
            
        except Exception as e:
            self.logger.error(f"Volume analysis error: {e}")
            return {'increasing': False}
            
    async def _check_rsi_condition(self, symbol: str) -> Dict:
        """Проверка условий RSI"""
        # Здесь должен быть анализ RSI
        # Пока возвращаем заглушку
        return {
            'signal': False,
            'value': 50,
            'condition': 'neutral'
        }
        
    async def _check_support_nearby(self, symbol: str, current_price: float) -> Dict:
        """Проверка близости к поддержке"""
        # Здесь должен быть анализ уровней
        # Пока возвращаем заглушку
        return {
            'nearby': False,
            'nearest_support': 0,
            'distance_percent': 0
        }
        
    def _check_trend_alignment(self, macd_15m: dict, macd_4h: dict) -> Dict:
        """Проверка выравнивания трендов"""
        aligned = (
            macd_15m.get('trend', 'neutral') == 'bullish' and
            macd_4h.get('trend', 'neutral') == 'bullish'
        )
        
        return {
            'aligned': aligned,
            '15m_trend': macd_15m.get('trend', 'neutral'),
            '4h_trend': macd_4h.get('trend', 'neutral')
        }
        
    def _calculate_formation_strength(self, macd_data: dict, optional_count: int,
                                    volume_analysis: dict) -> float:
        """Расчет силы формирования"""
        strength = 0.0
        
        # MACD вклад
        if macd_data.get('histogram_growing'):
            strength += 30
        if macd_data.get('acceleration', 0) > 0:
            strength += 20
            
        # Дополнительные сигналы
        strength += optional_count * 15
        
        # Объем
        if volume_analysis.get('increasing'):
            strength += 20
            
        return min(strength, 100)
        
    def _is_macd_strengthening(self, old_macd: dict, new_macd: dict) -> bool:
        """Проверка усиления MACD"""
        # Проверяем рост гистограммы
        if (new_macd.get('histogram', 0) > old_macd.get('histogram', 0) and
            new_macd.get('histogram', 0) > 0):
            return True
            
        # Проверяем ускорение
        if new_macd.get('acceleration', 0) > old_macd.get('acceleration', 0):
            return True
            
        return False
        
    def _calculate_signal_confidence(self, signal: dict) -> float:
        """Расчет уверенности в сигнале"""
        confidence = 50.0  # Базовая уверенность
        
        if signal['type'] == 'classic_ready':
            # Классический сигнал
            confidence += 20  # Проверенный паттерн
            if signal.get('optional_count', 0) >= 3:
                confidence += 15
            if signal.get('formation_time', 0) < 180:  # Быстрое формирование
                confidence += 10
                
        elif signal['type'] == 'momentum_burst':
            # Momentum burst
            confidence += 10  # Менее надежный
            if signal.get('pattern_count', 0) >= 4:
                confidence += 20
            if signal.get('volume_spike', 1) > 3:
                confidence += 15
                
        return min(confidence, 100)
        
    def _get_recommended_action(self, signal: dict) -> str:
        """Получение рекомендуемого действия"""
        if signal['type'] == 'momentum_burst':
            return 'immediate_entry'  # Немедленный вход
        elif signal.get('phase3_confidence', 0) > 80:
            return 'strong_entry'  # Сильный сигнал
        elif signal.get('phase3_confidence', 0) > 60:
            return 'normal_entry'  # Обычный вход
        else:
            return 'wait_confirmation'  # Ждать подтверждения
            
    def get_monitoring_status(self) -> Dict:
        """Получение статуса мониторинга"""
        return {
            'active_symbols': list(self.monitoring_symbols),
            'symbols_count': len(self.monitoring_symbols),
            'active_formations': sum(
                len(formations) for formations in self.formation_tracking.values()
            ),
            'websocket_connected': self.ws_manager.is_running
        }