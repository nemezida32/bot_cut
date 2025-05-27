"""
Фаза 4: Верификация сигналов перед исполнением
Цель: Финальная проверка и расчет точных параметров входа
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import numpy as np

from ..core.data_manager import DataManager
from ..core.balance_manager import BalanceManager
from ..ml.movement_predictor import MovementPredictor
from ..indicators.classic.macd import MACDAnalyzer


class Phase4VerifyAnalyzer:
    """
    Финальный верификатор сигналов перед исполнением
    Проверяет сигналы на 1m и 3m таймфреймах, использует ML предсказания
    """
    
    def __init__(self, config: dict, data_manager: DataManager):
        self.config = config
        self.data_manager = data_manager
        self.logger = logging.getLogger(__name__)
        
        # ML предсказатель движения
        self.movement_predictor = MovementPredictor(config)
        
        # Анализатор MACD для финальной проверки
        self.macd_analyzer = MACDAnalyzer(config)
        
        # Настройки верификации
        self.min_probability = self.config['entry_conditions']['classic']['required_signals']['prediction_1_5_percent']
        self.verification_timeout = self.config['scanner']['phase4_timeout']
        
        # История верификаций для анализа
        self.verification_history = []
        
    async def verify_signal(self, signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Верификация сигнала из фазы 3
        
        Args:
            signal: Сигнал для проверки
            
        Returns:
            Верифицированный сигнал с параметрами или None
        """
        start_time = datetime.now()
        symbol = signal['symbol']
        signal_type = signal['type']
        
        self.logger.info(f"Phase 4 verification started for {symbol} - {signal_type}")
        
        try:
            # Параллельная проверка всех условий
            verification_tasks = [
                self._verify_micro_timeframes(symbol),
                self._verify_liquidity(symbol),
                self._verify_market_conditions(symbol),
                self._get_ml_predictions(symbol, signal),
                self._simulate_execution(symbol, signal['entry_price'])
            ]
            
            results = await asyncio.gather(*verification_tasks)
            
            micro_check, liquidity_check, market_check, ml_predictions, execution_sim = results
            
            # Проверка результатов
            if not all([micro_check['passed'], liquidity_check['passed'], 
                       market_check['passed'], execution_sim['feasible']]):
                self.logger.info(
                    f"Verification failed for {symbol}: "
                    f"micro={micro_check['passed']}, "
                    f"liquidity={liquidity_check['passed']}, "
                    f"market={market_check['passed']}, "
                    f"execution={execution_sim['feasible']}"
                )
                return None
                
            # Проверка ML предсказаний
            if ml_predictions['probability_1_5_percent'] < self.min_probability:
                self.logger.info(
                    f"ML prediction too low for {symbol}: "
                    f"{ml_predictions['probability_1_5_percent']:.1f}% < {self.min_probability}%"
                )
                return None
                
            # Расчет оптимальных параметров входа
            entry_params = await self._calculate_entry_parameters(
                symbol, signal, ml_predictions, execution_sim
            )
            
            # Определение уровней DCA
            dca_levels = self._calculate_dca_levels(
                entry_params['entry_price'], 
                ml_predictions, 
                signal_type
            )
            
            # Финальная оценка
            final_confidence = self._calculate_final_confidence(
                signal, micro_check, liquidity_check, 
                market_check, ml_predictions
            )
            
            # Время верификации
            verification_time = (datetime.now() - start_time).total_seconds()
            
            # Формируем верифицированный сигнал
            verified_signal = {
                # Основная информация
                'symbol': symbol,
                'direction': 'LONG',  # В текущей версии только лонги
                'signal_type': signal_type,
                'signal_strength': self._determine_signal_strength(final_confidence),
                
                # Параметры входа
                'entry_price': entry_params['entry_price'],
                'current_price': entry_params['current_price'],
                'slippage_expected': entry_params['expected_slippage'],
                
                # ML прогнозы
                'ml_predictions': ml_predictions,
                'expected_move': ml_predictions['expected_move_percent'],
                'expected_duration': ml_predictions['expected_duration_minutes'],
                
                # Take Profit
                'take_profit': entry_params['take_profit'],
                'take_profit_percent': entry_params['take_profit_percent'],
                
                # DCA уровни
                'dca_levels': dca_levels,
                'dca_enabled': len(dca_levels) > 0,
                
                # Верификация
                'verification_details': {
                    'micro_timeframes': micro_check,
                    'liquidity': liquidity_check,
                    'market_conditions': market_check,
                    'execution_simulation': execution_sim
                },
                
                # Финальные метрики
                'phase4_confidence': final_confidence,
                'ml_confidence': ml_predictions['confidence'],
                'verification_time': verification_time,
                'timestamp': datetime.now().isoformat(),
                
                # Флаг исполнения
                'execute': final_confidence >= 60,  # Минимум 60% уверенности
                
                # Предупреждения
                'warnings': self._generate_warnings(
                    signal, micro_check, liquidity_check, 
                    market_check, ml_predictions
                ),
                
                # Контекст из предыдущих фаз
                'phase3_data': signal
            }
            
            # Сохраняем в историю
            self._save_verification(verified_signal)
            
            self.logger.info(
                f"Phase 4 verification completed for {symbol}: "
                f"confidence={final_confidence:.1f}%, "
                f"execute={verified_signal['execute']}, "
                f"time={verification_time:.2f}s"
            )
            
            return verified_signal
            
        except Exception as e:
            self.logger.error(f"Verification error for {symbol}: {e}")
            return None
            
    async def _verify_micro_timeframes(self, symbol: str) -> Dict[str, Any]:
        """Проверка на 1m и 3m таймфреймах"""
        try:
            # Получаем данные
            klines_1m = await self.data_manager.get_klines(symbol, '1m', limit=30)
            klines_3m = await self.data_manager.get_klines(symbol, '3m', limit=20)
            
            if not klines_1m or not klines_3m:
                return {'passed': False, 'reason': 'no_data'}
                
            # Анализ MACD на микро таймфреймах
            macd_1m = await self.macd_analyzer.analyze(klines_1m, '1m')
            macd_3m = await self.macd_analyzer.analyze(klines_3m, '3m')
            
            # Проверка momentum
            momentum_1m = self._calculate_micro_momentum(klines_1m)
            momentum_3m = self._calculate_micro_momentum(klines_3m)
            
            # Проверка отсутствия резких движений
            recent_spike = self._check_recent_spike(klines_1m)
            
            # Условия прохождения
            passed = (
                macd_1m.get('trend') != 'bearish' and
                macd_3m.get('trend') != 'bearish' and
                momentum_1m['stable'] and
                momentum_3m['stable'] and
                not recent_spike
            )
            
            return {
                'passed': passed,
                'macd_1m': macd_1m.get('trend', 'unknown'),
                'macd_3m': macd_3m.get('trend', 'unknown'),
                'momentum_1m': momentum_1m,
                'momentum_3m': momentum_3m,
                'recent_spike': recent_spike,
                'reason': 'micro_momentum_unstable' if not passed else 'ok'
            }
            
        except Exception as e:
            self.logger.error(f"Micro timeframe verification error: {e}")
            return {'passed': False, 'reason': 'error'}
            
    async def _verify_liquidity(self, symbol: str) -> Dict[str, Any]:
        """Проверка ликвидности"""
        try:
            # Получаем стакан
            orderbook = await self.data_manager.get_orderbook(symbol, limit=20)
            
            if not orderbook:
                return {'passed': False, 'reason': 'no_orderbook'}
                
            # Анализ глубины
            bids = orderbook['bids']
            asks = orderbook['asks']
            
            # Расчет ликвидности в пределах 0.1%
            current_price = float(asks[0][0])
            price_range = current_price * 0.001  # 0.1%
            
            bid_liquidity = sum(
                float(bid[1]) * float(bid[0]) 
                for bid in bids 
                if float(bid[0]) >= current_price - price_range
            )
            
            ask_liquidity = sum(
                float(ask[1]) * float(ask[0]) 
                for ask in asks 
                if float(ask[0]) <= current_price + price_range
            )
            
            # Минимальная ликвидность
            min_liquidity = self.config['filters']['liquidity']['min_bid_ask_depth']
            
            # Спред
            spread = (float(asks[0][0]) - float(bids[0][0])) / float(bids[0][0]) * 100
            max_spread = self.config['filters']['liquidity']['max_spread_percent']
            
            passed = (
                bid_liquidity >= min_liquidity and
                ask_liquidity >= min_liquidity and
                spread <= max_spread
            )
            
            return {
                'passed': passed,
                'bid_liquidity': bid_liquidity,
                'ask_liquidity': ask_liquidity,
                'spread_percent': spread,
                'orderbook_levels': len(bids),
                'reason': 'insufficient_liquidity' if not passed else 'ok'
            }
            
        except Exception as e:
            self.logger.error(f"Liquidity verification error: {e}")
            return {'passed': False, 'reason': 'error'}
            
    async def _verify_market_conditions(self, symbol: str) -> Dict[str, Any]:
        """Проверка общих рыночных условий"""
        try:
            # Проверка корреляции с BTC
            btc_check = await self._check_btc_correlation(symbol)
            
            # Проверка волатильности рынка
            volatility_check = await self._check_market_volatility()
            
            # Проверка новостного фона
            news_check = self._check_news_impact()
            
            # Проверка времени суток
            time_check = self._check_optimal_trading_time()
            
            passed = (
                btc_check['safe'] and
                volatility_check['safe'] and
                news_check['safe'] and
                time_check['optimal']
            )
            
            return {
                'passed': passed,
                'btc_correlation': btc_check,
                'market_volatility': volatility_check,
                'news_impact': news_check,
                'trading_time': time_check,
                'reason': self._get_market_fail_reason(
                    btc_check, volatility_check, news_check, time_check
                ) if not passed else 'ok'
            }
            
        except Exception as e:
            self.logger.error(f"Market conditions verification error: {e}")
            return {'passed': False, 'reason': 'error'}
            
    async def _get_ml_predictions(self, symbol: str, signal: Dict) -> Dict[str, Any]:
        """Получение ML предсказаний"""
        try:
            # Подготовка features для ML
            features = await self._prepare_ml_features(symbol, signal)
            
            # Получение предсказаний
            predictions = await self.movement_predictor.predict(features)
            
            # Анализ симметрии
            symmetry_score = await self._analyze_symmetry_for_prediction(symbol)
            
            # Корректировка предсказаний на основе типа сигнала
            if signal['type'] == 'momentum_burst':
                # Momentum burst обычно дает меньшие движения
                predictions['expected_move_percent'] *= 0.8
                predictions['expected_duration_minutes'] *= 0.7
                
            # Добавляем симметрию к предсказаниям
            if symmetry_score['found']:
                predictions['symmetry_target'] = symmetry_score['expected_move']
                predictions['confidence'] *= 1.1  # Увеличиваем уверенность
                
            return predictions
            
        except Exception as e:
            self.logger.error(f"ML prediction error: {e}")
            # Возвращаем консервативные предсказания
            return {
                'probability_1_percent': 50,
                'probability_1_5_percent': 30,
                'probability_2_percent': 20,
                'expected_move_percent': 1.0,
                'expected_duration_minutes': 60,
                'confidence': 0.5
            }
            
    async def _simulate_execution(self, symbol: str, target_price: float) -> Dict[str, Any]:
        """Симуляция исполнения ордера"""
        try:
            # Получаем текущий стакан
            orderbook = await self.data_manager.get_orderbook(symbol, limit=50)
            
            if not orderbook:
                return {'feasible': False, 'reason': 'no_orderbook'}
                
            # Текущая цена
            current_ask = float(orderbook['asks'][0][0])
            
            # Расчет проскальзывания
            if target_price < current_ask:
                # Цена уже ушла
                slippage = (current_ask - target_price) / target_price * 100
                
                if slippage > 0.2:  # Более 0.2% проскальзывания
                    return {
                        'feasible': False,
                        'reason': 'price_moved',
                        'current_price': current_ask,
                        'target_price': target_price,
                        'slippage_percent': slippage
                    }
                    
            # Симуляция market ордера
            # Здесь должна быть более сложная логика расчета исполнения
            # с учетом размера позиции и глубины стакана
            
            expected_slippage = 0.05  # 0.05% для ликвидных пар
            expected_execution_price = current_ask * (1 + expected_slippage / 100)
            
            return {
                'feasible': True,
                'current_price': current_ask,
                'expected_execution_price': expected_execution_price,
                'expected_slippage_percent': expected_slippage,
                'liquidity_impact': 'low',  # Для малых позиций
                'execution_type': 'MARKET'  # или LIMIT
            }
            
        except Exception as e:
            self.logger.error(f"Execution simulation error: {e}")
            return {'feasible': False, 'reason': 'error'}
            
    async def _calculate_entry_parameters(self, symbol: str, signal: Dict,
                                        ml_predictions: Dict, 
                                        execution_sim: Dict) -> Dict[str, Any]:
        """Расчет оптимальных параметров входа"""
        current_price = execution_sim['current_price']
        expected_execution = execution_sim['expected_execution_price']
        
        # Take Profit на основе ML предсказаний
        base_tp_percent = self.config['exit_conditions']['take_profit']['primary_target']
        
        # Корректировка TP на основе предсказаний
        if ml_predictions['probability_1_5_percent'] > 80:
            tp_percent = base_tp_percent  # Используем полный таргет
        elif ml_predictions['probability_1_5_percent'] > 60:
            tp_percent = base_tp_percent * 0.9  # 90% от таргета
        else:
            tp_percent = base_tp_percent * 0.75  # 75% от таргета
            
        # Для momentum burst используем меньший таргет
        if signal['type'] == 'momentum_burst':
            tp_percent = min(tp_percent, 15)  # Максимум 15% для momentum burst
            
        # Расчет цены TP с учетом плеча
        leverage = self.config['trading']['leverage']
        price_move_percent = tp_percent / leverage
        take_profit_price = expected_execution * (1 + price_move_percent / 100)
        
        return {
            'entry_price': expected_execution,
            'current_price': current_price,
            'expected_slippage': execution_sim['expected_slippage_percent'],
            'take_profit': take_profit_price,
            'take_profit_percent': tp_percent,
            'price_move_required': price_move_percent,
            'leverage': leverage
        }
        
    def _calculate_dca_levels(self, entry_price: float, ml_predictions: Dict,
                            signal_type: str) -> List[Dict[str, Any]]:
        """Расчет уровней DCA"""
        dca_levels = []
        
        # Получаем настройки DCA
        dca_config = self.config['trading']['dca']
        
        if not dca_config['enabled']:
            return dca_levels
            
        # Базовые уровни DCA из конфига
        for level_config in dca_config.get('levels', []):
            trigger_percent = level_config['trigger']
            
            # Расчет цены срабатывания с учетом плеча
            leverage = self.config['trading']['leverage']
            price_drop_percent = abs(trigger_percent) / leverage
            trigger_price = entry_price * (1 - price_drop_percent / 100)
            
            # Проверка вероятности отскока на этом уровне
            # Здесь должен быть более сложный анализ
            bounce_probability = self._estimate_bounce_probability(
                trigger_percent, ml_predictions
            )
            
            if bounce_probability >= level_config['required_probability']:
                dca_levels.append({
                    'level': len(dca_levels) + 1,
                    'trigger_price': trigger_price,
                    'trigger_percent': trigger_percent,
                    'size_multiplier': level_config['size_multiplier'],
                    'bounce_probability': bounce_probability,
                    'analysis_required': True  # Требуется анализ перед исполнением
                })
                
        # Для momentum burst ограничиваем DCA
        if signal_type == 'momentum_burst':
            dca_levels = dca_levels[:1]  # Максимум 1 уровень DCA
            
        return dca_levels
        
    def _calculate_final_confidence(self, signal: Dict, micro_check: Dict,
                                  liquidity_check: Dict, market_check: Dict,
                                  ml_predictions: Dict) -> float:
        """Расчет финальной уверенности в сигнале"""
        confidence = 0.0
        
        # Базовая уверенность от фазы 3
        phase3_confidence = signal.get('phase3_confidence', 50)
        confidence += phase3_confidence * 0.3  # 30% веса
        
        # ML предсказания - основной вес
        ml_confidence = ml_predictions['confidence'] * 100
        probability_score = ml_predictions['probability_1_5_percent']
        confidence += ((ml_confidence + probability_score) / 2) * 0.4  # 40% веса
        
        # Микро проверки
        if micro_check['macd_1m'] == 'bullish':
            confidence += 10
        if not micro_check['recent_spike']:
            confidence += 5
            
        # Ликвидность
        if liquidity_check['spread_percent'] < 0.05:
            confidence += 5
        if liquidity_check['bid_liquidity'] > 50000:
            confidence += 5
            
        # Рыночные условия
        if market_check['btc_correlation']['safe']:
            confidence += 5
        if market_check['trading_time']['optimal']:
            confidence += 5
            
        # Штрафы
        if signal['type'] == 'momentum_burst':
            confidence *= 0.85  # Momentum burst менее надежны
            
        if ml_predictions['expected_move_percent'] < 1.2:
            confidence *= 0.9  # Малый потенциал движения
            
        return min(confidence, 100)
        
    def _determine_signal_strength(self, confidence: float) -> int:
        """Определение силы сигнала (1-5 звезд)"""
        if confidence >= 85:
            return 5
        elif confidence >= 75:
            return 4
        elif confidence >= 65:
            return 3
        elif confidence >= 55:
            return 2
        else:
            return 1
            
    def _generate_warnings(self, signal: Dict, micro_check: Dict,
                         liquidity_check: Dict, market_check: Dict,
                         ml_predictions: Dict) -> List[str]:
        """Генерация предупреждений"""
        warnings = []
        
        # Проверка микро таймфреймов
        if micro_check['recent_spike']:
            warnings.append('recent_price_spike_detected')
            
        # Проверка ликвидности
        if liquidity_check['spread_percent'] > 0.08:
            warnings.append('high_spread')
            
        # Проверка ML
        if ml_predictions['confidence'] < 0.6:
            warnings.append('low_ml_confidence')
            
        if ml_predictions['expected_move_percent'] < 1.0:
            warnings.append('low_profit_potential')
            
        # Проверка типа сигнала
        if signal['type'] == 'momentum_burst':
            warnings.append('momentum_burst_higher_risk')
            
        # Рыночные условия
        if not market_check['trading_time']['optimal']:
            warnings.append('suboptimal_trading_time')
            
        return warnings
        
    def _calculate_micro_momentum(self, klines: List) -> Dict[str, Any]:
        """Расчет momentum на микро таймфреймах"""
        if len(klines) < 5:
            return {'stable': False, 'reason': 'insufficient_data'}
            
        closes = [float(k[4]) for k in klines[-5:]]
        
        # Расчет изменений
        changes = []
        for i in range(1, len(closes)):
            change = (closes[i] - closes[i-1]) / closes[i-1] * 100
            changes.append(change)
            
        # Проверка стабильности
        avg_change = np.mean(np.abs(changes))
        max_change = max(np.abs(changes))
        
        # Стабильный momentum - нет резких движений
        stable = avg_change < 0.1 and max_change < 0.2
        
        return {
            'stable': stable,
            'avg_change': avg_change,
            'max_change': max_change,
            'direction': 'up' if np.mean(changes) > 0 else 'down'
        }
        
    def _check_recent_spike(self, klines_1m: List) -> bool:
        """Проверка недавнего спайка цены"""
        if len(klines_1m) < 10:
            return False
            
        # Последние 10 минут
        recent_klines = klines_1m[-10:]
        closes = [float(k[4]) for k in recent_klines]
        
        # Ищем резкое движение > 0.3%
        for i in range(1, len(closes)):
            change = abs((closes[i] - closes[i-1]) / closes[i-1] * 100)
            if change > 0.3:
                return True
                
        return False
        
    async def _check_btc_correlation(self, symbol: str) -> Dict[str, Any]:
        """Проверка корреляции с BTC"""
        try:
            # Получаем данные BTC
            btc_ticker = await self.data_manager.get_ticker_24h('BTCUSDT')
            symbol_ticker = await self.data_manager.get_ticker_24h(symbol)
            
            if not btc_ticker or not symbol_ticker:
                return {'safe': True, 'reason': 'no_data'}
                
            btc_change = float(btc_ticker['priceChangePercent'])
            symbol_change = float(symbol_ticker['priceChangePercent'])
            
            # Проверка на противоположное движение
            if btc_change < -3 and symbol_change > 0:
                # Альт растет когда BTC падает - рискованно
                return {
                    'safe': False,
                    'reason': 'inverse_correlation',
                    'btc_change': btc_change,
                    'symbol_change': symbol_change
                }
                
            # Проверка на сильное падение BTC
            if btc_change < -5:
                return {
                    'safe': False,
                    'reason': 'btc_crash',
                    'btc_change': btc_change
                }
                
            # Определяем группу корреляции
            btc_influence = self.config['correlations']['btc_influence']
            symbol_base = symbol.replace('USDT', '')
            
            correlation_multiplier = 1.0
            if symbol_base in btc_influence['strong']:
                correlation_multiplier = 0.8
            elif symbol_base in btc_influence['medium']:
                correlation_multiplier = 0.6
            elif symbol_base in btc_influence['weak']:
                correlation_multiplier = 0.4
                
            return {
                'safe': True,
                'btc_change': btc_change,
                'symbol_change': symbol_change,
                'correlation_multiplier': correlation_multiplier
            }
            
        except Exception as e:
            self.logger.error(f"BTC correlation check error: {e}")
            return {'safe': True, 'reason': 'error'}
            
    async def _check_market_volatility(self) -> Dict[str, Any]:
        """Проверка общей волатильности рынка"""
        try:
            # Получаем топ монеты по объему
            tickers = await self.data_manager.get_tickers_24h_batch()
            
            if not tickers:
                return {'safe': True, 'reason': 'no_data'}
                
            # Сортируем по объему
            sorted_tickers = sorted(
                tickers, 
                key=lambda x: float(x['quoteVolume']), 
                reverse=True
            )[:20]  # Топ 20
            
            # Расчет средней волатильности
            volatilities = []
            for ticker in sorted_tickers:
                high = float(ticker['highPrice'])
                low = float(ticker['lowPrice'])
                if low > 0:
                    volatility = (high - low) / low * 100
                    volatilities.append(volatility)
                    
            avg_volatility = np.mean(volatilities) if volatilities else 0
            
            # Проверка экстремальной волатильности
            safe = avg_volatility < 10  # Менее 10% средняя волатильность
            
            return {
                'safe': safe,
                'avg_volatility': avg_volatility,
                'max_volatility': max(volatilities) if volatilities else 0,
                'volatile_symbols': sum(1 for v in volatilities if v > 15)
            }
            
        except Exception as e:
            self.logger.error(f"Market volatility check error: {e}")
            return {'safe': True, 'reason': 'error'}
            
    def _check_news_impact(self) -> Dict[str, Any]:
        """Проверка влияния новостей"""
        # В реальной версии здесь должна быть интеграция с новостными API
        # Пока возвращаем заглушку
        current_time = datetime.now()
        
        # Проверяем время важных событий (например, заседания ФРС)
        # Это упрощенная логика
        if current_time.weekday() == 2 and 18 <= current_time.hour <= 20:
            # Среда, время возможных новостей ФРС
            return {
                'safe': False,
                'reason': 'potential_fed_news',
                'blackout_minutes': 30
            }
            
        return {
            'safe': True,
            'news_sentiment': 'neutral'
        }
        
    def _check_optimal_trading_time(self) -> Dict[str, Any]:
        """Проверка оптимального времени торговли"""
        current_time = datetime.utcnow()
        hour = current_time.hour
        
        # Лучшее время из конфига
        optimal_times = []
        for time_range in self.config['time_patterns']['preferred_hours_utc']:
            start, end = time_range.split('-')
            start_hour = int(start.split(':')[0])
            end_hour = int(end.split(':')[0])
            
            if start_hour <= hour <= end_hour:
                optimal_times.append(time_range)
                
        # Проверка запретного времени
        avoid_times = self.config['time_patterns']['avoid_times']
        day_name = current_time.strftime('%A').lower()
        
        is_restricted = False
        for restriction in avoid_times:
            if restriction['day'] == day_name:
                hours = restriction['hours']
                start, end = hours.split('-')
                start_hour = int(start.split(':')[0])
                end_hour = int(end.split(':')[0])
                
                if start_hour <= hour <= end_hour:
                    is_restricted = True
                    break
                    
        return {
            'optimal': len(optimal_times) > 0 and not is_restricted,
            'current_session': self._get_trading_session(hour),
            'optimal_times': optimal_times,
            'is_restricted': is_restricted
        }
        
    def _get_trading_session(self, hour: int) -> str:
        """Определение торговой сессии"""
        if 0 <= hour < 8:
            return 'asian_early'
        elif 8 <= hour < 12:
            return 'asian_late'
        elif 12 <= hour < 16:
            return 'european_early'
        elif 16 <= hour < 20:
            return 'european_late'
        else:
            return 'american'
            
    async def _prepare_ml_features(self, symbol: str, signal: Dict) -> Dict[str, float]:
        """Подготовка features для ML модели"""
        features = {}
        
        try:
            # Технические индикаторы из сигнала
            if 'macd_15m' in signal:
                features['macd_15m_value'] = signal['macd_15m'].get('value', 0)
                features['macd_15m_signal'] = signal['macd_15m'].get('signal', 0)
                features['macd_15m_histogram'] = signal['macd_15m'].get('histogram', 0)
                
            # Объемные метрики
            if 'volume_analysis' in signal:
                features['volume_increase'] = float(signal['volume_analysis'].get('increasing', False))
                features['buy_pressure'] = signal['volume_analysis'].get('buy_pressure', 0.5)
                
            # Расстояния
            if 'distance_check' in signal:
                features['distance_to_high'] = signal['distance_check'].get('to_high', 0)
                features['distance_to_low'] = signal['distance_check'].get('to_low', 0)
                
            # Рыночные данные
            ticker = await self.data_manager.get_ticker_24h(symbol)
            if ticker:
                features['price_change_24h'] = float(ticker['priceChangePercent'])
                features['volume_24h'] = float(ticker['quoteVolume'])
                features['trades_count_24h'] = float(ticker['count'])
                
            # Добавляем временные features
            now = datetime.now()
            features['hour_of_day'] = now.hour
            features['day_of_week'] = now.weekday()
            
            # Тип сигнала
            features['is_momentum_burst'] = float(signal['type'] == 'momentum_burst')
            features['is_classic'] = float(signal['type'] in ['classic_formation', 'classic_ready'])
            
            # Заполняем отсутствующие значения
            required_features = self.movement_predictor.get_required_features()
            for feature in required_features:
                if feature not in features:
                    features[feature] = 0.0
                    
        except Exception as e:
            self.logger.error(f"Feature preparation error: {e}")
            
        return features
        
    async def _analyze_symmetry_for_prediction(self, symbol: str) -> Dict[str, Any]:
        """Анализ симметрии для корректировки предсказаний"""
        # Здесь должен быть вызов SymmetryAnalyzer
        # Пока возвращаем заглушку
        return {
            'found': False,
            'expected_move': 1.5,
            'confidence': 0.7
        }
        
    def _estimate_bounce_probability(self, drawdown_percent: float, 
                                   ml_predictions: Dict) -> float:
        """Оценка вероятности отскока для DCA"""
        base_probability = 50
        
        # Корректировка на основе просадки
        if abs(drawdown_percent) <= 20:
            base_probability += 10
        elif abs(drawdown_percent) <= 40:
            base_probability += 5
        else:
            base_probability -= 10
            
        # Корректировка на основе ML
        if ml_predictions['confidence'] > 0.7:
            base_probability += 15
            
        # Корректировка на основе ожидаемого движения
        if ml_predictions['expected_move_percent'] > 2:
            base_probability += 10
            
        return min(max(base_probability, 0), 100)
        
    def _get_market_fail_reason(self, btc_check: Dict, volatility_check: Dict,
                               news_check: Dict, time_check: Dict) -> str:
        """Определение причины непрохождения рыночных условий"""
        if not btc_check['safe']:
            return btc_check.get('reason', 'btc_correlation_issue')
        elif not volatility_check['safe']:
            return 'high_market_volatility'
        elif not news_check['safe']:
            return news_check.get('reason', 'news_risk')
        elif not time_check['optimal']:
            return 'suboptimal_trading_time'
        else:
            return 'unknown_market_issue'
            
    def _save_verification(self, verified_signal: Dict):
        """Сохранение верификации для анализа"""
        self.verification_history.append({
            'timestamp': datetime.now(),
            'symbol': verified_signal['symbol'],
            'signal_type': verified_signal['signal_type'],
            'confidence': verified_signal['phase4_confidence'],
            'executed': verified_signal['execute'],
            'ml_prediction': verified_signal['ml_predictions']['expected_move_percent']
        })
        
        # Ограничиваем размер истории
        if len(self.verification_history) > 1000:
            self.verification_history = self.verification_history[-1000:]
            
    def get_verification_stats(self) -> Dict[str, Any]:
        """Получение статистики верификаций"""
        if not self.verification_history:
            return {
                'total_verifications': 0,
                'execution_rate': 0,
                'avg_confidence': 0
            }
            
        total = len(self.verification_history)
        executed = sum(1 for v in self.verification_history if v['executed'])
        avg_confidence = np.mean([v['confidence'] for v in self.verification_history])
        
        # Статистика по типам сигналов
        signal_types = {}
        for v in self.verification_history:
            sig_type = v['signal_type']
            if sig_type not in signal_types:
                signal_types[sig_type] = {'count': 0, 'executed': 0}
            signal_types[sig_type]['count'] += 1
            if v['executed']:
                signal_types[sig_type]['executed'] += 1
                
        return {
            'total_verifications': total,
            'execution_rate': executed / total if total > 0 else 0,
            'avg_confidence': avg_confidence,
            'by_signal_type': signal_types,
            'recent_verifications': self.verification_history[-10:]
        }