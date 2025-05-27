"""
Основной параллельный сканер для анализа монет
"""

import asyncio
import aiohttp
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging
from collections import defaultdict
import json
from concurrent.futures import ThreadPoolExecutor
import numpy as np

from .data_manager import DataManager
from .balance_manager import BalanceManager
from ..analyzers.phase1_global import Phase1GlobalAnalyzer
from ..analyzers.phase2_medium import Phase2MediumAnalyzer
from ..analyzers.phase3_entry import Phase3EntryAnalyzer
from ..analyzers.phase4_verify import Phase4VerifyAnalyzer
from ..monitoring.analytics_logger import AnalyticsLogger


class ParallelScanner:
    """Высокоскоростной параллельный сканер для анализа криптовалют"""
    
    def __init__(self, config: dict, analytics_logger: AnalyticsLogger):
        self.config = config
        self.analytics_logger = analytics_logger
        self.logger = logging.getLogger(__name__)
        
        # Менеджер данных для кеширования
        self.data_manager = DataManager(config)
        
        # Менеджер баланса для расчета позиций
        self.balance_manager = BalanceManager(config)
        
        # Анализаторы для каждой фазы
        self.phase1_analyzer = Phase1GlobalAnalyzer(config, self.data_manager)
        self.phase2_analyzer = Phase2MediumAnalyzer(config, self.data_manager)
        self.phase3_analyzer = Phase3EntryAnalyzer(config, self.data_manager)
        self.phase4_analyzer = Phase4VerifyAnalyzer(config, self.data_manager)
        
        # Настройки сканера
        self.scanner_config = config['scanner']
        self.batch_size = self.scanner_config['batch_size']
        self.max_workers = self.scanner_config['max_workers']
        
        # Результаты фаз
        self.phase_results = {
            'phase1': {'symbols': [], 'timestamp': None, 'market_type': None},
            'phase2': {'symbols': [], 'timestamp': None},
            'phase3': {'symbols': [], 'timestamp': None},
            'phase4': {'signals': [], 'timestamp': None}
        }
        
        # Таймеры для фаз
        self.phase_timers = {
            'phase1': None,
            'phase2': None,
            'phase3': None,
            'phase4': None
        }
        
        # Статистика производительности
        self.performance_stats = defaultdict(list)
        
        # HTTP сессия для API запросов
        self.session = None
        
        # Флаг работы сканера
        self.is_running = False
        
    async def start(self):
        """Запуск сканера"""
        self.logger.info("Starting parallel scanner...")
        
        # Создаем HTTP сессию
        self.session = aiohttp.ClientSession()
        
        # Инициализируем менеджеры
        await self.data_manager.initialize()
        await self.balance_manager.initialize()
        
        # Получаем список всех торгуемых символов
        await self._init_symbols()
        
        self.is_running = True
        
        # Запускаем циклы для каждой фазы
        tasks = [
            asyncio.create_task(self._phase1_loop()),
            asyncio.create_task(self._phase2_loop()),
            asyncio.create_task(self._phase3_loop()),
            asyncio.create_task(self._phase4_loop()),
            asyncio.create_task(self._performance_monitor_loop())
        ]
        
        self.logger.info("Scanner started successfully")
        
        # Ждем завершения (бесконечный цикл)
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"Scanner error: {e}")
            await self.stop()
            
    async def stop(self):
        """Остановка сканера"""
        self.logger.info("Stopping scanner...")
        self.is_running = False
        
        if self.session:
            await self.session.close()
            
        self.logger.info("Scanner stopped")
        
    async def _init_symbols(self):
        """Инициализация списка символов"""
        try:
            # Получаем информацию о всех фьючерсах
            url = f"{self.config['binance']['futures_base_url']}/fapi/v1/exchangeInfo"
            async with self.session.get(url) as response:
                data = await response.json()
                
            # Фильтруем активные USDT пары
            self.all_symbols = []
            for symbol_info in data['symbols']:
                if (symbol_info['status'] == 'TRADING' and 
                    symbol_info['quoteAsset'] == 'USDT' and
                    symbol_info['symbol'] not in self.scanner_config['excluded_symbols']):
                    self.all_symbols.append(symbol_info['symbol'])
                    
            self.logger.info(f"Initialized {len(self.all_symbols)} trading symbols")
            
        except Exception as e:
            self.logger.error(f"Error initializing symbols: {e}")
            self.all_symbols = []
            
    async def _phase1_loop(self):
        """Цикл фазы 1: Глобальный анализ"""
        interval = self.scanner_config['phase1_interval'] * 60  # в секундах
        
        while self.is_running:
            try:
                start_time = time.time()
                
                # Выполняем анализ
                top_symbols, market_type = await self._run_phase1()
                
                # Сохраняем результаты
                self.phase_results['phase1'] = {
                    'symbols': top_symbols[:50],  # Топ-50 для фазы 2
                    'timestamp': datetime.now(),
                    'market_type': market_type,
                    'execution_time': time.time() - start_time
                }
                
                # Логируем производительность
                self.performance_stats['phase1'].append({
                    'timestamp': datetime.now(),
                    'execution_time': time.time() - start_time,
                    'symbols_analyzed': len(self.all_symbols),
                    'symbols_selected': len(top_symbols)
                })
                
                self.logger.info(
                    f"Phase 1 completed in {time.time() - start_time:.2f}s. "
                    f"Selected {len(top_symbols)} symbols. Market type: {market_type}"
                )
                
                # Ждем следующий цикл
                await asyncio.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"Phase 1 error: {e}")
                await asyncio.sleep(30)  # Пауза при ошибке
                
    async def _run_phase1(self) -> Tuple[List[str], str]:
        """Выполнение фазы 1: параллельный анализ всех монет"""
        # Разбиваем символы на батчи
        batches = [
            self.all_symbols[i:i + self.batch_size] 
            for i in range(0, len(self.all_symbols), self.batch_size)
        ]
        
        # Анализируем батчи параллельно
        tasks = []
        for batch in batches:
            task = asyncio.create_task(self.phase1_analyzer.analyze_batch(batch))
            tasks.append(task)
            
        # Ждем завершения всех батчей
        batch_results = await asyncio.gather(*tasks)
        
        # Объединяем результаты
        all_scores = []
        for result in batch_results:
            if result:
                all_scores.extend(result['symbol_scores'])
                
        # Сортируем по score и берем топ
        all_scores.sort(key=lambda x: x['score'], reverse=True)
        top_symbols = [s['symbol'] for s in all_scores[:50]]
        
        # Определяем тип рынка
        market_type = await self.phase1_analyzer.classify_market(batch_results)
        
        return top_symbols, market_type
        
    async def _phase2_loop(self):
        """Цикл фазы 2: Средний анализ"""
        interval = self.scanner_config['phase2_interval'] * 60
        
        while self.is_running:
            try:
                # Ждем результатов фазы 1
                if not self.phase_results['phase1']['symbols']:
                    await asyncio.sleep(10)
                    continue
                    
                start_time = time.time()
                
                # Берем символы из фазы 1
                symbols = self.phase_results['phase1']['symbols']
                market_type = self.phase_results['phase1']['market_type']
                
                # Выполняем анализ
                top_symbols = await self._run_phase2(symbols, market_type)
                
                # Сохраняем результаты
                self.phase_results['phase2'] = {
                    'symbols': top_symbols[:15],  # Топ-15 для фазы 3
                    'timestamp': datetime.now(),
                    'execution_time': time.time() - start_time
                }
                
                self.logger.info(
                    f"Phase 2 completed in {time.time() - start_time:.2f}s. "
                    f"Selected {len(top_symbols)} symbols"
                )
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"Phase 2 error: {e}")
                await asyncio.sleep(30)
                
    async def _run_phase2(self, symbols: List[str], market_type: str) -> List[str]:
        """Выполнение фазы 2: детальный анализ топ-50"""
        # Анализируем символы параллельно
        tasks = []
        for symbol in symbols:
            task = asyncio.create_task(
                self.phase2_analyzer.analyze_symbol(symbol, market_type)
            )
            tasks.append(task)
            
        # Ждем результаты
        results = await asyncio.gather(*tasks)
        
        # Фильтруем и сортируем
        valid_results = [r for r in results if r and r['potential_score'] > 0]
        valid_results.sort(key=lambda x: x['potential_score'], reverse=True)
        
        # Возвращаем топ символы
        return [r['symbol'] for r in valid_results[:15]]
        
    async def _phase3_loop(self):
        """Цикл фазы 3: Точечный вход (real-time)"""
        # Фаза 3 работает в реальном времени через WebSocket
        while self.is_running:
            try:
                # Ждем результатов фазы 2
                if not self.phase_results['phase2']['symbols']:
                    await asyncio.sleep(5)
                    continue
                    
                # Берем символы для мониторинга
                symbols = self.phase_results['phase2']['symbols']
                
                # Запускаем real-time мониторинг
                await self.phase3_analyzer.monitor_symbols(
                    symbols,
                    callback=self._phase3_signal_callback
                )
                
            except Exception as e:
                self.logger.error(f"Phase 3 error: {e}")
                await asyncio.sleep(10)
                
    async def _phase3_signal_callback(self, signal: dict):
        """Обработка сигнала из фазы 3"""
        self.logger.info(f"Phase 3 signal: {signal['symbol']} - {signal['type']}")
        
        # Добавляем в очередь для фазы 4
        if 'phase3' not in self.phase_results:
            self.phase_results['phase3'] = {'signals': []}
            
        self.phase_results['phase3']['signals'].append({
            'signal': signal,
            'timestamp': datetime.now()
        })
        
    async def _phase4_loop(self):
        """Цикл фазы 4: Верификация сигналов"""
        while self.is_running:
            try:
                # Проверяем очередь сигналов из фазы 3
                if ('phase3' in self.phase_results and 
                    self.phase_results['phase3'].get('signals')):
                    
                    # Берем сигнал из очереди
                    signal_data = self.phase_results['phase3']['signals'].pop(0)
                    signal = signal_data['signal']
                    
                    # Верифицируем сигнал
                    start_time = time.time()
                    verified_signal = await self.phase4_analyzer.verify_signal(signal)
                    
                    if verified_signal and verified_signal['execute']:
                        # ВАЖНО: Рассчитываем размер позиции от баланса
                        position_params = await self.balance_manager.calculate_position_size(verified_signal)
                        
                        if position_params['can_trade']:
                            # Добавляем параметры позиции к сигналу
                            verified_signal.update(position_params)
                            
                            # Логируем готовый сигнал
                            signal_id = self.analytics_logger.log_signal(verified_signal)
                            
                            # Логируем расчет позиции
                            self.balance_manager.log_position_calculation(position_params)
                            
                            # Добавляем в результаты
                            self.phase_results['phase4']['signals'].append({
                                'signal_id': signal_id,
                                'signal': verified_signal,
                                'timestamp': datetime.now(),
                                'verification_time': time.time() - start_time
                            })
                            
                            self.logger.info(
                                f"Phase 4 verified signal: {verified_signal['symbol']} - "
                                f"Size: ${position_params['position_size_usdt']:.2f} "
                                f"({position_params['position_size_percent']:.2f}% of balance), "
                                f"Leverage: {position_params['leverage']}x"
                            )
                        else:
                            self.logger.warning(
                                f"Cannot trade {verified_signal['symbol']}: {position_params['reason']}"
                            )
                        
                await asyncio.sleep(1)  # Небольшая пауза между проверками
                
            except Exception as e:
                self.logger.error(f"Phase 4 error: {e}")
                await asyncio.sleep(5)
                
    async def _performance_monitor_loop(self):
        """Мониторинг производительности"""
        while self.is_running:
            try:
                # Каждый час генерируем отчет о производительности
                await asyncio.sleep(3600)
                
                report = self._generate_performance_report()
                self.logger.info(f"Performance report: {json.dumps(report, indent=2)}")
                
                # Очищаем старую статистику
                self._cleanup_old_stats()
                
            except Exception as e:
                self.logger.error(f"Performance monitor error: {e}")
                
    def _generate_performance_report(self) -> dict:
        """Генерация отчета о производительности"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'uptime_hours': self._calculate_uptime(),
            'phases': {}
        }
        
        # Статистика по каждой фазе
        for phase, stats in self.performance_stats.items():
            if stats:
                recent_stats = stats[-100:]  # Последние 100 записей
                exec_times = [s['execution_time'] for s in recent_stats]
                
                report['phases'][phase] = {
                    'avg_execution_time': np.mean(exec_times),
                    'max_execution_time': np.max(exec_times),
                    'min_execution_time': np.min(exec_times),
                    'total_executions': len(stats)
                }
                
        # Статистика сигналов
        if 'phase4' in self.phase_results:
            signals = self.phase_results['phase4'].get('signals', [])
            report['signals'] = {
                'total_verified': len(signals),
                'last_signal_time': signals[-1]['timestamp'].isoformat() if signals else None
            }
            
        return report
        
    def _calculate_uptime(self) -> float:
        """Расчет времени работы в часах"""
        if hasattr(self, 'start_time'):
            return (datetime.now() - self.start_time).total_seconds() / 3600
        return 0
        
    def _cleanup_old_stats(self):
        """Очистка старой статистики"""
        max_records = 1000
        
        for phase in self.performance_stats:
            if len(self.performance_stats[phase]) > max_records:
                self.performance_stats[phase] = self.performance_stats[phase][-max_records:]
                
    def get_phase_results(self, phase: int) -> dict:
        """Получение результатов конкретной фазы"""
        phase_key = f'phase{phase}'
        
        if phase_key not in self.phase_results:
            return {'error': 'Phase not found'}
            
        results = self.phase_results[phase_key].copy()
        
        # Форматируем для отображения
        if phase in [1, 2]:
            # Для фаз 1 и 2 показываем только символы
            return {
                'symbols': results.get('symbols', []),
                'count': len(results.get('symbols', [])),
                'timestamp': results.get('timestamp', datetime.now()).isoformat(),
                'execution_time': results.get('execution_time', 0)
            }
        elif phase == 3:
            # Для фазы 3 показываем очередь сигналов
            return {
                'pending_signals': len(results.get('signals', [])),
                'symbols_monitoring': self.phase_results['phase2'].get('symbols', []),
                'timestamp': datetime.now().isoformat()
            }
        elif phase == 4:
            # Для фазы 4 показываем проверенные сигналы
            recent_signals = results.get('signals', [])[-5:]  # Последние 5
            return {
                'recent_signals': [
                    {
                        'symbol': s['signal']['symbol'],
                        'confidence': s['signal']['phase4_confidence'],
                        'timestamp': s['timestamp'].isoformat()
                    }
                    for s in recent_signals
                ],
                'total_verified': len(results.get('signals', [])),
                'timestamp': datetime.now().isoformat()
            }
            
        return {}
        
    def get_all_phases_summary(self) -> dict:
        """Получение сводки по всем фазам"""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'scanner_status': 'running' if self.is_running else 'stopped',
            'phases': {}
        }
        
        for i in range(1, 5):
            phase_data = self.get_phase_results(i)
            summary['phases'][f'phase{i}'] = phase_data
            
        # Добавляем производительность
        summary['performance'] = self._generate_performance_report()
        
        return summary
        
    async def force_rescan(self, phase: int = None):
        """Принудительный запуск сканирования"""
        self.logger.info(f"Force rescan requested for phase {phase if phase else 'all'}")
        
        if phase == 1 or phase is None:
            # Сбрасываем таймер фазы 1
            self.phase_timers['phase1'] = 0
            
        if phase == 2 or phase is None:
            # Сбрасываем таймер фазы 2
            self.phase_timers['phase2'] = 0
            
        # Фазы 3 и 4 работают в реальном времени
        
    def update_config(self, new_config: dict):
        """Обновление конфигурации на лету"""
        self.logger.info("Updating scanner configuration")
        
        # Обновляем только разрешенные параметры
        if 'scanner' in new_config:
            for key in ['phase1_interval', 'phase2_interval', 'batch_size']:
                if key in new_config['scanner']:
                    self.scanner_config[key] = new_config['scanner'][key]
                    
        # Передаем обновления анализаторам
        self.phase1_analyzer.update_config(new_config)
        self.phase2_analyzer.update_config(new_config)
        self.phase3_analyzer.update_config(new_config)
        self.phase4_analyzer.update_config(new_config)