"""
Специальный логгер для последующего анализа результатов торговли
"""

import json
import logging
import os
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from pathlib import Path
import asyncio
from collections import defaultdict
import statistics


class AnalyticsLogger:
    """Специальный логгер для последующего анализа Claude и оптимизации стратегий"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logs_dir = Path("logs")
        self.logs_dir.mkdir(exist_ok=True)
        
        # Файлы логов
        self.analysis_log = self.logs_dir / "analysis.log"
        self.performance_file = self.logs_dir / "performance.json"
        self.signals_db = self.logs_dir / "signals_db.json"
        
        # Кеш для производительности
        self.signals_cache = []
        self.performance_cache = defaultdict(list)
        
        # Настройка базового логгера
        self._setup_logger()
        
    def _setup_logger(self):
        """Настройка базового логгера"""
        self.logger = logging.getLogger("analytics")
        self.logger.setLevel(logging.INFO)
        
        # Форматтер для читаемости
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Handler для файла
        file_handler = logging.FileHandler(self.analysis_log)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
    def log_signal(self, signal_data: dict) -> str:
        """
        Логирует каждый сигнал с полным контекстом
        
        Args:
            signal_data: Данные сигнала со всеми индикаторами и прогнозами
            
        Returns:
            signal_id: Уникальный ID сигнала для отслеживания
        """
        # Генерируем уникальный ID
        signal_id = f"{datetime.now().isoformat()}_{signal_data.get('symbol', 'UNKNOWN')}_{signal_data.get('direction', 'LONG')}"
        
        # Собираем полную информацию о сигнале
        entry = {
            'signal_id': signal_id,
            'timestamp': datetime.now().isoformat(),
            'symbol': signal_data.get('symbol'),
            'direction': signal_data.get('direction', 'LONG'),
            
            # Прогрессия по фазам
            'phase_progression': {
                'phase1_score': signal_data.get('phase1_score', 0),
                'phase1_rank': signal_data.get('phase1_rank', 0),
                'phase2_rank': signal_data.get('phase2_rank', 0),
                'phase2_indicators': signal_data.get('phase2_indicators', {}),
                'phase3_timing': signal_data.get('phase3_timing'),
                'phase3_type': signal_data.get('signal_type', 'classic'),
                'phase4_confidence': signal_data.get('phase4_confidence', 0),
                'phase4_ml_score': signal_data.get('ml_confidence', 0)
            },
            
            # Все значения индикаторов
            'indicators': {
                'macd': signal_data.get('macd_values', {}),
                'rsi': signal_data.get('rsi_values', {}),
                'volume': signal_data.get('volume_data', {}),
                'momentum': signal_data.get('momentum_data', {}),
                'support_resistance': signal_data.get('sr_levels', {}),
                'market_structure': signal_data.get('market_structure', {})
            },
            
            # ML предсказания
            'ml_predictions': {
                'movement_1_percent': signal_data.get('pred_1_percent', {}),
                'movement_1_5_percent': signal_data.get('pred_1_5_percent', {}),
                'movement_2_percent': signal_data.get('pred_2_percent', {}),
                'symmetry_analysis': signal_data.get('symmetry_data', {}),
                'expected_duration': signal_data.get('expected_duration_min', 0)
            },
            
            # Контекст рынка
            'market_context': {
                'global_trend': signal_data.get('global_trend'),
                'market_phase': signal_data.get('market_phase'),
                'btc_correlation': signal_data.get('btc_correlation'),
                'sector_strength': signal_data.get('sector_strength'),
                'volatility_regime': signal_data.get('volatility_regime')
            },
            
            # Решение и параметры
            'decision': {
                'action': signal_data.get('action', 'NO_TRADE'),
                'signal_strength': signal_data.get('signal_strength', 0),
                'confidence': signal_data.get('final_confidence', 0),
                'reasons': signal_data.get('reasons', []),
                'warnings': signal_data.get('warnings', [])
            },
            
            # Параметры исполнения
            'execution_params': {
                'position_size_percent': signal_data.get('position_size_percent'),
                'leverage': signal_data.get('leverage'),
                'entry_price': signal_data.get('entry_price'),
                'take_profit': signal_data.get('take_profit'),
                'dca_levels': signal_data.get('dca_levels', [])
            },
            
            # Результат (заполняется позже)
            'execution': None,
            'result': None
        }
        
        # Сохраняем в кеш и файл
        self.signals_cache.append(entry)
        self._append_to_json_log(self.signals_db, entry)
        
        # Логируем краткую информацию
        self.logger.info(f"Signal logged: {signal_id} - {entry['decision']['action']}")
        
        return signal_id
        
    def log_execution_result(self, signal_id: str, execution_data: dict):
        """
        Логирует результат исполнения сигнала
        
        Args:
            signal_id: ID сигнала
            execution_data: Данные об исполнении и результате
        """
        result = {
            'execution_timestamp': datetime.now().isoformat(),
            'entry_price': execution_data.get('entry_price'),
            'entry_slippage': execution_data.get('entry_slippage', 0),
            'position_size_usdt': execution_data.get('position_size_usdt'),
            'actual_leverage': execution_data.get('actual_leverage'),
            
            # Результат сделки
            'exit_price': execution_data.get('exit_price'),
            'exit_timestamp': execution_data.get('exit_timestamp'),
            'pnl_usdt': execution_data.get('pnl_usdt'),
            'pnl_percent': execution_data.get('pnl_percent'),
            'roe_percent': execution_data.get('roe_percent'),
            
            # Детали исполнения
            'duration_minutes': execution_data.get('duration_minutes'),
            'max_profit_percent': execution_data.get('max_profit_percent'),
            'max_drawdown_percent': execution_data.get('max_drawdown_percent'),
            'exit_reason': execution_data.get('exit_reason'),
            
            # DCA информация
            'dca_triggered': execution_data.get('dca_count', 0) > 0,
            'dca_count': execution_data.get('dca_count', 0),
            'dca_details': execution_data.get('dca_details', []),
            
            # Метрики для анализа
            'price_movement_percent': execution_data.get('price_movement_percent'),
            'target_achieved': execution_data.get('target_achieved', False),
            'ml_prediction_accuracy': execution_data.get('ml_accuracy')
        }
        
        # Обновляем сигнал в кеше и файле
        self._update_signal_result(signal_id, result)
        
        # Обновляем статистику производительности
        self._update_performance_stats(signal_id, result)
        
        self.logger.info(
            f"Execution result: {signal_id} - "
            f"PnL: {result['pnl_percent']:.2f}% - "
            f"Reason: {result['exit_reason']}"
        )
        
    def generate_daily_report(self) -> dict:
        """Генерирует дневной отчет для анализа"""
        today = date.today().isoformat()
        
        # Собираем все сигналы за день
        today_signals = [
            s for s in self.signals_cache 
            if s['timestamp'].startswith(today)
        ]
        
        # Базовая статистика
        total_signals = len(today_signals)
        executed_signals = [s for s in today_signals if s['execution'] is not None]
        profitable_trades = [s for s in executed_signals if s['result']['pnl_percent'] > 0]
        
        # Расчет метрик
        report = {
            'date': today,
            'timestamp': datetime.now().isoformat(),
            
            # Общая статистика
            'summary': {
                'total_signals_generated': total_signals,
                'signals_executed': len(executed_signals),
                'execution_rate': self._safe_divide(len(executed_signals), total_signals),
                'profitable_trades': len(profitable_trades),
                'success_rate': self._safe_divide(len(profitable_trades), len(executed_signals)),
                'total_pnl_percent': sum(s['result']['pnl_percent'] for s in executed_signals) if executed_signals else 0,
                'average_pnl_percent': self._calculate_average([s['result']['pnl_percent'] for s in executed_signals]),
                'profit_factor': self._calculate_profit_factor(executed_signals)
            },
            
            # Анализ по типам сигналов
            'signal_types_analysis': self._analyze_signal_types(today_signals),
            
            # Анализ по символам
            'symbol_performance': self._analyze_symbol_performance(executed_signals),
            
            # Анализ индикаторов
            'indicator_effectiveness': self._analyze_indicator_performance(executed_signals),
            
            # ML точность
            'ml_accuracy': self._evaluate_ml_predictions(executed_signals),
            
            # Анализ по времени
            'time_analysis': self._analyze_time_patterns(executed_signals),
            
            # Анализ режимов
            'mode_analysis': self._analyze_trading_modes(executed_signals),
            
            # Условия рынка
            'market_conditions': self._summarize_market_conditions(today_signals),
            
            # Рекомендации
            'recommendations': self._generate_recommendations(report)
        }
        
        # Сохраняем отчет
        self._save_performance_report(report)
        
        return report
        
    def _analyze_signal_types(self, signals: List[dict]) -> dict:
        """Анализ эффективности разных типов сигналов"""
        signal_types = defaultdict(lambda: {'count': 0, 'executed': 0, 'profitable': 0, 'total_pnl': 0})
        
        for signal in signals:
            sig_type = signal['phase_progression'].get('phase3_type', 'classic')
            signal_types[sig_type]['count'] += 1
            
            if signal['execution']:
                signal_types[sig_type]['executed'] += 1
                pnl = signal['result']['pnl_percent']
                signal_types[sig_type]['total_pnl'] += pnl
                if pnl > 0:
                    signal_types[sig_type]['profitable'] += 1
                    
        # Расчет метрик для каждого типа
        result = {}
        for sig_type, data in signal_types.items():
            result[sig_type] = {
                'count': data['count'],
                'execution_rate': self._safe_divide(data['executed'], data['count']),
                'success_rate': self._safe_divide(data['profitable'], data['executed']),
                'avg_pnl': self._safe_divide(data['total_pnl'], data['executed']),
                'total_pnl': data['total_pnl']
            }
            
        return result
        
    def _analyze_symbol_performance(self, executed_signals: List[dict]) -> dict:
        """Анализ производительности по символам"""
        symbol_stats = defaultdict(lambda: {'trades': 0, 'profitable': 0, 'total_pnl': 0, 'durations': []})
        
        for signal in executed_signals:
            symbol = signal['symbol']
            pnl = signal['result']['pnl_percent']
            duration = signal['result']['duration_minutes']
            
            symbol_stats[symbol]['trades'] += 1
            symbol_stats[symbol]['total_pnl'] += pnl
            symbol_stats[symbol]['durations'].append(duration)
            if pnl > 0:
                symbol_stats[symbol]['profitable'] += 1
                
        # Топ и худшие символы
        sorted_symbols = sorted(
            symbol_stats.items(), 
            key=lambda x: x[1]['total_pnl'], 
            reverse=True
        )
        
        return {
            'top_5_performers': [
                {
                    'symbol': sym,
                    'trades': data['trades'],
                    'success_rate': self._safe_divide(data['profitable'], data['trades']),
                    'total_pnl': data['total_pnl'],
                    'avg_duration': self._calculate_average(data['durations'])
                }
                for sym, data in sorted_symbols[:5]
            ],
            'worst_5_performers': [
                {
                    'symbol': sym,
                    'trades': data['trades'],
                    'success_rate': self._safe_divide(data['profitable'], data['trades']),
                    'total_pnl': data['total_pnl'],
                    'avg_duration': self._calculate_average(data['durations'])
                }
                for sym, data in sorted_symbols[-5:] if data['total_pnl'] < 0
            ]
        }
        
    def _analyze_indicator_performance(self, executed_signals: List[dict]) -> dict:
        """Анализ эффективности индикаторов"""
        indicator_stats = defaultdict(lambda: {'triggered': 0, 'profitable': 0, 'total_pnl': 0})
        
        for signal in executed_signals:
            indicators = signal['indicators']
            pnl = signal['result']['pnl_percent']
            
            # Проверяем какие индикаторы были активны
            if indicators.get('macd', {}).get('bullish_signal'):
                indicator_stats['macd']['triggered'] += 1
                indicator_stats['macd']['total_pnl'] += pnl
                if pnl > 0:
                    indicator_stats['macd']['profitable'] += 1
                    
            if indicators.get('volume', {}).get('spike_detected'):
                indicator_stats['volume_spike']['triggered'] += 1
                indicator_stats['volume_spike']['total_pnl'] += pnl
                if pnl > 0:
                    indicator_stats['volume_spike']['profitable'] += 1
                    
            # И так далее для других индикаторов...
            
        # Расчет эффективности
        result = {}
        for indicator, stats in indicator_stats.items():
            if stats['triggered'] > 0:
                result[indicator] = {
                    'triggers': stats['triggered'],
                    'success_rate': self._safe_divide(stats['profitable'], stats['triggered']),
                    'avg_pnl': self._safe_divide(stats['total_pnl'], stats['triggered']),
                    'contribution_score': stats['total_pnl']
                }
                
        return dict(sorted(result.items(), key=lambda x: x[1]['contribution_score'], reverse=True))
        
    def _evaluate_ml_predictions(self, executed_signals: List[dict]) -> dict:
        """Оценка точности ML предсказаний"""
        ml_stats = {
            '1_percent': {'predicted': 0, 'achieved': 0},
            '1_5_percent': {'predicted': 0, 'achieved': 0},
            '2_percent': {'predicted': 0, 'achieved': 0},
            'duration': {'errors': [], 'predictions': []}
        }
        
        for signal in executed_signals:
            ml_pred = signal['ml_predictions']
            actual_movement = abs(signal['result']['price_movement_percent'])
            actual_duration = signal['result']['duration_minutes']
            
            # Проверка предсказаний движения
            if ml_pred['movement_1_percent'].get('probability', 0) > 0.7:
                ml_stats['1_percent']['predicted'] += 1
                if actual_movement >= 1.0:
                    ml_stats['1_percent']['achieved'] += 1
                    
            if ml_pred['movement_1_5_percent'].get('probability', 0) > 0.7:
                ml_stats['1_5_percent']['predicted'] += 1
                if actual_movement >= 1.5:
                    ml_stats['1_5_percent']['achieved'] += 1
                    
            # Ошибка предсказания длительности
            if ml_pred.get('expected_duration'):
                predicted_duration = ml_pred['expected_duration']
                error = abs(predicted_duration - actual_duration) / actual_duration
                ml_stats['duration']['errors'].append(error)
                ml_stats['duration']['predictions'].append({
                    'predicted': predicted_duration,
                    'actual': actual_duration
                })
                
        return {
            'movement_accuracy': {
                '1_percent': self._safe_divide(
                    ml_stats['1_percent']['achieved'], 
                    ml_stats['1_percent']['predicted']
                ),
                '1_5_percent': self._safe_divide(
                    ml_stats['1_5_percent']['achieved'], 
                    ml_stats['1_5_percent']['predicted']
                ),
                '2_percent': self._safe_divide(
                    ml_stats['2_percent']['achieved'], 
                    ml_stats['2_percent']['predicted']
                )
            },
            'duration_accuracy': {
                'mean_error_percent': self._calculate_average(ml_stats['duration']['errors']) * 100,
                'samples': len(ml_stats['duration']['errors'])
            }
        }
        
    def _analyze_time_patterns(self, executed_signals: List[dict]) -> dict:
        """Анализ паттернов по времени"""
        hourly_stats = defaultdict(lambda: {'trades': 0, 'profitable': 0, 'total_pnl': 0})
        
        for signal in executed_signals:
            hour = datetime.fromisoformat(signal['timestamp']).hour
            pnl = signal['result']['pnl_percent']
            
            hourly_stats[hour]['trades'] += 1
            hourly_stats[hour]['total_pnl'] += pnl
            if pnl > 0:
                hourly_stats[hour]['profitable'] += 1
                
        # Лучшие и худшие часы
        best_hours = []
        worst_hours = []
        
        for hour, stats in hourly_stats.items():
            if stats['trades'] >= 3:  # Минимум 3 сделки для статистики
                success_rate = self._safe_divide(stats['profitable'], stats['trades'])
                avg_pnl = self._safe_divide(stats['total_pnl'], stats['trades'])
                
                hour_data = {
                    'hour': f"{hour:02d}:00",
                    'trades': stats['trades'],
                    'success_rate': success_rate,
                    'avg_pnl': avg_pnl
                }
                
                if success_rate > 0.7 and avg_pnl > 0:
                    best_hours.append(hour_data)
                elif success_rate < 0.3 or avg_pnl < -5:
                    worst_hours.append(hour_data)
                    
        return {
            'best_trading_hours': sorted(best_hours, key=lambda x: x['avg_pnl'], reverse=True)[:3],
            'worst_trading_hours': sorted(worst_hours, key=lambda x: x['avg_pnl'])[:3],
            'hourly_distribution': dict(sorted(hourly_stats.items()))
        }
        
    def _analyze_trading_modes(self, executed_signals: List[dict]) -> dict:
        """Анализ эффективности режимов торговли"""
        mode_stats = defaultdict(lambda: {'trades': 0, 'profitable': 0, 'total_pnl': 0, 'avg_duration': []})
        
        for signal in executed_signals:
            mode = signal.get('trading_mode', 'balanced')
            pnl = signal['result']['pnl_percent']
            duration = signal['result']['duration_minutes']
            
            mode_stats[mode]['trades'] += 1
            mode_stats[mode]['total_pnl'] += pnl
            mode_stats[mode]['avg_duration'].append(duration)
            if pnl > 0:
                mode_stats[mode]['profitable'] += 1
                
        result = {}
        for mode, stats in mode_stats.items():
            result[mode] = {
                'trades': stats['trades'],
                'success_rate': self._safe_divide(stats['profitable'], stats['trades']),
                'avg_pnl': self._safe_divide(stats['total_pnl'], stats['trades']),
                'total_pnl': stats['total_pnl'],
                'avg_duration': self._calculate_average(stats['avg_duration'])
            }
            
        return result
        
    def _summarize_market_conditions(self, signals: List[dict]) -> dict:
        """Суммирует рыночные условия дня"""
        if not signals:
            return {}
            
        # Собираем все рыночные контексты
        market_phases = []
        volatility_regimes = []
        global_trends = []
        
        for signal in signals:
            context = signal.get('market_context', {})
            if context.get('market_phase'):
                market_phases.append(context['market_phase'])
            if context.get('volatility_regime'):
                volatility_regimes.append(context['volatility_regime'])
            if context.get('global_trend'):
                global_trends.append(context['global_trend'])
                
        # Определяем доминирующие условия
        return {
            'dominant_market_phase': self._get_most_common(market_phases),
            'dominant_volatility': self._get_most_common(volatility_regimes),
            'dominant_trend': self._get_most_common(global_trends),
            'phase_changes': len(set(market_phases)),
            'market_stability': 'stable' if len(set(market_phases)) <= 2 else 'changing'
        }
        
    def _generate_recommendations(self, daily_stats: dict) -> List[str]:
        """Генерирует рекомендации на основе анализа"""
        recommendations = []
        
        # Анализ success rate
        success_rate = daily_stats['summary']['success_rate']
        if success_rate < 0.6:
            recommendations.append(
                f"⚠️ Success rate низкий ({success_rate:.1%}). "
                "Рекомендуется ужесточить фильтры входа или перейти в Conservative режим."
            )
        elif success_rate > 0.8:
            recommendations.append(
                f"✅ Success rate высокий ({success_rate:.1%}). "
                "Можно попробовать увеличить размер позиций или перейти в Aggressive режим."
            )
            
        # Анализ по типам сигналов
        signal_analysis = daily_stats.get('signal_types_analysis', {})
        if 'momentum_burst' in signal_analysis:
            mb_success = signal_analysis['momentum_burst']['success_rate']
            if mb_success < 0.5:
                recommendations.append(
                    f"📉 Momentum burst сигналы показывают низкую эффективность ({mb_success:.1%}). "
                    "Рекомендуется отключить или доработать детектор."
                )
                
        # Анализ по времени
        time_analysis = daily_stats.get('time_analysis', {})
        if time_analysis.get('worst_trading_hours'):
            worst_hours = [h['hour'] for h in time_analysis['worst_trading_hours']]
            recommendations.append(
                f"🕐 Избегайте торговли в часы: {', '.join(worst_hours)}. "
                "Статистика показывает отрицательные результаты."
            )
            
        # Анализ ML точности
        ml_accuracy = daily_stats.get('ml_accuracy', {})
        if ml_accuracy.get('movement_accuracy', {}).get('1_5_percent', 0) < 0.6:
            recommendations.append(
                "🤖 ML модель показывает низкую точность предсказаний. "
                "Требуется переобучение на свежих данных."
            )
            
        # Анализ по символам
        symbol_perf = daily_stats.get('symbol_performance', {})
        if symbol_perf.get('worst_5_performers'):
            worst_symbols = [s['symbol'] for s in symbol_perf['worst_5_performers'][:3]]
            recommendations.append(
                f"🚫 Исключите из торговли: {', '.join(worst_symbols)}. "
                "Постоянно показывают убытки."
            )
            
        # Общие рекомендации по режиму
        market_conditions = daily_stats.get('market_conditions', {})
        if market_conditions.get('dominant_volatility') == 'HIGH':
            recommendations.append(
                "💨 Высокая волатильность на рынке. "
                "Рекомендуется уменьшить размеры позиций и использовать Conservative режим."
            )
            
        return recommendations
        
    # Вспомогательные методы
    def _safe_divide(self, numerator: float, denominator: float) -> float:
        """Безопасное деление"""
        return numerator / denominator if denominator != 0 else 0
        
    def _calculate_average(self, values: List[float]) -> float:
        """Расчет среднего"""
        return statistics.mean(values) if values else 0
        
    def _calculate_profit_factor(self, executed_signals: List[dict]) -> float:
        """Расчет profit factor"""
        gross_profit = sum(s['result']['pnl_percent'] for s in executed_signals if s['result']['pnl_percent'] > 0)
        gross_loss = abs(sum(s['result']['pnl_percent'] for s in executed_signals if s['result']['pnl_percent'] < 0))
        return self._safe_divide(gross_profit, gross_loss)
        
    def _get_most_common(self, items: List[str]) -> str:
        """Находит наиболее частый элемент"""
        if not items:
            return "unknown"
        return max(set(items), key=items.count)
        
    def _append_to_json_log(self, filepath: Path, data: dict):
        """Добавляет запись в JSON лог"""
        try:
            # Читаем существующие данные
            if filepath.exists():
                with open(filepath, 'r', encoding='utf-8') as f:
                    logs = json.load(f)
            else:
                logs = []
                
            # Добавляем новую запись
            logs.append(data)
            
            # Сохраняем обратно
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(logs, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            self.logger.error(f"Error saving to JSON log: {e}")
            
    def _update_signal_result(self, signal_id: str, result: dict):
        """Обновляет результат сигнала"""
        # Обновляем в кеше
        for signal in self.signals_cache:
            if signal['signal_id'] == signal_id:
                signal['execution'] = result
                signal['result'] = result
                break
                
        # Обновляем в файле
        try:
            if self.signals_db.exists():
                with open(self.signals_db, 'r', encoding='utf-8') as f:
                    logs = json.load(f)
                    
                for signal in logs:
                    if signal['signal_id'] == signal_id:
                        signal['execution'] = result
                        signal['result'] = result
                        break
                        
                with open(self.signals_db, 'w', encoding='utf-8') as f:
                    json.dump(logs, f, indent=2, ensure_ascii=False)
                    
        except Exception as e:
            self.logger.error(f"Error updating signal result: {e}")
            
    def _update_performance_stats(self, signal_id: str, result: dict):
        """Обновляет статистику производительности"""
        stats_key = datetime.now().strftime("%Y-%m-%d")
        self.performance_cache[stats_key].append({
            'signal_id': signal_id,
            'pnl_percent': result['pnl_percent'],
            'duration': result['duration_minutes'],
            'success': result['pnl_percent'] > 0
        })
        
    def _save_performance_report(self, report: dict):
        """Сохраняет отчет о производительности"""
        try:
            # Читаем существующие отчеты
            if self.performance_file.exists():
                with open(self.performance_file, 'r', encoding='utf-8') as f:
                    reports = json.load(f)
            else:
                reports = []
                
            # Добавляем новый отчет
            reports.append(report)
            
            # Оставляем только последние 30 дней
            if len(reports) > 30:
                reports = reports[-30:]
                
            # Сохраняем
            with open(self.performance_file, 'w', encoding='utf-8') as f:
                json.dump(reports, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"Daily report saved: {report['date']}")
            
        except Exception as e:
            self.logger.error(f"Error saving performance report: {e}")
            
    async def cleanup_old_logs(self, days_to_keep: int = 30):
        """Очистка старых логов"""
        cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
        
        for log_file in self.logs_dir.glob("*.log"):
            if log_file.stat().st_mtime < cutoff_date:
                log_file.unlink()
                self.logger.info(f"Deleted old log file: {log_file}")
                
    def get_signal_history(self, symbol: str = None, days: int = 7) -> List[dict]:
        """Получает историю сигналов"""
        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        signals = []
        if self.signals_db.exists():
            with open(self.signals_db, 'r', encoding='utf-8') as f:
                all_signals = json.load(f)
                
            for signal in all_signals:
                if signal['timestamp'] >= cutoff_date:
                    if symbol is None or signal['symbol'] == symbol:
                        signals.append(signal)
                        
        return signals
        
    def get_performance_summary(self, days: int = 30) -> dict:
        """Получает сводку производительности"""
        if not self.performance_file.exists():
            return {}
            
        with open(self.performance_file, 'r', encoding='utf-8') as f:
            reports = json.load(f)
            
        # Берем последние N дней
        recent_reports = reports[-days:] if len(reports) > days else reports
        
        if not recent_reports:
            return {}
            
        # Агрегируем статистику
        total_trades = sum(r['summary']['signals_executed'] for r in recent_reports)
        total_pnl = sum(r['summary']['total_pnl_percent'] for r in recent_reports)
        
        return {
            'period_days': len(recent_reports),
            'total_trades': total_trades,
            'total_pnl_percent': total_pnl,
            'avg_daily_pnl': total_pnl / len(recent_reports),
            'avg_daily_trades': total_trades / len(recent_reports),
            'best_day': max(recent_reports, key=lambda x: x['summary']['total_pnl_percent'])['date'],
            'worst_day': min(recent_reports, key=lambda x: x['summary']['total_pnl_percent'])['date'],
            'overall_success_rate': self._calculate_average([r['summary']['success_rate'] for r in recent_reports])
        }