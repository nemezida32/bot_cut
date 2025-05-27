"""
Главный файл для запуска торгового бота v2.0
"""

import asyncio
import logging
import sys
import os
import signal
import yaml
from datetime import datetime
from pathlib import Path

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.core.scanner import ParallelScanner
from src.core.balance_manager import BalanceManager
from src.monitoring.analytics_logger import AnalyticsLogger
from src.monitoring.telegram_interface import TelegramInterface
from src.trading.position_manager import PositionManager
from src.trading.execution_engine import ExecutionEngine


class TradingBot:
    """Основной класс торгового бота"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        
        # Настройка логирования
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Компоненты системы
        self.analytics_logger = None
        self.balance_manager = None
        self.scanner = None
        self.telegram = None
        self.position_manager = None
        self.execution_engine = None
        
        # Флаг работы
        self.is_running = False
        
    def _load_config(self) -> dict:
        """Загрузка конфигурации"""
        config = {}
        
        # Загружаем основную конфигурацию
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config.update(yaml.safe_load(f))
            
        # Загружаем дополнительные конфигурации
        config_dir = Path(self.config_path).parent
        
        # Торговая конфигурация
        trading_config_path = config_dir / "trading_config.yaml"
        if trading_config_path.exists():
            with open(trading_config_path, 'r', encoding='utf-8') as f:
                config['trading'].update(yaml.safe_load(f))
                
        # Конфигурация индикаторов
        indicators_config_path = config_dir / "indicators_config.yaml"
        if indicators_config_path.exists():
            with open(indicators_config_path, 'r', encoding='utf-8') as f:
                config['indicators'] = yaml.safe_load(f)
                
        return config
        
    def _setup_logging(self):
        """Настройка системы логирования"""
        log_level = self.config['system']['log_level']
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # Создаем директорию для логов
        Path("logs").mkdir(exist_ok=True)
        
        # Настройка корневого логгера
        logging.basicConfig(
            level=getattr(logging, log_level),
            format=log_format,
            handlers=[
                logging.FileHandler(f"logs/trading_bot_{datetime.now().strftime('%Y%m%d')}.log"),
                logging.StreamHandler()
            ]
        )
        
    async def initialize(self):
        """Инициализация всех компонентов"""
        self.logger.info("=" * 50)
        self.logger.info("Trading Bot v2.0 Starting...")
        self.logger.info(f"Mode: {self.config['trading']['mode']}")
        self.logger.info("=" * 50)
        
        try:
            # Инициализируем компоненты в правильном порядке
            
            # 1. Analytics Logger
            self.logger.info("Initializing Analytics Logger...")
            self.analytics_logger = AnalyticsLogger(self.config)
            
            # 2. Balance Manager - критически важный компонент
            self.logger.info("Initializing Balance Manager...")
            self.balance_manager = BalanceManager(self.config)
            await self.balance_manager.initialize()
            
            # Проверяем баланс
            balance_info = self.balance_manager.get_current_balance()
            self.logger.info(f"Current balance: ${balance_info['total_balance']:.2f}")
            
            # Проверяем возможность торговли
            can_trade, reason = self.balance_manager.can_open_position()
            if not can_trade:
                self.logger.error(f"Cannot start trading: {reason}")
                return False
                
            # 3. Scanner
            self.logger.info("Initializing Scanner...")
            self.scanner = ParallelScanner(self.config, self.analytics_logger)
            
            # 4. Position Manager
            self.logger.info("Initializing Position Manager...")
            self.position_manager = PositionManager(self.config, self.balance_manager)
            await self.position_manager.initialize()
            
            # 5. Execution Engine
            self.logger.info("Initializing Execution Engine...")
            self.execution_engine = ExecutionEngine(
                self.config, 
                self.balance_manager,
                self.position_manager,
                self.analytics_logger
            )
            await self.execution_engine.initialize()
            
            # 6. Telegram Interface
            self.logger.info("Initializing Telegram Interface...")
            self.telegram = TelegramInterface(
                self.config,
                self.scanner,
                self.position_manager,
                self.balance_manager,
                self.analytics_logger
            )
            await self.telegram.initialize()
            
            self.logger.info("All components initialized successfully!")
            
            # Отправляем стартовое сообщение
            await self.telegram.send_startup_message()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Initialization error: {e}", exc_info=True)
            return False
            
    async def start(self):
        """Запуск бота"""
        if not await self.initialize():
            self.logger.error("Failed to initialize bot")
            return
            
        self.is_running = True
        
        try:
            # Запускаем основные задачи
            tasks = [
                # Сканер - основной компонент
                asyncio.create_task(self.scanner.start()),
                
                # Обработчик сигналов
                asyncio.create_task(self._signal_processor()),
                
                # Мониторинг позиций
                asyncio.create_task(self.position_manager.monitor_positions()),
                
                # Telegram обработчик
                asyncio.create_task(self.telegram.start()),
                
                # Генератор отчетов
                asyncio.create_task(self._report_generator()),
                
                # Мониторинг производительности
                asyncio.create_task(self._performance_monitor())
            ]
            
            self.logger.info("Bot started successfully!")
            
            # Ждем завершения
            await asyncio.gather(*tasks)
            
        except Exception as e:
            self.logger.error(f"Bot error: {e}", exc_info=True)
            
        finally:
            await self.stop()
            
    async def stop(self):
        """Остановка бота"""
        self.logger.info("Stopping bot...")
        self.is_running = False
        
        # Останавливаем компоненты в обратном порядке
        if self.telegram:
            await self.telegram.send_shutdown_message()
            await self.telegram.stop()
            
        if self.execution_engine:
            await self.execution_engine.close()
            
        if self.position_manager:
            await self.position_manager.close()
            
        if self.scanner:
            await self.scanner.stop()
            
        if self.balance_manager:
            await self.balance_manager.close()
            
        self.logger.info("Bot stopped")
        
    async def _signal_processor(self):
        """Обработка сигналов из фазы 4"""
        while self.is_running:
            try:
                # Проверяем новые сигналы из сканера
                phase4_results = self.scanner.get_phase_results(4)
                
                if phase4_results.get('recent_signals'):
                    for signal_info in phase4_results['recent_signals']:
                        # Сигнал уже содержит расчет позиции из balance_manager
                        signal = signal_info.get('signal')
                        
                        if signal and signal.get('can_trade'):
                            # Проверяем, можем ли открыть позицию
                            can_open, reason = self.balance_manager.can_open_position()
                            
                            if can_open:
                                # Исполняем сигнал
                                result = await self.execution_engine.execute_signal(signal)
                                
                                if result['success']:
                                    # Логируем результат
                                    self.analytics_logger.log_execution_result(
                                        signal_info['signal_id'],
                                        result
                                    )
                                    
                                    # Уведомляем в Telegram
                                    await self.telegram.send_position_opened(result)
                                else:
                                    self.logger.error(
                                        f"Failed to execute signal: {result.get('error')}"
                                    )
                            else:
                                self.logger.warning(f"Cannot open position: {reason}")
                                
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Signal processor error: {e}")
                await asyncio.sleep(5)
                
    async def _report_generator(self):
        """Генератор ежедневных отчетов"""
        while self.is_running:
            try:
                # Ждем до времени генерации отчета (например, 00:00 UTC)
                now = datetime.now()
                next_report = now.replace(hour=0, minute=0, second=0, microsecond=0)
                if next_report <= now:
                    next_report = next_report.replace(day=now.day + 1)
                    
                wait_seconds = (next_report - now).total_seconds()
                await asyncio.sleep(wait_seconds)
                
                # Генерируем отчет
                daily_report = self.analytics_logger.generate_daily_report()
                
                # Отправляем в Telegram
                await self.telegram.send_daily_report(daily_report)
                
                self.logger.info("Daily report generated and sent")
                
            except Exception as e:
                self.logger.error(f"Report generator error: {e}")
                await asyncio.sleep(3600)  # Ждем час при ошибке
                
    async def _performance_monitor(self):
        """Мониторинг производительности системы"""
        while self.is_running:
            try:
                await asyncio.sleep(300)  # Каждые 5 минут
                
                # Собираем метрики
                metrics = {
                    'timestamp': datetime.now().isoformat(),
                    'balance': self.balance_manager.get_current_balance(),
                    'scanner': self.scanner.get_all_phases_summary(),
                    'positions': self.position_manager.get_positions_summary(),
                    'data_manager_stats': self.scanner.data_manager.get_stats()
                }
                
                # Логируем
                self.logger.info(f"Performance metrics: {metrics}")
                
                # Проверяем критические параметры
                if metrics['balance']['total_balance'] < 10:
                    self.logger.critical("Balance below minimum threshold!")
                    await self.telegram.send_alert("⚠️ CRITICAL: Balance below $10!")
                    
            except Exception as e:
                self.logger.error(f"Performance monitor error: {e}")
                
    def handle_shutdown(self, signum, frame):
        """Обработка сигнала завершения"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(self.stop())


async def main():
    """Главная функция"""
    # Создаем экземпляр бота
    bot = TradingBot()
    
    # Устанавливаем обработчики сигналов
    signal.signal(signal.SIGINT, bot.handle_shutdown)
    signal.signal(signal.SIGTERM, bot.handle_shutdown)
    
    try:
        # Запускаем бота
        await bot.start()
    except KeyboardInterrupt:
        bot.logger.info("Keyboard interrupt received")
    except Exception as e:
        bot.logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        await bot.stop()


if __name__ == "__main__":
    # Проверяем наличие конфигурации
    if not Path("config/config.yaml").exists():
        print("Error: config/config.yaml not found!")
        print("Please copy config/config.example.yaml to config/config.yaml and edit it.")
        sys.exit(1)
        
    # Запускаем
    asyncio.run(main())