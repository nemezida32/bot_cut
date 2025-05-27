"""
Менеджер баланса - критически важный модуль для расчета размера позиций
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import aiohttp
from decimal import Decimal, ROUND_DOWN


class BalanceManager:
    """Управление балансом и расчет размера позиций в % от депозита"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # API настройки
        self.api_key = config['binance']['api_key']
        self.api_secret = config['binance']['api_secret']
        self.base_url = config['binance']['futures_base_url']
        
        # Кеш баланса
        self.balance_cache = {
            'total_balance': 0.0,
            'available_balance': 0.0,
            'used_balance': 0.0,
            'unrealized_pnl': 0.0,
            'margin_balance': 0.0,
            'positions_count': 0,
            'last_update': None
        }
        
        # Настройки позиций
        self.position_sizes = config['trading']['position_sizes']
        self.max_position_percent = config['system']['max_position_percent']
        
        # HTTP сессия
        self.session = None
        
        # Таймер обновления баланса
        self.update_interval = 5  # секунд
        
    async def initialize(self):
        """Инициализация менеджера баланса"""
        self.session = aiohttp.ClientSession()
        
        # Получаем начальный баланс
        await self.update_balance()
        
        # Запускаем автообновление
        asyncio.create_task(self._balance_update_loop())
        
        self.logger.info(f"Balance manager initialized. Total balance: ${self.balance_cache['total_balance']:.2f}")
        
    async def close(self):
        """Закрытие менеджера"""
        if self.session:
            await self.session.close()
            
    async def _balance_update_loop(self):
        """Цикл обновления баланса"""
        while True:
            try:
                await asyncio.sleep(self.update_interval)
                await self.update_balance()
            except Exception as e:
                self.logger.error(f"Balance update error: {e}")
                await asyncio.sleep(10)
                
    async def update_balance(self) -> Dict[str, float]:
        """Обновление информации о балансе"""
        try:
            # Получаем информацию об аккаунте
            account_info = await self._get_account_info()
            
            if not account_info:
                return self.balance_cache
                
            # Парсим данные
            total_balance = float(account_info['totalWalletBalance'])
            available_balance = float(account_info['availableBalance'])
            unrealized_pnl = float(account_info['totalUnrealizedProfit'])
            margin_balance = float(account_info['totalMarginBalance'])
            
            # Считаем позиции
            positions = [p for p in account_info['positions'] if float(p['positionAmt']) != 0]
            positions_count = len(positions)
            used_balance = sum(float(p['initialMargin']) for p in positions)
            
            # Обновляем кеш
            self.balance_cache = {
                'total_balance': total_balance,
                'available_balance': available_balance,
                'used_balance': used_balance,
                'unrealized_pnl': unrealized_pnl,
                'margin_balance': margin_balance,
                'positions_count': positions_count,
                'last_update': datetime.now()
            }
            
            self.logger.debug(
                f"Balance updated: Total=${total_balance:.2f}, "
                f"Available=${available_balance:.2f}, "
                f"Positions={positions_count}"
            )
            
            return self.balance_cache
            
        except Exception as e:
            self.logger.error(f"Error updating balance: {e}")
            return self.balance_cache
            
    async def calculate_position_size(self, signal_data: dict) -> Dict[str, any]:
        """
        Расчет размера позиции в USDT на основе % от баланса
        
        Args:
            signal_data: Данные сигнала с информацией о силе, типе и т.д.
            
        Returns:
            Dict с параметрами позиции
        """
        # Обновляем баланс перед расчетом
        await self.update_balance()
        
        # Базовые данные
        total_balance = self.balance_cache['total_balance']
        available_balance = self.balance_cache['available_balance']
        
        if total_balance <= 0 or available_balance <= 0:
            self.logger.error("Insufficient balance for trading")
            return {
                'can_trade': False,
                'reason': 'Insufficient balance'
            }
            
        # Определяем базовый процент от баланса
        base_percent = self._get_base_percent(total_balance)
        
        # Получаем модификаторы
        mode = self.config['trading']['mode']
        mode_config = self.config['trading']['modes'][mode]
        mode_multiplier = mode_config['position_size_multiplier']
        
        # Модификатор по типу сигнала
        signal_type = signal_data.get('signal_type', 'classic')
        signal_strength = signal_data.get('signal_strength', 3)
        
        if signal_type == 'momentum_burst' and mode == 'aggressive':
            size_multiplier = mode_config.get('momentum_burst_position_reduce', 0.5)
        elif signal_strength == 5:
            size_multiplier = 2.0  # Удваиваем на 5-звездочных
        else:
            size_multiplier = 1.0
            
        # Расчет финального процента
        final_percent = base_percent * mode_multiplier * size_multiplier
        
        # Проверка лимитов
        final_percent = min(final_percent, self.max_position_percent)
        
        # Расчет размера в USDT
        position_size_usdt = total_balance * final_percent / 100
        
        # Проверка доступного баланса
        if position_size_usdt > available_balance:
            position_size_usdt = available_balance * 0.95  # Оставляем 5% резерв
            final_percent = position_size_usdt / total_balance * 100
            
        # Получаем цену символа
        symbol = signal_data['symbol']
        price = signal_data.get('entry_price', signal_data.get('current_price', 0))
        
        if price <= 0:
            return {
                'can_trade': False,
                'reason': 'Invalid price'
            }
            
        # Расчет плеча
        leverage = self._calculate_leverage(signal_data, mode_config)
        
        # Расчет количества контрактов
        notional_value = position_size_usdt * leverage
        contracts = notional_value / price
        
        # Получаем параметры символа для округления
        symbol_info = await self._get_symbol_info(symbol)
        if symbol_info:
            contracts = self._round_contracts(contracts, symbol_info)
            
        # Финальная проверка
        actual_position_size = contracts * price / leverage
        
        return {
            'can_trade': True,
            'symbol': symbol,
            'side': signal_data.get('direction', 'LONG'),
            'contracts': contracts,
            'position_size_usdt': actual_position_size,
            'position_size_percent': final_percent,
            'leverage': leverage,
            'entry_price': price,
            'notional_value': contracts * price,
            'margin_required': actual_position_size,
            'balance_info': {
                'total_balance': total_balance,
                'available_balance': available_balance,
                'balance_after_trade': available_balance - actual_position_size
            },
            'calculation_details': {
                'base_percent': base_percent,
                'mode_multiplier': mode_multiplier,
                'signal_multiplier': size_multiplier,
                'final_percent': final_percent,
                'signal_strength': signal_strength,
                'signal_type': signal_type
            }
        }
        
    def _get_base_percent(self, balance: float) -> float:
        """Получение базового процента в зависимости от баланса"""
        for size_config in self.position_sizes:
            if balance <= size_config['max_balance']:
                return size_config['base_percent']
                
        # Если баланс больше всех лимитов, используем последний
        return self.position_sizes[-1]['base_percent']
        
    def _calculate_leverage(self, signal_data: dict, mode_config: dict) -> int:
        """Расчет плеча для позиции"""
        base_leverage = self.config['trading']['leverage']
        max_leverage = self.config['trading']['max_leverage']
        
        # Модификатор режима
        leverage_multiplier = mode_config.get('leverage_multiplier', 1.0)
        
        # Модификатор по типу сигнала
        signal_type = signal_data.get('signal_type', 'classic')
        if signal_type == 'momentum_burst':
            leverage_multiplier *= 0.8  # Снижаем риск на momentum burst
            
        # Модификатор по силе сигнала
        signal_strength = signal_data.get('signal_strength', 3)
        if signal_strength == 5:
            leverage_multiplier *= 1.5
        elif signal_strength < 3:
            leverage_multiplier *= 0.7
            
        # Расчет финального плеча
        final_leverage = int(base_leverage * leverage_multiplier)
        final_leverage = max(1, min(final_leverage, max_leverage))
        
        return final_leverage
        
    def _round_contracts(self, contracts: float, symbol_info: dict) -> float:
        """Округление количества контрактов по правилам биржи"""
        # Получаем параметры лота
        lot_size = None
        for filter_info in symbol_info.get('filters', []):
            if filter_info['filterType'] == 'LOT_SIZE':
                lot_size = filter_info
                break
                
        if not lot_size:
            return round(contracts, 3)
            
        # Используем Decimal для точного округления
        step_size = Decimal(str(lot_size['stepSize']))
        contracts_decimal = Decimal(str(contracts))
        
        # Округляем вниз до ближайшего шага
        rounded = (contracts_decimal // step_size) * step_size
        
        return float(rounded)
        
    async def _get_account_info(self) -> Optional[dict]:
        """Получение информации об аккаунте через API"""
        try:
            # Здесь должна быть подпись запроса
            # Для примера показан упрощенный вариант
            url = f"{self.base_url}/fapi/v2/account"
            headers = {'X-MBX-APIKEY': self.api_key}
            
            # В реальном коде нужно добавить timestamp и signature
            params = {
                'timestamp': int(datetime.now().timestamp() * 1000)
            }
            
            async with self.session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    self.logger.error(f"Account API error: {response.status}")
                    
        except Exception as e:
            self.logger.error(f"Error getting account info: {e}")
            
        return None
        
    async def _get_symbol_info(self, symbol: str) -> Optional[dict]:
        """Получение информации о символе"""
        try:
            url = f"{self.base_url}/fapi/v1/exchangeInfo"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    for symbol_info in data['symbols']:
                        if symbol_info['symbol'] == symbol:
                            return symbol_info
                            
        except Exception as e:
            self.logger.error(f"Error getting symbol info: {e}")
            
        return None
        
    def get_current_balance(self) -> dict:
        """Получение текущего баланса из кеша"""
        return self.balance_cache.copy()
        
    def can_open_position(self) -> Tuple[bool, str]:
        """Проверка возможности открыть новую позицию"""
        # Проверка количества позиций
        max_positions = self.config['trading']['max_positions']
        if self.balance_cache['positions_count'] >= max_positions:
            return False, f"Maximum positions limit reached ({max_positions})"
            
        # Проверка доступного баланса
        min_balance = 10  # Минимум $10 для торговли
        if self.balance_cache['available_balance'] < min_balance:
            return False, f"Insufficient available balance (${self.balance_cache['available_balance']:.2f})"
            
        # Проверка дневного лимита убытков
        daily_loss_limit = self.config['system']['daily_loss_limit_percent']
        # Здесь должна быть проверка дневных убытков
        
        return True, "OK"
        
    async def calculate_dca_size(self, position_data: dict, dca_level: int = 1) -> Dict[str, any]:
        """
        Расчет размера DCA (докупки) для существующей позиции
        
        Args:
            position_data: Данные текущей позиции
            dca_level: Уровень DCA (1, 2, и т.д.)
        """
        # Обновляем баланс
        await self.update_balance()
        
        # Получаем настройки DCA
        dca_config = self.config['trading']['dca']
        dca_multiplier = dca_config['multiplier']
        
        # Базовый размер DCA = изначальный размер * множитель
        original_size = position_data['position_size_usdt']
        dca_size = original_size * dca_multiplier
        
        # Проверяем доступный баланс
        available = self.balance_cache['available_balance']
        
        if dca_size > available:
            dca_size = available * 0.95  # Оставляем 5% резерв
            
        # Проверяем общий риск
        total_position_after_dca = original_size + dca_size
        total_percent = total_position_after_dca / self.balance_cache['total_balance'] * 100
        
        if total_percent > self.max_position_percent * 1.5:  # Разрешаем до 150% от макс для DCA
            return {
                'can_dca': False,
                'reason': 'Position size limit exceeded'
            }
            
        return {
            'can_dca': True,
            'dca_size_usdt': dca_size,
            'dca_size_percent': dca_size / self.balance_cache['total_balance'] * 100,
            'total_position_after': total_position_after_dca,
            'total_percent_after': total_percent,
            'available_after': available - dca_size
        }
        
    def log_position_calculation(self, calculation: dict):
        """Логирование расчета позиции для анализа"""
        self.logger.info(
            f"Position calculated for {calculation['symbol']}: "
            f"Size: ${calculation['position_size_usdt']:.2f} ({calculation['position_size_percent']:.2f}%), "
            f"Leverage: {calculation['leverage']}x, "
            f"Contracts: {calculation['contracts']}"
        )
        
        self.logger.debug(
            f"Calculation details: {calculation['calculation_details']}"
        )