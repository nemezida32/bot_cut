"""
WebSocket менеджер для real-time данных с Binance
"""

import asyncio
import websockets
import json
import logging
from typing import Dict, List, Callable, Optional, Set
from datetime import datetime
import aiohttp
from collections import defaultdict


class WebSocketManager:
    """Управление WebSocket подключениями к Binance"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # WebSocket URL
        self.ws_base_url = config['binance']['websocket_base_url']
        
        # Активные подключения
        self.connections = {}
        
        # Подписки
        self.subscriptions = defaultdict(set)  # symbol -> set of callbacks
        self.stream_subscriptions = defaultdict(set)  # stream -> set of callbacks
        
        # Буферы данных
        self.price_buffer = {}
        self.kline_buffer = defaultdict(dict)
        self.depth_buffer = {}
        self.trade_buffer = defaultdict(list)
        
        # Статус
        self.is_running = False
        self.reconnect_delay = 5
        self.max_reconnect_delay = 60
        
        # Пинг для поддержания соединения
        self.ping_interval = self.config['system']['websocket_ping_interval']
        
    async def start(self):
        """Запуск WebSocket менеджера"""
        self.is_running = True
        self.logger.info("Starting WebSocket manager...")
        
        # Запускаем основной цикл обработки
        asyncio.create_task(self._connection_manager())
        
    async def stop(self):
        """Остановка WebSocket менеджера"""
        self.logger.info("Stopping WebSocket manager...")
        self.is_running = False
        
        # Закрываем все соединения
        for stream, ws in self.connections.items():
            try:
                await ws.close()
            except Exception as e:
                self.logger.error(f"Error closing WebSocket {stream}: {e}")
                
        self.connections.clear()
        
    async def subscribe_ticker(self, symbols: List[str], callback: Callable):
        """
        Подписка на обновления цен
        
        Args:
            symbols: Список символов
            callback: Функция обратного вызова (symbol, price, timestamp)
        """
        for symbol in symbols:
            stream = f"{symbol.lower()}@ticker"
            self.stream_subscriptions[stream].add(('ticker', callback))
            
        # Обновляем подписки
        await self._update_subscriptions()
        
    async def subscribe_klines(self, symbol: str, interval: str, callback: Callable):
        """
        Подписка на обновления свечей
        
        Args:
            symbol: Символ
            interval: Интервал (1m, 3m, 5m, 15m, etc)
            callback: Функция обратного вызова
        """
        stream = f"{symbol.lower()}@kline_{interval}"
        self.stream_subscriptions[stream].add(('kline', callback))
        
        await self._update_subscriptions()
        
    async def subscribe_depth(self, symbols: List[str], callback: Callable):
        """
        Подписка на обновления стакана
        
        Args:
            symbols: Список символов
            callback: Функция обратного вызова
        """
        for symbol in symbols:
            stream = f"{symbol.lower()}@depth20@100ms"
            self.stream_subscriptions[stream].add(('depth', callback))
            
        await self._update_subscriptions()
        
    async def subscribe_trades(self, symbols: List[str], callback: Callable):
        """
        Подписка на поток сделок
        
        Args:
            symbols: Список символов
            callback: Функция обратного вызова
        """
        for symbol in symbols:
            stream = f"{symbol.lower()}@aggTrade"
            self.stream_subscriptions[stream].add(('trade', callback))
            
        await self._update_subscriptions()
        
    async def unsubscribe(self, stream: str, callback: Callable):
        """Отписка от потока"""
        if stream in self.stream_subscriptions:
            self.stream_subscriptions[stream].discard(callback)
            
            # Если больше нет подписчиков, удаляем поток
            if not self.stream_subscriptions[stream]:
                del self.stream_subscriptions[stream]
                await self._update_subscriptions()
                
    async def _connection_manager(self):
        """Основной цикл управления соединениями"""
        reconnect_delay = self.reconnect_delay
        
        while self.is_running:
            try:
                # Получаем список необходимых потоков
                required_streams = list(self.stream_subscriptions.keys())
                
                if not required_streams:
                    await asyncio.sleep(1)
                    continue
                    
                # Создаем URL для combined stream
                streams = "/".join(required_streams)
                url = f"{self.ws_base_url}/stream?streams={streams}"
                
                self.logger.info(f"Connecting to WebSocket with {len(required_streams)} streams")
                
                async with websockets.connect(url) as websocket:
                    self.connections['main'] = websocket
                    reconnect_delay = self.reconnect_delay  # Сброс задержки при успешном подключении
                    
                    # Запускаем обработчики
                    tasks = [
                        asyncio.create_task(self._handle_messages(websocket)),
                        asyncio.create_task(self._ping_loop(websocket))
                    ]
                    
                    # Ждем завершения любой задачи
                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    
                    # Отменяем оставшиеся задачи
                    for task in pending:
                        task.cancel()
                        
            except Exception as e:
                self.logger.error(f"WebSocket connection error: {e}")
                
                # Экспоненциальная задержка переподключения
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, self.max_reconnect_delay)
                
    async def _handle_messages(self, websocket):
        """Обработка входящих сообщений"""
        async for message in websocket:
            try:
                data = json.loads(message)
                
                # Combined stream формат
                if 'stream' in data and 'data' in data:
                    stream = data['stream']
                    stream_data = data['data']
                    
                    # Определяем тип данных и обрабатываем
                    if '@ticker' in stream:
                        await self._process_ticker(stream, stream_data)
                    elif '@kline' in stream:
                        await self._process_kline(stream, stream_data)
                    elif '@depth' in stream:
                        await self._process_depth(stream, stream_data)
                    elif '@aggTrade' in stream:
                        await self._process_trade(stream, stream_data)
                        
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error: {e}")
            except Exception as e:
                self.logger.error(f"Message processing error: {e}")
                
    async def _process_ticker(self, stream: str, data: dict):
        """Обработка данных тикера"""
        try:
            symbol = data['s']
            price = float(data['c'])  # Current price
            timestamp = data['E']  # Event time
            
            # Обновляем буфер
            self.price_buffer[symbol] = {
                'price': price,
                'timestamp': timestamp,
                'volume': float(data['v']),  # Volume
                'quote_volume': float(data['q']),  # Quote volume
                'price_change': float(data['p']),  # Price change
                'price_change_percent': float(data['P'])  # Price change percent
            }
            
            # Вызываем callbacks
            if stream in self.stream_subscriptions:
                for sub_type, callback in self.stream_subscriptions[stream]:
                    if sub_type == 'ticker':
                        try:
                            await callback(symbol, self.price_buffer[symbol])
                        except Exception as e:
                            self.logger.error(f"Ticker callback error: {e}")
                            
        except Exception as e:
            self.logger.error(f"Ticker processing error: {e}")
            
    async def _process_kline(self, stream: str, data: dict):
        """Обработка данных свечей"""
        try:
            kline = data['k']
            symbol = kline['s']
            interval = kline['i']
            
            kline_data = {
                'timestamp': kline['t'],  # Kline start time
                'open': float(kline['o']),
                'high': float(kline['h']),
                'low': float(kline['l']),
                'close': float(kline['c']),
                'volume': float(kline['v']),
                'close_time': kline['T'],
                'quote_volume': float(kline['q']),
                'trades': kline['n'],  # Number of trades
                'is_closed': kline['x']  # Is this kline closed?
            }
            
            # Обновляем буфер
            if symbol not in self.kline_buffer:
                self.kline_buffer[symbol] = {}
            self.kline_buffer[symbol][interval] = kline_data
            
            # Вызываем callbacks только для закрытых свечей или всех обновлений
            if stream in self.stream_subscriptions:
                for sub_type, callback in self.stream_subscriptions[stream]:
                    if sub_type == 'kline':
                        try:
                            await callback(symbol, interval, kline_data)
                        except Exception as e:
                            self.logger.error(f"Kline callback error: {e}")
                            
        except Exception as e:
            self.logger.error(f"Kline processing error: {e}")
            
    async def _process_depth(self, stream: str, data: dict):
        """Обработка данных стакана"""
        try:
            symbol = stream.split('@')[0].upper()
            
            depth_data = {
                'timestamp': datetime.now().timestamp() * 1000,
                'bids': data['bids'],  # [price, quantity]
                'asks': data['asks'],
                'last_update_id': data['lastUpdateId']
            }
            
            # Обновляем буфер
            self.depth_buffer[symbol] = depth_data
            
            # Анализ дисбаланса
            bid_volume = sum(float(bid[1]) for bid in data['bids'][:5])
            ask_volume = sum(float(ask[1]) for ask in data['asks'][:5])
            
            depth_data['bid_volume'] = bid_volume
            depth_data['ask_volume'] = ask_volume
            depth_data['imbalance'] = (bid_volume - ask_volume) / (bid_volume + ask_volume) if (bid_volume + ask_volume) > 0 else 0
            
            # Вызываем callbacks
            if stream in self.stream_subscriptions:
                for sub_type, callback in self.stream_subscriptions[stream]:
                    if sub_type == 'depth':
                        try:
                            await callback(symbol, depth_data)
                        except Exception as e:
                            self.logger.error(f"Depth callback error: {e}")
                            
        except Exception as e:
            self.logger.error(f"Depth processing error: {e}")
            
    async def _process_trade(self, stream: str, data: dict):
        """Обработка данных сделок"""
        try:
            symbol = data['s']
            
            trade_data = {
                'timestamp': data['T'],  # Trade time
                'price': float(data['p']),
                'quantity': float(data['q']),
                'is_buyer_maker': data['m'],  # Was the buyer the maker?
                'trade_id': data['a']  # Aggregate trade ID
            }
            
            # Добавляем в буфер (храним последние 100 сделок)
            if symbol not in self.trade_buffer:
                self.trade_buffer[symbol] = []
                
            self.trade_buffer[symbol].append(trade_data)
            if len(self.trade_buffer[symbol]) > 100:
                self.trade_buffer[symbol] = self.trade_buffer[symbol][-100:]
                
            # Вызываем callbacks
            if stream in self.stream_subscriptions:
                for sub_type, callback in self.stream_subscriptions[stream]:
                    if sub_type == 'trade':
                        try:
                            await callback(symbol, trade_data)
                        except Exception as e:
                            self.logger.error(f"Trade callback error: {e}")
                            
        except Exception as e:
            self.logger.error(f"Trade processing error: {e}")
            
    async def _ping_loop(self, websocket):
        """Поддержание соединения через ping"""
        while self.is_running:
            try:
                await asyncio.sleep(self.ping_interval)
                await websocket.ping()
                self.logger.debug("WebSocket ping sent")
            except Exception as e:
                self.logger.error(f"Ping error: {e}")
                break
                
    async def _update_subscriptions(self):
        """Обновление подписок (переподключение при изменении)"""
        # В текущей реализации требуется переподключение
        # В продакшене можно использовать SUBSCRIBE/UNSUBSCRIBE команды
        pass
        
    def get_latest_price(self, symbol: str) -> Optional[Dict]:
        """Получение последней цены из буфера"""
        return self.price_buffer.get(symbol)
        
    def get_latest_kline(self, symbol: str, interval: str) -> Optional[Dict]:
        """Получение последней свечи из буфера"""
        if symbol in self.kline_buffer:
            return self.kline_buffer[symbol].get(interval)
        return None
        
    def get_orderbook_snapshot(self, symbol: str) -> Optional[Dict]:
        """Получение снимка стакана"""
        return self.depth_buffer.get(symbol)
        
    def get_recent_trades(self, symbol: str, limit: int = 50) -> List[Dict]:
        """Получение последних сделок"""
        if symbol in self.trade_buffer:
            return self.trade_buffer[symbol][-limit:]
        return []
        
    async def wait_for_price_update(self, symbol: str, timeout: float = 5.0) -> Optional[float]:
        """Ожидание обновления цены с таймаутом"""
        start_time = asyncio.get_event_loop().time()
        initial_data = self.price_buffer.get(symbol)
        initial_timestamp = initial_data['timestamp'] if initial_data else 0
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            current_data = self.price_buffer.get(symbol)
            if current_data and current_data['timestamp'] > initial_timestamp:
                return current_data['price']
            await asyncio.sleep(0.1)
            
        return None
        
    def calculate_vwap(self, symbol: str, period_minutes: int = 5) -> Optional[float]:
        """Расчет VWAP на основе последних сделок"""
        if symbol not in self.trade_buffer or not self.trade_buffer[symbol]:
            return None
            
        trades = self.trade_buffer[symbol]
        current_time = datetime.now().timestamp() * 1000
        cutoff_time = current_time - (period_minutes * 60 * 1000)
        
        # Фильтруем сделки за период
        recent_trades = [t for t in trades if t['timestamp'] > cutoff_time]
        
        if not recent_trades:
            return None
            
        # Расчет VWAP
        total_volume = sum(t['quantity'] for t in recent_trades)
        if total_volume == 0:
            return None
            
        vwap = sum(t['price'] * t['quantity'] for t in recent_trades) / total_volume
        return vwap
        
    def analyze_order_flow(self, symbol: str) -> Dict:
        """Анализ потока ордеров"""
        result = {
            'buy_volume': 0,
            'sell_volume': 0,
            'buy_count': 0,
            'sell_count': 0,
            'large_buy_volume': 0,
            'large_sell_volume': 0,
            'imbalance': 0
        }
        
        if symbol not in self.trade_buffer:
            return result
            
        trades = self.trade_buffer[symbol]
        if not trades:
            return result
            
        # Определяем средний размер сделки
        avg_trade_size = sum(t['quantity'] for t in trades) / len(trades)
        large_threshold = avg_trade_size * 3  # Крупная сделка = 3x от среднего
        
        for trade in trades:
            if trade['is_buyer_maker']:
                # Продажа в бид (market sell)
                result['sell_volume'] += trade['quantity']
                result['sell_count'] += 1
                if trade['quantity'] > large_threshold:
                    result['large_sell_volume'] += trade['quantity']
            else:
                # Покупка в аск (market buy)
                result['buy_volume'] += trade['quantity']
                result['buy_count'] += 1
                if trade['quantity'] > large_threshold:
                    result['large_buy_volume'] += trade['quantity']
                    
        # Расчет дисбаланса
        total_volume = result['buy_volume'] + result['sell_volume']
        if total_volume > 0:
            result['imbalance'] = (result['buy_volume'] - result['sell_volume']) / total_volume
            
        return result
        
    async def detect_momentum_spike(self, symbol: str, callback: Callable):
        """
        Детектор momentum spike в реальном времени
        
        Args:
            symbol: Символ для мониторинга
            callback: Функция вызывается при обнаружении spike
        """
        # Подписываемся на необходимые потоки
        await self.subscribe_ticker([symbol], self._momentum_ticker_handler)
        await self.subscribe_trades([symbol], self._momentum_trade_handler)
        
        # Сохраняем callback для symbol
        if not hasattr(self, 'momentum_callbacks'):
            self.momentum_callbacks = {}
        self.momentum_callbacks[symbol] = callback
        
        # Инициализация данных для детекции
        if not hasattr(self, 'momentum_data'):
            self.momentum_data = {}
            
        self.momentum_data[symbol] = {
            'baseline_price': None,
            'baseline_volume': 0,
            'spike_detected': False,
            'detection_time': None,
            'volume_accumulator': 0,
            'price_at_detection': None,
            'trades_count': 0
        }
        
    async def _momentum_ticker_handler(self, symbol: str, ticker_data: dict):
        """Обработчик тикера для momentum детектора"""
        if symbol not in self.momentum_data:
            return
            
        data = self.momentum_data[symbol]
        current_price = ticker_data['price']
        
        # Инициализация baseline
        if data['baseline_price'] is None:
            data['baseline_price'] = current_price
            return
            
        # Проверка изменения цены
        price_change = abs((current_price - data['baseline_price']) / data['baseline_price'] * 100)
        
        # Условия для momentum spike
        if (not data['spike_detected'] and 
            price_change < 0.2 and  # Цена изменилась менее чем на 0.2%
            data['volume_accumulator'] > data['baseline_volume'] * 2):  # Объем вырос в 2+ раза
            
            data['spike_detected'] = True
            data['detection_time'] = datetime.now()
            data['price_at_detection'] = current_price
            
            # Вызываем callback
            if symbol in self.momentum_callbacks:
                spike_info = {
                    'symbol': symbol,
                    'type': 'momentum_spike',
                    'price': current_price,
                    'price_change': price_change,
                    'volume_ratio': data['volume_accumulator'] / data['baseline_volume'] if data['baseline_volume'] > 0 else 0,
                    'timestamp': datetime.now().isoformat()
                }
                await self.momentum_callbacks[symbol](spike_info)
                
    async def _momentum_trade_handler(self, symbol: str, trade_data: dict):
        """Обработчик сделок для momentum детектора"""
        if symbol not in self.momentum_data:
            return
            
        data = self.momentum_data[symbol]
        
        # Накапливаем объем
        data['volume_accumulator'] += trade_data['quantity']
        data['trades_count'] += 1
        
        # Обновляем baseline каждые 100 сделок
        if data['trades_count'] % 100 == 0:
            data['baseline_volume'] = data['volume_accumulator'] / data['trades_count'] * 100
            data['volume_accumulator'] = 0
            data['trades_count'] = 0