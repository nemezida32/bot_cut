"""
Менеджер данных для кеширования и оптимизации запросов к API
"""

import asyncio
import aiohttp
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging
import json
import pickle
from collections import defaultdict
import redis.asyncio as redis
import numpy as np
from cachetools import TTLCache
import pandas as pd


class DataManager:
    """Управление данными, кешированием и оптимизацией запросов"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Binance API endpoints
        self.base_url = config['binance']['futures_base_url']
        self.ws_url = config['binance']['websocket_base_url']
        
        # Кеш в памяти для быстрого доступа
        cache_ttl = config['system']['cache_ttl']
        self.price_cache = TTLCache(maxsize=1000, ttl=5)  # 5 секунд для цен
        self.klines_cache = TTLCache(maxsize=5000, ttl=cache_ttl)
        self.ticker_cache = TTLCache(maxsize=1000, ttl=10)
        self.orderbook_cache = TTLCache(maxsize=500, ttl=2)
        
        # Redis для долгосрочного кеша
        self.redis_client = None
        self.redis_enabled = config.get('database', {}).get('redis', {}).get('enabled', True)
        
        # HTTP сессия
        self.session = None
        
        # Rate limiting
        self.rate_limiter = RateLimiter(
            max_requests=config['system']['api_rate_limit'],
            time_window=60  # 1 минута
        )
        
        # Статистика
        self.stats = {
            'api_calls': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'errors': 0
        }
        
    async def initialize(self):
        """Инициализация менеджера данных"""
        # Создаем HTTP сессию
        connector = aiohttp.TCPConnector(limit=100)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        )
        
        # Подключаемся к Redis
        if self.redis_enabled:
            try:
                redis_config = self.config['database']['redis']
                self.redis_client = await redis.create_redis_pool(
                    host=redis_config['host'],
                    port=redis_config['port'],
                    db=redis_config['db'],
                    password=redis_config.get('password'),
                    encoding='utf-8'
                )
                self.logger.info("Connected to Redis")
            except Exception as e:
                self.logger.warning(f"Redis connection failed: {e}. Using memory cache only.")
                self.redis_enabled = False
                
    async def close(self):
        """Закрытие соединений"""
        if self.session:
            await self.session.close()
            
        if self.redis_client:
            self.redis_client.close()
            await self.redis_client.wait_closed()
            
    async def get_symbol_price(self, symbol: str) -> Optional[float]:
        """Получение текущей цены символа"""
        cache_key = f"price:{symbol}"
        
        # Проверяем кеш
        if cache_key in self.price_cache:
            self.stats['cache_hits'] += 1
            return self.price_cache[cache_key]
            
        # Запрашиваем из API
        try:
            url = f"{self.base_url}/fapi/v1/ticker/price"
            params = {'symbol': symbol}
            
            data = await self._make_request(url, params)
            if data:
                price = float(data['price'])
                self.price_cache[cache_key] = price
                return price
                
        except Exception as e:
            self.logger.error(f"Error getting price for {symbol}: {e}")
            
        return None
        
    async def get_symbol_prices_batch(self, symbols: List[str]) -> Dict[str, float]:
        """Получение цен для нескольких символов"""
        prices = {}
        uncached_symbols = []
        
        # Проверяем кеш
        for symbol in symbols:
            cache_key = f"price:{symbol}"
            if cache_key in self.price_cache:
                prices[symbol] = self.price_cache[cache_key]
                self.stats['cache_hits'] += 1
            else:
                uncached_symbols.append(symbol)
                
        # Запрашиваем недостающие
        if uncached_symbols:
            try:
                url = f"{self.base_url}/fapi/v1/ticker/price"
                data = await self._make_request(url)
                
                if data:
                    for item in data:
                        if item['symbol'] in uncached_symbols:
                            price = float(item['price'])
                            prices[item['symbol']] = price
                            self.price_cache[f"price:{item['symbol']}"] = price
                            
            except Exception as e:
                self.logger.error(f"Error getting batch prices: {e}")
                
        return prices
        
    async def get_klines(self, symbol: str, interval: str, limit: int = 100) -> List[List]:
        """Получение свечей"""
        cache_key = f"klines:{symbol}:{interval}:{limit}"
        
        # Проверяем память
        if cache_key in self.klines_cache:
            self.stats['cache_hits'] += 1
            return self.klines_cache[cache_key]
            
        # Проверяем Redis
        if self.redis_enabled:
            cached = await self._get_from_redis(cache_key)
            if cached:
                self.stats['cache_hits'] += 1
                self.klines_cache[cache_key] = cached
                return cached
                
        # Запрашиваем из API
        try:
            url = f"{self.base_url}/fapi/v1/klines"
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': limit
            }
            
            data = await self._make_request(url, params)
            if data:
                # Сохраняем в кеш
                self.klines_cache[cache_key] = data
                await self._save_to_redis(cache_key, data, ttl=300)  # 5 минут
                return data
                
        except Exception as e:
            self.logger.error(f"Error getting klines for {symbol}: {e}")
            
        return []
        
    async def get_ticker_24h(self, symbol: str) -> Optional[dict]:
        """Получение 24h статистики"""
        cache_key = f"ticker24h:{symbol}"
        
        if cache_key in self.ticker_cache:
            self.stats['cache_hits'] += 1
            return self.ticker_cache[cache_key]
            
        try:
            url = f"{self.base_url}/fapi/v1/ticker/24hr"
            params = {'symbol': symbol}
            
            data = await self._make_request(url, params)
            if data:
                self.ticker_cache[cache_key] = data
                return data
                
        except Exception as e:
            self.logger.error(f"Error getting 24h ticker for {symbol}: {e}")
            
        return None
        
    async def get_tickers_24h_batch(self, symbols: List[str] = None) -> List[dict]:
        """Получение 24h статистики для всех или выбранных символов"""
        try:
            url = f"{self.base_url}/fapi/v1/ticker/24hr"
            data = await self._make_request(url)
            
            if data:
                # Фильтруем если нужно
                if symbols:
                    data = [t for t in data if t['symbol'] in symbols]
                    
                # Кешируем каждый тикер
                for ticker in data:
                    cache_key = f"ticker24h:{ticker['symbol']}"
                    self.ticker_cache[cache_key] = ticker
                    
                return data
                
        except Exception as e:
            self.logger.error(f"Error getting batch 24h tickers: {e}")
            
        return []
        
    async def get_orderbook(self, symbol: str, limit: int = 20) -> Optional[dict]:
        """Получение стакана заявок"""
        cache_key = f"orderbook:{symbol}:{limit}"
        
        if cache_key in self.orderbook_cache:
            self.stats['cache_hits'] += 1
            return self.orderbook_cache[cache_key]
            
        try:
            url = f"{self.base_url}/fapi/v1/depth"
            params = {
                'symbol': symbol,
                'limit': limit
            }
            
            data = await self._make_request(url, params)
            if data:
                self.orderbook_cache[cache_key] = data
                return data
                
        except Exception as e:
            self.logger.error(f"Error getting orderbook for {symbol}: {e}")
            
        return None
        
    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        """Получение funding rate"""
        try:
            url = f"{self.base_url}/fapi/v1/fundingRate"
            params = {
                'symbol': symbol,
                'limit': 1
            }
            
            data = await self._make_request(url, params)
            if data and len(data) > 0:
                return float(data[0]['fundingRate'])
                
        except Exception as e:
            self.logger.error(f"Error getting funding rate for {symbol}: {e}")
            
        return None
        
    async def get_open_interest(self, symbol: str) -> Optional[float]:
        """Получение открытого интереса"""
        try:
            url = f"{self.base_url}/fapi/v1/openInterest"
            params = {'symbol': symbol}
            
            data = await self._make_request(url, params)
            if data:
                return float(data['openInterest'])
                
        except Exception as e:
            self.logger.error(f"Error getting open interest for {symbol}: {e}")
            
        return None
        
    async def calculate_indicators_batch(self, symbols: List[str], timeframe: str) -> Dict[str, dict]:
        """Расчет базовых индикаторов для нескольких символов"""
        results = {}
        
        # Получаем данные параллельно
        tasks = []
        for symbol in symbols:
            task = asyncio.create_task(self._calculate_symbol_indicators(symbol, timeframe))
            tasks.append((symbol, task))
            
        # Ждем результаты
        for symbol, task in tasks:
            try:
                indicators = await task
                if indicators:
                    results[symbol] = indicators
            except Exception as e:
                self.logger.error(f"Error calculating indicators for {symbol}: {e}")
                
        return results
        
    async def _calculate_symbol_indicators(self, symbol: str, timeframe: str) -> Optional[dict]:
        """Расчет индикаторов для одного символа"""
        # Получаем свечи
        klines = await self.get_klines(symbol, timeframe, limit=100)
        if not klines or len(klines) < 20:
            return None
            
        # Преобразуем в numpy arrays
        closes = np.array([float(k[4]) for k in klines])
        highs = np.array([float(k[2]) for k in klines])
        lows = np.array([float(k[3]) for k in klines])
        volumes = np.array([float(k[5]) for k in klines])
        
        # Базовые расчеты
        indicators = {
            'current_price': closes[-1],
            'price_change_percent': ((closes[-1] - closes[-2]) / closes[-2] * 100) if len(closes) > 1 else 0,
            'volume_ratio': volumes[-1] / np.mean(volumes[-20:]) if len(volumes) >= 20 else 1,
            'volatility': np.std(closes[-20:]) / np.mean(closes[-20:]) * 100 if len(closes) >= 20 else 0,
            'high_low_ratio': (highs[-1] - lows[-1]) / closes[-1] * 100
        }
        
        return indicators
        
    async def _make_request(self, url: str, params: dict = None) -> Optional[Any]:
        """Выполнение HTTP запроса с rate limiting"""
        # Проверяем rate limit
        await self.rate_limiter.acquire()
        
        try:
            self.stats['api_calls'] += 1
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    self.logger.error(f"API error: {response.status} - {await response.text()}")
                    self.stats['errors'] += 1
                    
        except asyncio.TimeoutError:
            self.logger.error(f"Request timeout: {url}")
            self.stats['errors'] += 1
        except Exception as e:
            self.logger.error(f"Request error: {e}")
            self.stats['errors'] += 1
            
        self.stats['cache_misses'] += 1
        return None
        
    async def _get_from_redis(self, key: str) -> Optional[Any]:
        """Получение из Redis"""
        if not self.redis_enabled or not self.redis_client:
            return None
            
        try:
            data = await self.redis_client.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            self.logger.debug(f"Redis get error: {e}")
            
        return None
        
    async def _save_to_redis(self, key: str, data: Any, ttl: int = 300):
        """Сохранение в Redis"""
        if not self.redis_enabled or not self.redis_client:
            return
            
        try:
            await self.redis_client.setex(
                key, 
                ttl, 
                json.dumps(data)
            )
        except Exception as e:
            self.logger.debug(f"Redis set error: {e}")
            
    def get_stats(self) -> dict:
        """Получение статистики"""
        total_requests = self.stats['cache_hits'] + self.stats['cache_misses']
        cache_hit_rate = self.stats['cache_hits'] / total_requests if total_requests > 0 else 0
        
        return {
            'api_calls': self.stats['api_calls'],
            'cache_hits': self.stats['cache_hits'],
            'cache_misses': self.stats['cache_misses'],
            'cache_hit_rate': cache_hit_rate,
            'errors': self.stats['errors'],
            'error_rate': self.stats['errors'] / self.stats['api_calls'] if self.stats['api_calls'] > 0 else 0
        }
        
    def reset_stats(self):
        """Сброс статистики"""
        self.stats = {
            'api_calls': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'errors': 0
        }


class RateLimiter:
    """Rate limiter для API запросов"""
    
    def __init__(self, max_requests: int, time_window: int):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = asyncio.Lock()
        
    async def acquire(self):
        """Ожидание разрешения на запрос"""
        async with self.lock:
            now = time.time()
            
            # Удаляем старые запросы
            self.requests = [r for r in self.requests if r > now - self.time_window]
            
            # Проверяем лимит
            if len(self.requests) >= self.max_requests:
                # Ждем до освобождения слота
                sleep_time = self.requests[0] + self.time_window - now + 0.1
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    
                # Рекурсивно пробуем снова
                await self.acquire()
            else:
                # Добавляем текущий запрос
                self.requests.append(now)


class MarketDataAggregator:
    """Агрегатор рыночных данных для анализа"""
    
    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager
        self.logger = logging.getLogger(__name__)
        
    async def get_market_overview(self) -> dict:
        """Получение обзора рынка"""
        try:
            # Получаем данные по всем символам
            tickers = await self.data_manager.get_tickers_24h_batch()
            
            if not tickers:
                return {}
                
            # Анализируем данные
            total_volume = sum(float(t['quoteVolume']) for t in tickers)
            
            # Топ по объему
            sorted_by_volume = sorted(
                tickers, 
                key=lambda x: float(x['quoteVolume']), 
                reverse=True
            )[:10]
            
            # Топ растущих
            sorted_by_gain = sorted(
                tickers, 
                key=lambda x: float(x['priceChangePercent']), 
                reverse=True
            )[:10]
            
            # Топ падающих
            sorted_by_loss = sorted(
                tickers, 
                key=lambda x: float(x['priceChangePercent'])
            )[:10]
            
            # Средние показатели
            price_changes = [float(t['priceChangePercent']) for t in tickers]
            
            return {
                'total_symbols': len(tickers),
                'total_volume_24h': total_volume,
                'average_price_change': np.mean(price_changes),
                'positive_symbols': len([p for p in price_changes if p > 0]),
                'negative_symbols': len([p for p in price_changes if p < 0]),
                'top_volume': [t['symbol'] for t in sorted_by_volume],
                'top_gainers': [(t['symbol'], float(t['priceChangePercent'])) for t in sorted_by_gain],
                'top_losers': [(t['symbol'], float(t['priceChangePercent'])) for t in sorted_by_loss]
            }
            
        except Exception as e:
            self.logger.error(f"Error getting market overview: {e}")
            return {}
            
    async def get_sector_performance(self, sector_mapping: dict) -> dict:
        """Анализ производительности по секторам"""
        try:
            tickers = await self.data_manager.get_tickers_24h_batch()
            
            if not tickers:
                return {}
                
            # Группируем по секторам
            sector_data = defaultdict(list)
            
            for ticker in tickers:
                symbol = ticker['symbol']
                # Определяем сектор (упрощенная логика)
                sector = self._determine_sector(symbol, sector_mapping)
                sector_data[sector].append(float(ticker['priceChangePercent']))
                
            # Расчет средних по секторам
            sector_performance = {}
            for sector, changes in sector_data.items():
                if changes:
                    sector_performance[sector] = {
                        'average_change': np.mean(changes),
                        'symbols_count': len(changes),
                        'best_performer': max(changes),
                        'worst_performer': min(changes)
                    }
                    
            return sector_performance
            
        except Exception as e:
            self.logger.error(f"Error analyzing sector performance: {e}")
            return {}
            
    def _determine_sector(self, symbol: str, sector_mapping: dict) -> str:
        """Определение сектора символа"""
        # Проверяем прямое соответствие
        if symbol in sector_mapping:
            return sector_mapping[symbol]
            
        # Простая логика по именам
        symbol_upper = symbol.upper()
        
        if 'BTC' in symbol_upper or 'ETH' in symbol_upper:
            return 'MAJORS'
        elif any(defi in symbol_upper for defi in ['AAVE', 'UNI', 'SUSHI', 'COMP', 'MKR']):
            return 'DEFI'
        elif any(game in symbol_upper for game in ['AXS', 'SAND', 'MANA', 'GALA', 'ENJ']):
            return 'GAMING'
        elif any(ai in symbol_upper for ai in ['FET', 'AGIX', 'OCEAN', 'NMR']):
            return 'AI'
        elif any(meme in symbol_upper for meme in ['DOGE', 'SHIB', 'PEPE', 'FLOKI']):
            return 'MEME'
        else:
            return 'OTHER'