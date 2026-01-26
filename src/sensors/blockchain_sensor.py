"""Blockchain Sensor for Matrix Watcher.

Collects blockchain data from Ethereum and Bitcoin networks.
"""

import asyncio
import logging
import time
from typing import Any

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)

# Public API endpoints (no key required)
ETHEREUM_API = "https://api.etherscan.io/api"
ETHEREUM_BLOCKSCOUT = "https://eth.blockscout.com/api/v2"
BITCOIN_API = "https://blockchain.info"
BLOCKCHAIR_API = "https://api.blockchair.com"


class BlockchainSensor(BaseSensor):
    """Sensor for collecting blockchain network data.
    
    Monitors:
    - Ethereum: block height, hash, timestamp, tx count, gas used/limit
    - Bitcoin: block height, hash, timestamp, tx count, difficulty
    
    Calculates block_interval_sec and flags anomalous intervals (>50% deviation).
    
    Example:
        sensor = BlockchainSensor(networks=["ethereum", "bitcoin"])
        reading = await sensor.collect()
        print(f"ETH block: {reading.data['ethereum']['block_height']}")
    """
    
    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None,
        networks: list[str] | None = None,
        etherscan_api_key: str | None = None
    ):
        """Initialize Blockchain Sensor.
        
        Args:
            config: Sensor configuration
            event_bus: Event bus for publishing
            networks: Networks to monitor (default: ethereum, bitcoin)
            etherscan_api_key: Optional Etherscan API key for higher rate limits
        """
        super().__init__("blockchain", config, event_bus)
        self.networks = networks or ["ethereum", "bitcoin"]
        self.etherscan_api_key = etherscan_api_key
        
        # Track last block times for interval calculation
        self._last_block_times: dict[str, float] = {}
        self._last_block_heights: dict[str, int] = {}
        
        # Expected block intervals (seconds)
        self._expected_intervals = {
            "ethereum": 12,  # ~12 seconds
            "bitcoin": 600   # ~10 minutes
        }
    
    async def collect(self) -> SensorReading:
        """Collect blockchain data from all networks."""
        timestamp = time.time()
        networks_data = {}
        any_anomalous = False
        
        async with aiohttp.ClientSession() as session:
            for network in self.networks:
                if network == "ethereum":
                    data = await self._collect_ethereum(session)
                elif network == "bitcoin":
                    data = await self._collect_bitcoin(session)
                else:
                    logger.warning(f"Unknown network: {network}")
                    continue
                
                if data:
                    networks_data[network] = data
                    if data.get("interval_anomalous"):
                        any_anomalous = True
        
        result = {
            "timestamp": timestamp,
            "networks": networks_data,
            "networks_count": len(networks_data),
            "any_anomalous": any_anomalous
        }
        
        return SensorReading.create(self.name, result)
    
    async def _collect_ethereum(self, session: aiohttp.ClientSession) -> dict[str, Any] | None:
        """Collect Ethereum block data using Blockscout API with retry."""
        # Try Blockscout with retry
        for attempt in range(2):
            try:
                # Use Blockscout public API (no key required)
                async with session.get(
                    f"{ETHEREUM_BLOCKSCOUT}/blocks",
                    params={"type": "block"},
                    timeout=aiohttp.ClientTimeout(total=30),
                    headers={"Accept": "application/json"}
                ) as response:
                    if response.status != 200:
                        if attempt == 0:
                            logger.debug(f"Blockscout returned status {response.status}, retrying...")
                            await asyncio.sleep(1)
                            continue
                        logger.debug(f"Blockscout returned status {response.status}, trying fallback")
                        return await self._collect_ethereum_fallback(session)
                    data = await response.json()
                    
                if not data.get("items"):
                    if attempt == 0:
                        logger.debug("Blockscout returned no items, retrying...")
                        await asyncio.sleep(1)
                        continue
                    logger.debug("Blockscout returned no items, trying fallback")
                    return await self._collect_ethereum_fallback(session)
                
                block = data["items"][0]
                block_height = block.get("height", 0)
                block_hash = block.get("hash", "")
                block_time = int(time.time())  # Blockscout returns ISO timestamp
                
                # Parse timestamp if available
                if "timestamp" in block:
                    try:
                        from datetime import datetime
                        ts = block["timestamp"]
                        if isinstance(ts, str):
                            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                            block_time = int(dt.timestamp())
                        else:
                            block_time = int(ts)
                    except Exception as ts_err:
                        logger.debug(f"Failed to parse timestamp: {ts_err}")
                
                tx_count = int(block.get("tx_count", 0) or 0)
                gas_used = int(block.get("gas_used", 0) or 0)
                gas_limit = int(block.get("gas_limit", 1) or 1)
                
                # Calculate block interval
                interval_data = self._calculate_interval("ethereum", block_height, block_time)
                
                return {
                    "network": "ethereum",
                    "block_height": block_height,
                    "block_hash": block_hash,
                    "block_time": block_time,
                    "tx_count": tx_count,
                    "gas_used": gas_used,
                    "gas_limit": gas_limit,
                    "gas_used_percent": round(gas_used / gas_limit * 100, 2) if gas_limit > 0 else 0,
                    **interval_data
                }
                
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == 0:
                    logger.debug(f"Ethereum collection failed (network/timeout), retrying: {type(e).__name__}")
                    await asyncio.sleep(1)
                    continue
                logger.warning(f"Failed to collect Ethereum data (network/timeout): {type(e).__name__} - {str(e)}")
                return await self._collect_ethereum_fallback(session)
            except Exception as e:
                if attempt == 0:
                    logger.debug(f"Ethereum collection failed (unexpected), retrying: {type(e).__name__}")
                    await asyncio.sleep(1)
                    continue
                logger.warning(f"Failed to collect Ethereum data (unexpected): {type(e).__name__} - {str(e)}")
                return await self._collect_ethereum_fallback(session)
        
        # Should not reach here, but just in case
        return await self._collect_ethereum_fallback(session)
    
    async def _collect_ethereum_fallback(self, session: aiohttp.ClientSession) -> dict[str, Any] | None:
        """Fallback to Blockchair for Ethereum."""
        try:
            async with session.get(
                f"{BLOCKCHAIR_API}/ethereum/stats",
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    logger.warning(f"Blockchair fallback returned status {response.status}")
                    return None
                data = await response.json()
                stats = data.get("data", {})
            
            if not stats:
                logger.warning("Blockchair returned empty data")
                return None
            
            block_height = stats.get("blocks", 0)
            block_time = int(time.time())
            
            interval_data = self._calculate_interval("ethereum", block_height, block_time)
            
            logger.debug(f"Ethereum data collected via Blockchair fallback (block {block_height})")
            
            return {
                "network": "ethereum",
                "block_height": block_height,
                "block_hash": stats.get("best_block_hash", ""),
                "block_time": block_time,
                "tx_count": stats.get("transactions_24h", 0),
                "gas_used": 0,
                "gas_limit": 1,
                "gas_used_percent": 0,
                **interval_data
            }
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Ethereum fallback failed (network/timeout): {type(e).__name__} - {str(e)}")
            return None
        except Exception as e:
            logger.warning(f"Ethereum fallback failed (unexpected): {type(e).__name__} - {str(e)}")
            return None
    
    async def _collect_bitcoin(self, session: aiohttp.ClientSession) -> dict[str, Any] | None:
        """Collect Bitcoin block data."""
        try:
            # Try blockchain.info first
            async with session.get(
                f"{BITCOIN_API}/latestblock",
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    return await self._collect_bitcoin_fallback(session)
                latest = await response.json()
            
            block_height = latest["height"]
            block_hash = latest["hash"]
            block_time = latest["time"]
            
            # Get full block details
            async with session.get(
                f"{BITCOIN_API}/rawblock/{block_hash}",
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    tx_count = latest.get("n_tx", 0)
                    difficulty = 0
                else:
                    block = await response.json()
                    tx_count = block.get("n_tx", len(block.get("tx", [])))
                    difficulty = block.get("bits", 0)
            
            # Calculate block interval
            interval_data = self._calculate_interval("bitcoin", block_height, block_time)
            
            return {
                "network": "bitcoin",
                "block_height": block_height,
                "block_hash": block_hash,
                "block_time": block_time,
                "tx_count": tx_count,
                "difficulty": difficulty,
                **interval_data
            }
            
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Failed to collect Bitcoin data (network/timeout): {type(e).__name__} - {str(e)}")
            return await self._collect_bitcoin_fallback(session)
        except Exception as e:
            logger.warning(f"Failed to collect Bitcoin data (unexpected): {type(e).__name__} - {str(e)}")
            return await self._collect_bitcoin_fallback(session)
    
    async def _collect_bitcoin_fallback(self, session: aiohttp.ClientSession) -> dict[str, Any] | None:
        """Fallback to Blockchair API for Bitcoin."""
        try:
            async with session.get(
                f"{BLOCKCHAIR_API}/bitcoin/stats",
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    return None
                data = await response.json()
                stats = data.get("data", {})
            
            block_height = stats.get("blocks", 0)
            block_time = int(time.time())  # Approximate
            
            interval_data = self._calculate_interval("bitcoin", block_height, block_time)
            
            return {
                "network": "bitcoin",
                "block_height": block_height,
                "block_hash": stats.get("best_block_hash", ""),
                "block_time": block_time,
                "tx_count": stats.get("transactions_24h", 0),
                "difficulty": stats.get("difficulty", 0),
                **interval_data
            }
            
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Bitcoin fallback failed (network/timeout): {type(e).__name__} - {str(e)}")
            return None
        except Exception as e:
            logger.warning(f"Bitcoin fallback failed (unexpected): {type(e).__name__} - {str(e)}")
            return None
    
    def _calculate_interval(
        self, 
        network: str, 
        block_height: int, 
        block_time: float
    ) -> dict[str, Any]:
        """Calculate block interval and detect anomalies.
        
        Args:
            network: Network name
            block_height: Current block height
            block_time: Current block timestamp
            
        Returns:
            Interval data dictionary
        """
        last_time = self._last_block_times.get(network)
        last_height = self._last_block_heights.get(network)
        
        self._last_block_times[network] = block_time
        self._last_block_heights[network] = block_height
        
        if last_time is None or last_height is None or block_height <= last_height:
            return {
                "block_interval_sec": None,
                "interval_anomalous": False,
                "blocks_since_last": 0
            }
        
        blocks_diff = block_height - last_height
        time_diff = block_time - last_time
        interval_per_block = time_diff / blocks_diff if blocks_diff > 0 else 0
        
        # Check for anomaly (>50% deviation from expected)
        expected = self._expected_intervals.get(network, 60)
        deviation = abs(interval_per_block - expected) / expected if expected > 0 else 0
        is_anomalous = deviation > 0.5
        
        return {
            "block_interval_sec": round(interval_per_block, 2),
            "interval_anomalous": is_anomalous,
            "interval_deviation_percent": round(deviation * 100, 2),
            "blocks_since_last": blocks_diff
        }
    
    def get_schema(self) -> dict[str, type]:
        """Get schema for blockchain sensor data."""
        return {
            "timestamp": float,
            "networks": dict,
            "networks_count": int,
            "any_anomalous": bool
        }
