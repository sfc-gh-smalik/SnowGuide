import requests
from bs4 import BeautifulSoup, Comment, SoupStrainer
import time
import logging
import hashlib
import sys
from urllib.parse import urljoin, urlparse, urlunparse
from collections import defaultdict, deque
from typing import Set, Dict, List, Optional, Tuple, Union
import re
from datetime import datetime, timedelta
import json
import os
import csv
from dataclasses import dataclass, asdict
from langdetect import detect
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import random
import psutil
import weakref
from snowflake.snowpark import Session
from snowflake.snowpark.types import *
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class CrawlResult:
    url: str
    found_on: str
    depth: int
    timestamp: str
    content_hash: str
    page_title: str
    status_code: int
    language: str
    last_visited: str = ""
    content_changed: bool = False
    previous_hash: str = ""
    visit_count: int = 1
    # Enhanced content fields for database storage
    raw_html: str = ""
    cleaned_text: str = ""
    content_size: int = 0
    content_type: str = ""
    extracted_links: List[str] = None
    meta_description: str = ""
    meta_keywords: str = ""
    headings: Dict[str, List[str]] = None
    images: List[Dict[str, str]] = None
    structured_data: Dict = None

    def __post_init__(self):
        if self.extracted_links is None:
            self.extracted_links = []
        if self.headings is None:
            self.headings = {}
        if self.images is None:
            self.images = []
        if self.structured_data is None:
            self.structured_data = {}


class MemoryMonitor:
    """Monitor memory usage and implement cache size limits."""

    def __init__(self, max_memory_mb: int = 1024):
        self.max_memory_mb = max_memory_mb
        self.process = psutil.Process()

    def get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB."""
        return self.process.memory_info().rss / 1024 / 1024

    def is_memory_limit_exceeded(self) -> bool:
        """Check if memory limit is exceeded."""
        return self.get_memory_usage_mb() > self.max_memory_mb

    def log_memory_usage(self):
        """Log current memory usage."""
        usage = self.get_memory_usage_mb()
        logging.info(f"Memory usage: {usage:.2f} MB")


class URLValidator:
    """Enhanced URL validation with security checks."""

    @staticmethod
    def is_valid_https_url(url: str) -> bool:
        """Validate HTTPS URL with security checks."""
        if not url or not isinstance(url, str):
            return False

        try:
            parsed = urlparse(url)

            # Must be HTTPS
            if parsed.scheme != 'https':
                return False

            # Must have valid netloc
            if not parsed.netloc:
                return False

            # Block localhost and private IPs
            if any(blocked in parsed.netloc.lower() for blocked in
                   ['localhost', '127.0.0.1', '0.0.0.0', '10.', '192.168.', '172.']):
                return False

            # Block suspicious file extensions
            suspicious_extensions = ['.exe', '.zip', '.pdf', '.doc', '.docx', '.xls', '.xlsx']
            if any(parsed.path.lower().endswith(ext) for ext in suspicious_extensions):
                return False

            return True
        except Exception:
            return False


def detect_language_from_url_path(url: str) -> Optional[str]:
    """Detect language from URL path patterns like /en/, /fr/, /de/, etc."""
    if not url:
        return None

    try:
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()

        language_patterns = {
            'en': [r'/en/'],
            'fr': [r'/fr/'],
            'de': [r'/de/'],
            'ja': [r'/ja/'],
            'ko': [r'/ko/'],
            'pt': [r'/pt/'],
            'es': [r'/es/'],
            'it': [r'/it/'],
            'ru': [r'/ru/'],
            'zh': [r'/zh/']
        }

        for lang, patterns in language_patterns.items():
            for pattern in patterns:
                if re.search(pattern, path):
                    return lang

        if parsed_url.netloc:
            subdomain = parsed_url.netloc.split('.')[0].lower()
            for lang in language_patterns.keys():
                if subdomain == lang or subdomain.startswith(f"{lang}-"):
                    return lang

        if parsed_url.query:
            query_lower = parsed_url.query.lower()
            for lang in language_patterns.keys():
                if f"lang={lang}" in query_lower or f"language={lang}" in query_lower:
                    return lang

        return None

    except Exception:
        return None


class GracefulKiller:
    """Handle graceful shutdown without using signals."""

    def __init__(self, timeout_hours: Optional[float] = None):
        self.kill_now = threading.Event()
        self.start_time = time.time()
        self.timeout_hours = timeout_hours

    def stop(self):
        """Stop the crawler gracefully."""
        self.kill_now.set()

    # def should_stop(self) -> bool:
    #     """Check if we should stop."""
    #     return self.kill_now.is_set()

    def should_stop(self) -> bool:
        """Check if we should stop due to manual intervention or timeout."""
        if self.kill_now.is_set():
            return True
            
        if self.timeout_hours:
            elapsed_hours = (time.time() - self.start_time) / 3600
            if elapsed_hours >= self.timeout_hours:
                logging.info(f"Maximum runtime of {self.timeout_hours} hours reached. Stopping crawler.")
                self.stop()
                return True
        
        return False


class DatabaseConnectionPool:
    """Connection pool for database operations."""

    def __init__(self, config: Dict, pool_size: int = 5):
        self.config = config
        self.pool_size = pool_size
        self.connections = Queue(maxsize=pool_size)
        self.lock = threading.Lock()
        self._initialize_pool()

    def _initialize_pool(self):
        """Initialize connection pool."""
        try:
            import snowflake.connector
            for _ in range(self.pool_size):
                conn = snowflake.connector.connect(**self.config)
                self.connections.put(conn)
        except Exception as e:
            logging.error(f"Failed to initialize connection pool: {e}")

    def get_connection(self):
        """Get connection from pool."""
        try:
            return self.connections.get(timeout=5)
        except Empty:
            # Create new connection if pool is empty
            import snowflake.connector
            return snowflake.connector.connect(**self.config)

    def return_connection(self, conn):
        """Return connection to pool."""
        try:
            if conn and not conn.is_closed():
                self.connections.put(conn, timeout=1)
        except:
            # Connection is bad, don't return to pool
            pass


def test_snowflake_connection():
    """Test Snowflake connection before crawling."""
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        result = session.sql("SELECT CURRENT_VERSION()").collect()
        print(f"Snowflake connection successful: {result[0][0]}")
        return True
    except Exception as e:
        print(f"Snowflake connection failed: {e}")
        return False


class SnowparkStorage:
    """Enhanced Snowflake database operations with content storage capabilities."""

    def __init__(self, table_prefix: str = "CRAWLER", max_cache_size: int = 10000):
        from snowflake.snowpark.context import get_active_session
        self.session = get_active_session()
        self.table_prefix = table_prefix.upper()
        self.max_cache_size = max_cache_size
        self.lock = threading.RLock()
        self.memory_monitor = MemoryMonitor()

        # Batch processing for optimization
        self.batch_size = 100
        self.visited_urls_batch = []
        self.content_hashes_batch = []
        self.discovered_urls_batch = []

        # In-memory cache with size limits
        self.visited_cache = set()
        self.content_hash_cache = set()
        self.url_cache = set()

        # Table names
        self.discovered_urls_table = f"{self.table_prefix}_DISCOVERED_URLS"
        self.crawler_state_table = f"{self.table_prefix}_STATE"
        self.visited_urls_table = f"{self.table_prefix}_VISITED_URLS"
        self.content_hashes_table = f"{self.table_prefix}_CONTENT_HASHES"
        self.revisit_schedule_table = f"{self.table_prefix}_REVISIT_SCHEDULE"

        self._create_tables()
        self._load_caches()

    def _manage_cache_size(self):
        """Manage cache size to prevent memory issues."""
        if len(self.visited_cache) > self.max_cache_size:
            # Remove oldest 20% of entries
            remove_count = len(self.visited_cache) // 5
            urls_to_remove = list(self.visited_cache)[:remove_count]
            for url in urls_to_remove:
                self.visited_cache.discard(url)
            logging.info(f"Trimmed visited cache by {remove_count} entries")

    def _create_tables(self):
        """Create enhanced tables with content storage capabilities."""
        with self.lock:
            try:
                # Enhanced discovered URLs table with content storage
                self.session.sql(f'''
                    CREATE TABLE IF NOT EXISTS {self.discovered_urls_table} (
                        URL STRING PRIMARY KEY,
                        FOUND_ON STRING,
                        DEPTH NUMBER,
                        TIMESTAMP STRING,
                        CONTENT_HASH STRING,
                        PAGE_TITLE STRING,
                        STATUS_CODE NUMBER,
                        LANGUAGE STRING,
                        LAST_VISITED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                        CONTENT_CHANGED BOOLEAN DEFAULT FALSE,
                        PREVIOUS_HASH STRING,
                        VISIT_COUNT NUMBER DEFAULT 1,
                        NEXT_REVISIT_TIME TIMESTAMP_NTZ,
                        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                        -- Content storage fields
                        RAW_HTML VARIANT,
                        CLEANED_TEXT VARCHAR,
                        CONTENT_SIZE NUMBER,
                        CONTENT_TYPE STRING,
                        EXTRACTED_LINKS VARIANT,
                        META_DESCRIPTION STRING,
                        META_KEYWORDS STRING,
                        HEADINGS VARIANT,
                        IMAGES VARIANT,
                        STRUCTURED_DATA VARIANT
                    )
                ''').collect()

                # Create content storage table for large content
                content_table = f"{self.table_prefix}_PAGE_CONTENT"
                self.session.sql(f'''
                    CREATE TABLE IF NOT EXISTS {content_table} (
                        URL STRING PRIMARY KEY,
                        RAW_HTML STRING,
                        CLEANED_TEXT STRING,
                        EXTRACTED_DATA VARIANT,
                        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                ''').collect()

                # Other tables with improved schema
                self.session.sql(f'''
                    CREATE TABLE IF NOT EXISTS {self.revisit_schedule_table} (
                        URL STRING PRIMARY KEY,
                        LAST_HASH STRING,
                        LAST_REVISIT TIMESTAMP_NTZ,
                        NEXT_REVISIT TIMESTAMP_NTZ,
                        REVISIT_INTERVAL_HOURS NUMBER DEFAULT 24,
                        CONSECUTIVE_UNCHANGED NUMBER DEFAULT 0,
                        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                ''').collect()

                self.session.sql(f'''
                    CREATE TABLE IF NOT EXISTS {self.crawler_state_table} (
                        STATE_KEY STRING PRIMARY KEY,
                        STATE_VALUE VARIANT,
                        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                ''').collect()

                self.session.sql(f'''
                    CREATE TABLE IF NOT EXISTS {self.visited_urls_table} (
                        URL STRING PRIMARY KEY,
                        VISITED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                ''').collect()

                self.session.sql(f'''
                    CREATE TABLE IF NOT EXISTS {self.content_hashes_table} (
                        CONTENT_HASH STRING PRIMARY KEY,
                        FIRST_URL STRING,
                        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                ''').collect()

                logging.info("Enhanced Snowflake tables created with content storage capabilities")

            except Exception as e:
                logging.error(f"Failed to create tables: {e}")
                raise

    def _load_caches(self):
        """Load existing data into memory caches with size limits."""
        try:
            # Load visited URLs with limit
            result = self.session.sql(f'''
                SELECT URL FROM {self.visited_urls_table} 
                ORDER BY VISITED_AT DESC 
                LIMIT {self.max_cache_size}
            ''').collect()
            self.visited_cache = {row['URL'] for row in result}

            # Load content hashes with limit
            result = self.session.sql(f'''
                SELECT CONTENT_HASH FROM {self.content_hashes_table} 
                ORDER BY CREATED_AT DESC 
                LIMIT {self.max_cache_size}
            ''').collect()
            self.content_hash_cache = {row['CONTENT_HASH'] for row in result}

            # Load discovered URLs with limit
            result = self.session.sql(f'''
                SELECT URL FROM {self.discovered_urls_table} 
                ORDER BY CREATED_AT DESC 
                LIMIT {self.max_cache_size}
            ''').collect()
            self.url_cache = {row['URL'] for row in result}

            logging.info(
                f"Loaded caches: {len(self.visited_cache)} visited, "
                f"{len(self.content_hash_cache)} hashes, {len(self.url_cache)} URLs"
            )
        except Exception as e:
            logging.error(f"Error loading caches: {e}")

    # def check_content_change(self, url: str, new_content_hash: str) -> Tuple[bool, str]:
    #     """Check if content has changed using parameterized queries."""
    #     with self.lock:
    #         try:
    #             # Use parameterized query to prevent SQL injection
    #             changed_content = f"""
    #                 SELECT CONTENT_HASH FROM {self.discovered_urls_table}
    #                 WHERE URL = '{url}' """
    #
    #             result = self.session.sql(changed_content).collect()
    #
    #             if result:
    #                 previous_hash = result[0]['CONTENT_HASH']
    #                 return new_content_hash != previous_hash, previous_hash
    #             return True, ""
    #         except Exception as e:
    #             print(f"Error checking content change: {e}")
    #             sys.exit(0)
    #             return True, ""

    def check_content_change(self, url: str, new_content_hash: str) -> Tuple[bool, str]:
        """Check if content has changed and update revisit schedule."""
        with self.lock:
            try:  # First check current content hash
                check_content = f"""SELECT CONTENT_HASH 
                                    FROM {self.discovered_urls_table} 
                                    WHERE URL = '{url}' """
                result = self.session.sql(check_content).collect()
                previous_hash = result[0]['CONTENT_HASH'] if result else ""
                content_changed = new_content_hash != previous_hash  # Update revisit schedule based on content change

                if content_changed:  # Content changed - schedule next revisit sooner
                    update_schedule = f"""MERGE INTO {self.revisit_schedule_table} AS target
                                    USING (SELECT '{url}' AS URL) AS source
                                    ON target.URL = source.URL
                                    WHEN MATCHED THEN UPDATE SET
                                        LAST_HASH = '{new_content_hash}',
                                        LAST_REVISIT = CURRENT_TIMESTAMP(),
                                        NEXT_REVISIT = DATEADD(hours, 24, CURRENT_TIMESTAMP()),
                                        CONSECUTIVE_UNCHANGED = 0
                                    WHEN NOT MATCHED THEN INSERT 
                                        (URL, LAST_HASH, LAST_REVISIT, NEXT_REVISIT, REVISIT_INTERVAL_HOURS)
                                    VALUES ('{url}', '{new_content_hash}', CURRENT_TIMESTAMP(), 
                                         DATEADD(hours, 24, CURRENT_TIMESTAMP()), 24)"""

                    self.session.sql(update_schedule).collect()

                else:  # Content unchanged - increase revisit interval
                    update_schedule = f"""MERGE INTO {self.revisit_schedule_table} AS target
                                    USING (SELECT '{url}' AS URL) AS source
                                    ON target.URL = source.URL
                                    WHEN MATCHED THEN UPDATE SET
                                        LAST_REVISIT = CURRENT_TIMESTAMP(),
                                        NEXT_REVISIT = DATEADD(hours, 
                                            LEAST(revisit_interval_hours * 2, 168), 
                                            CURRENT_TIMESTAMP()),
                                        CONSECUTIVE_UNCHANGED = CONSECUTIVE_UNCHANGED + 1,
                                        REVISIT_INTERVAL_HOURS = LEAST(revisit_interval_hours * 2, 168)
                                    WHEN NOT MATCHED THEN INSERT 
                                        (URL, LAST_HASH, LAST_REVISIT, NEXT_REVISIT, REVISIT_INTERVAL_HOURS)
                                    VALUES 
                                        ('{url}', '{new_content_hash}', CURRENT_TIMESTAMP(), 
                                         DATEADD(hours, 24, CURRENT_TIMESTAMP()), 24)
                                """
                self.session.sql(update_schedule).collect()
                return content_changed, previous_hash
            except Exception as e:
                logging.error(f"Error checking content change: {e}")
                return True, ""

    def check_duplicate_content(self, url: str, content_hash: str) -> Tuple[bool, Optional[str]]:
        """Check if content is duplicate and return (is_duplicate, canonical_url)."""
        with self.lock:
            try:
                chk_dup = f"""
                    SELECT FIRST_URL FROM {self.content_hashes_table} 
                    WHERE CONTENT_HASH = '{content_hash}'
                """

                result = self.session.sql().collect()

                if result:
                    canonical_url = result[0]['FIRST_URL']
                    if canonical_url != url:
                        return True, canonical_url
                return False, None
            except Exception as e:
                logging.error(f"Error checking duplicate content: {e}")
                return False, None

    def get_urls_for_revisit(self, max_age_hours: int = 24, limit: int = 100) -> List[Tuple[str, str, int]]:
        """Get URLs for revisit using parameterized queries."""
        try:
            url_revisit = f"""
                SELECT d.URL, d.FOUND_ON, d.DEPTH
                FROM {self.discovered_urls_table} d
                LEFT JOIN {self.revisit_schedule_table} r ON d.URL = r.URL
                WHERE r.NEXT_REVISIT <= CURRENT_TIMESTAMP()
                   OR DATEDIFF(HOUR, d.LAST_VISITED, CURRENT_TIMESTAMP()) >= {max_age_hours}
                   OR r.URL IS NULL
                ORDER BY d.LAST_VISITED ASC
                LIMIT {limit}
            """

            result = self.session.sql(url_revisit).collect()

            revisit_urls = [(row['URL'], row['FOUND_ON'], row['DEPTH']) for row in result]
            logging.info(f"Found {len(revisit_urls)} URLs due for revisit")
            return revisit_urls

        except Exception as e:
            print(f"Error getting URLs for revisit: {e}")
            sys.exit(0)
            return []

    # def _flush_batches(self):
    #     """Flush all pending batches using parameterized queries."""
    #     print('********Executing _flush_batches***********')
    #     with self.lock:
    #         try:
    #             # Flush visited URLs
    #             if self.visited_urls_batch:
    #                 for url in self.visited_urls_batch:

    #                     merge_url = f"""MERGE INTO {self.visited_urls_table} AS target
    #                         USING (SELECT '{url}' AS URL) AS source
    #                         ON target.URL = source.URL
    #                         WHEN NOT MATCHED THEN INSERT (URL) VALUES (source.URL)"""

    #                     self.session.sql(merge_url).collect()

    #                 self.visited_cache.update(self.visited_urls_batch)
    #                 self.visited_urls_batch.clear()

    #             # Flush content hashes
    #             if self.content_hashes_batch:
    #                 for content_hash, url in self.content_hashes_batch:

    #                     merge_hashes = f"""MERGE INTO {self.content_hashes_table} AS target
    #                         USING (SELECT '{content_hash}' AS CONTENT_HASH, '{url}' AS FIRST_URL) AS source
    #                         ON target.CONTENT_HASH = source.CONTENT_HASH
    #                         WHEN NOT MATCHED THEN INSERT (CONTENT_HASH, FIRST_URL)
    #                         VALUES (source.CONTENT_HASH, source.FIRST_URL)
    #                     """

    #                     self.session.sql(merge_hashes).collect()

    #                 self.content_hash_cache.update([item[0] for item in self.content_hashes_batch])
    #                 self.content_hashes_batch.clear()

    #             # Flush discovered URLs
    #             if self.discovered_urls_batch:
    #                 for result in self.discovered_urls_batch:
    #                     self.insert_discovered_url_with_content(result)

    #                 self.url_cache.update([result.url for result in self.discovered_urls_batch])
    #                 self.discovered_urls_batch.clear()

    #             self._manage_cache_size()

    #         except Exception as e:
    #             print(f"Error flushing batches: {e}")
    #             sys.exit(0)

    def _flush_batches(self):
        """Optimized batch processing with bulk operations."""
        with self.lock:
            try:
                # Flush visited URLs in bulk
                if self.visited_urls_batch:
                    values = ", ".join([f"('{url}')" for url in self.visited_urls_batch])
                    merge_url = f"""
                        MERGE INTO {self.visited_urls_table} AS target
                        USING (SELECT column1 AS URL FROM VALUES {values}) AS source
                        ON target.URL = source.URL
                        WHEN NOT MATCHED THEN INSERT (URL) VALUES (source.URL)
                    """
                    self.session.sql(merge_url).collect()
                    self.visited_cache.update(self.visited_urls_batch)
                    self.visited_urls_batch.clear()

                # Flush content hashes in bulk
                if self.content_hashes_batch:
                    values = ", ".join([f"('{h}', '{u}')" for h, u in self.content_hashes_batch])
                    merge_hashes = f"""
                        MERGE INTO {self.content_hashes_table} AS target
                        USING (SELECT column1 AS CONTENT_HASH, column2 AS FIRST_URL 
                               FROM VALUES {values}) AS source
                        ON target.CONTENT_HASH = source.CONTENT_HASH
                        WHEN NOT MATCHED THEN INSERT (CONTENT_HASH, FIRST_URL)
                        VALUES (source.CONTENT_HASH, source.FIRST_URL)
                    """
                    self.session.sql(merge_hashes).collect()
                    self.content_hash_cache.update([item[0] for item in self.content_hashes_batch])
                    self.content_hashes_batch.clear()

                # Process discovered URLs in chunks
                if self.discovered_urls_batch:
                    chunk_size = 100
                    for i in range(0, len(self.discovered_urls_batch), chunk_size):
                        chunk = self.discovered_urls_batch[i:i + chunk_size]
                        for result in chunk:
                            self.insert_discovered_url_with_content(result)

                    self.url_cache.update([result.url for result in self.discovered_urls_batch])
                    self.discovered_urls_batch.clear()

                self._manage_cache_size()

            except Exception as e:
                logging.error(f"Error flushing batches: {e}")
                sys.exit(0)

    def insert_discovered_url_with_content(self, crawl_result: CrawlResult) -> bool:
        """Insert discovered URL with full content data."""
        # print('Inside insert_discovered_url_with_content')
        with self.lock:
            try:

                cleaned_text = re.sub(r"'", r"''", crawl_result.cleaned_text)
                headings = json.dumps(crawl_result.headings)
                images = json.dumps(crawl_result.images)
                images = {}

                page_title = re.sub(r"'", r"''", crawl_result.page_title)

                # Insert main record with content
                merge_crawled_urls = f"""
                    MERGE INTO {self.discovered_urls_table} AS target
                    USING (SELECT 
                        '{crawl_result.url}' AS URL,
                        '{crawl_result.found_on}' AS FOUND_ON,
                        '{crawl_result.depth}' AS DEPTH,
                        '{crawl_result.timestamp}' AS TIMESTAMP,
                        '{crawl_result.content_hash}' AS CONTENT_HASH,
                        '{page_title}' AS PAGE_TITLE,
                        '{crawl_result.status_code}' AS STATUS_CODE,
                        '{crawl_result.language}' AS LANGUAGE,
                        '{crawl_result.last_visited}' AS LAST_VISITED,
                        '{str(crawl_result.content_changed).upper()}' AS CONTENT_CHANGED,
                        '{crawl_result.previous_hash}' AS PREVIOUS_HASH,
                        {crawl_result.visit_count} AS VISIT_COUNT,
                        '{cleaned_text}' AS CLEANED_TEXT,
                        '{crawl_result.content_size}' AS CONTENT_SIZE,
                        '{crawl_result.content_type}' AS CONTENT_TYPE,
                        PARSE_JSON('{json.dumps(crawl_result.extracted_links)}') AS EXTRACTED_LINKS,
                        '{crawl_result.meta_description}' AS META_DESCRIPTION,
                        '{crawl_result.meta_keywords}' AS META_KEYWORDS,
                        PARSE_JSON('{headings}') AS HEADINGS,
                        PARSE_JSON('{images}') AS IMAGES,
                        PARSE_JSON('{json.dumps(crawl_result.structured_data)}') AS STRUCTURED_DATA
                    ) AS source
                    ON target.URL = source.URL
                    WHEN MATCHED THEN UPDATE SET
                        CONTENT_HASH = source.CONTENT_HASH,
                        LAST_VISITED = CURRENT_TIMESTAMP(),
                        CONTENT_CHANGED = source.CONTENT_CHANGED,
                        VISIT_COUNT = target.VISIT_COUNT + 1,
                        CLEANED_TEXT = source.CLEANED_TEXT,
                        CONTENT_SIZE = source.CONTENT_SIZE,
                        EXTRACTED_LINKS = source.EXTRACTED_LINKS,
                        META_DESCRIPTION = source.META_DESCRIPTION,
                        META_KEYWORDS = source.META_KEYWORDS,
                        HEADINGS = source.HEADINGS,
                        IMAGES = source.IMAGES,
                        STRUCTURED_DATA = source.STRUCTURED_DATA,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        URL, FOUND_ON, DEPTH, TIMESTAMP, CONTENT_HASH, PAGE_TITLE, STATUS_CODE, LANGUAGE,
                        LAST_VISITED, CONTENT_CHANGED, PREVIOUS_HASH, VISIT_COUNT,
                        CLEANED_TEXT, CONTENT_SIZE, CONTENT_TYPE, EXTRACTED_LINKS,
                        META_DESCRIPTION, META_KEYWORDS, HEADINGS, IMAGES, STRUCTURED_DATA
                    ) VALUES (
                        source.URL, source.FOUND_ON, source.DEPTH, source.TIMESTAMP,
                        source.CONTENT_HASH, source.PAGE_TITLE, source.STATUS_CODE, source.LANGUAGE,
                        source.LAST_VISITED, source.CONTENT_CHANGED, source.PREVIOUS_HASH, source.VISIT_COUNT,
                        source.CLEANED_TEXT, source.CONTENT_SIZE, source.CONTENT_TYPE, source.EXTRACTED_LINKS,
                        source.META_DESCRIPTION, source.META_KEYWORDS, source.HEADINGS, source.IMAGES, source.STRUCTURED_DATA
                    )
                """

                self.session.sql(merge_crawled_urls).collect()

                # Store raw HTML separately if enabled and content is large
                if crawl_result.raw_html and len(crawl_result.raw_html) > 1000:
                    content_table = f"{self.table_prefix}_PAGE_CONTENT"

                    extracted_data = {}
                    extracted_data = {
                        'headings': crawl_result.headings,
                        'images': crawl_result.images,
                        'links': crawl_result.extracted_links,
                        'structured_data': crawl_result.structured_data
                    }

                    extracted_data = json.dumps(extracted_data)

                    cleaned_text = crawl_result.cleaned_text
                    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                    cleaned_text = re.sub(r"'", r"''", crawl_result.cleaned_text)

                    merge_content = f"""
                        MERGE INTO {content_table} AS target
                        USING (SELECT 
                            '{crawl_result.url}' AS URL, 
                            '' AS RAW_HTML, 
                            '{cleaned_text}' AS CLEANED_TEXT, 
                            PARSE_JSON('{extracted_data}') AS EXTRACTED_DATA
                        ) AS source
                        ON target.URL = source.URL
                        WHEN MATCHED THEN UPDATE SET 
                            RAW_HTML = source.RAW_HTML,
                            CLEANED_TEXT = source.CLEANED_TEXT,
                            EXTRACTED_DATA = source.EXTRACTED_DATA,
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT (URL, RAW_HTML, CLEANED_TEXT, EXTRACTED_DATA)
                        VALUES (source.URL, source.RAW_HTML, source.CLEANED_TEXT, source.EXTRACTED_DATA)
                    """

                    self.session.sql(merge_content).collect()

                return True

            except Exception as e:
                print(f"Error inserting URL with content: {e}")
                # return False  # Remove sys.exit(0) as it's not a good practice in a function
                sys.exit(0)

    def url_exists(self, url: str) -> bool:
        """Check if URL exists with cache fallback."""
        return url in self.url_cache

    def is_url_visited(self, url: str) -> bool:
        """Check if URL has been visited with cache fallback."""
        return url in self.visited_cache

    def mark_url_visited(self, url: str):
        """Mark URL as visited with batching."""
        with self.lock:
            if url not in self.visited_cache:
                self.visited_urls_batch.append(url)
                self.visited_cache.add(url)

                if len(self.visited_urls_batch) >= self.batch_size:
                    self._flush_batches()

    def add_content_hash(self, content_hash: str, url: str):
        """Add content hash with batching."""
        with self.lock:
            if content_hash not in self.content_hash_cache:
                self.content_hashes_batch.append((content_hash, url))
                self.content_hash_cache.add(content_hash)

                if len(self.content_hashes_batch) >= self.batch_size:
                    self._flush_batches()

    def insert_discovered_url(self, crawl_result: CrawlResult) -> bool:
        """Insert discovered URL with batching."""
        with self.lock:
            if crawl_result.url not in self.url_cache:
                self.discovered_urls_batch.append(crawl_result)
                self.url_cache.add(crawl_result.url)

                if len(self.discovered_urls_batch) >= self.batch_size:
                    self._flush_batches()
                return True
            return False

    def save_crawler_state(self, state_data: Dict):
        """Save crawler state using parameterized queries."""
        self._flush_batches()

        with self.lock:
            try:
                for key, value in state_data.items():
                    value_json = json.dumps(value)

                    merge_state = f"""
                        MERGE INTO {self.crawler_state_table} AS target
                        USING (SELECT '{key}' AS STATE_KEY, PARSE_JSON('{value_json}') AS STATE_VALUE) AS source
                        ON target.STATE_KEY = source.STATE_KEY
                        WHEN MATCHED THEN UPDATE SET 
                            STATE_VALUE = source.STATE_VALUE, 
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT 
                            (STATE_KEY, STATE_VALUE) 
                            VALUES (source.STATE_KEY, source.STATE_VALUE)
                    """

                    # Bind parameters safely
                    self.session.sql(merge_state).collect()

                logging.info("Crawler state saved to Snowflake")

            except Exception as e:
                print(f"Error saving crawler state: {e}")
                # sys.exit(0)

    def load_crawler_state(self) -> Dict:
        """Load crawler state using safe queries."""
        with self.lock:
            try:
                result = self.session.sql(f'''
                    SELECT STATE_KEY, STATE_VALUE FROM {self.crawler_state_table}
                ''').collect()

                state = {}
                for row in result:
                    key = row['STATE_KEY']
                    value_json = row['STATE_VALUE']
                    try:
                        state[key] = json.loads(value_json) if value_json else None
                    except (json.JSONDecodeError, TypeError):
                        state[key] = value_json

                logging.info(f"Loaded crawler state with {len(state)} keys")
                return state

            except Exception as e:
                logging.error(f"Error loading crawler state: {e}")
                return {}

    def get_discovered_urls_count(self) -> int:
        """Get count with cache fallback."""
        return len(self.url_cache)

    def get_visited_urls_count(self) -> int:
        """Get count with cache fallback."""
        return len(self.visited_cache)

    def get_unvisited_discovered_urls(self, limit: int = 100) -> List[Tuple[str, str, int]]:
        """Get unvisited URLs using parameterized queries."""
        try:
            get_unvisisted_urls = f"""
                SELECT d.URL, d.FOUND_ON, d.DEPTH + 1 as NEXT_DEPTH
                FROM {self.discovered_urls_table} d
                LEFT JOIN {self.visited_urls_table} v ON d.URL = v.URL
                WHERE v.URL IS NULL
                ORDER BY d.DEPTH, d.CREATED_AT
                LIMIT {limit} """

            result = self.session.sql(get_unvisisted_urls).collect()

            return [(row['URL'], row['FOUND_ON'], row['NEXT_DEPTH']) for row in result]
        except Exception as e:
            print(f"Error finding unvisited URLs: {e}")
            sys.exit(0) # hard stop
            return []

    def export_to_csv(self, filename: str):
        """Export to CSV using Snowpark with error handling."""
        self._flush_batches()

        with self.lock:
            try:
                from snowflake.snowpark.functions import col

                df = self.session.table(self.discovered_urls_table).select(
                    col("URL"), col("FOUND_ON"), col("DEPTH"), col("TIMESTAMP"),
                    col("CONTENT_HASH"), col("PAGE_TITLE"), col("STATUS_CODE"), col("LANGUAGE"),
                    col("LAST_VISITED"), col("CONTENT_CHANGED"), col("PREVIOUS_HASH"), col("VISIT_COUNT")
                ).order_by(col("CREATED_AT"))

                pandas_df = df.to_pandas()
                pandas_df.to_csv(filename, index=False, encoding='utf-8')

                logging.info(f"Exported discovered URLs to {filename}")

            except Exception as e:
                logging.error(f"Error exporting to CSV: {e}")

    def get_statistics(self) -> Dict:
        """Get statistics with error handling."""
        self._flush_batches()

        with self.lock:
            try:
                stats = {
                    'total_discovered': len(self.url_cache),
                    'total_visited': len(self.visited_cache)
                }

                # Language distribution
                result = self.session.sql(f'''
                    SELECT LANGUAGE, COUNT(*) as count 
                    FROM {self.discovered_urls_table} 
                    GROUP BY LANGUAGE 
                    ORDER BY COUNT(*) DESC
                ''').collect()
                stats['language_distribution'] = {row['LANGUAGE']: row['COUNT'] for row in result}

                # Depth distribution
                result = self.session.sql(f'''
                    SELECT DEPTH, COUNT(*) as count 
                    FROM {self.discovered_urls_table} 
                    GROUP BY DEPTH 
                    ORDER BY DEPTH
                ''').collect()
                stats['depth_distribution'] = {row['DEPTH']: row['COUNT'] for row in result}

                # Status code distribution
                result = self.session.sql(f'''
                    SELECT STATUS_CODE, COUNT(*) as count 
                    FROM {self.discovered_urls_table} 
                    GROUP BY STATUS_CODE 
                    ORDER BY COUNT(*) DESC
                ''').collect()
                stats['status_code_distribution'] = {row['STATUS_CODE']: row['COUNT'] for row in result}

                return stats

            except Exception as e:
                logging.error(f"Error getting statistics: {e}")
                return {}

    def get_content_statistics(self) -> Dict:
        """Get statistics about stored content."""
        try:
            stats = {}

            # Content size distribution
            result = self.session.sql(f'''
                SELECT 
                    AVG(CONTENT_SIZE) as avg_size,
                    MIN(CONTENT_SIZE) as min_size,
                    MAX(CONTENT_SIZE) as max_size,
                    COUNT(*) as total_pages
                FROM {self.discovered_urls_table}
                WHERE CONTENT_SIZE > 0
            ''').collect()

            if result:
                stats['content_size'] = {
                    'average': result[0]['AVG_SIZE'],
                    'minimum': result[0]['MIN_SIZE'],
                    'maximum': result[0]['MAX_SIZE'],
                    'total_pages': result[0]['TOTAL_PAGES']
                }

            # Meta description coverage
            result = self.session.sql(f'''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN META_DESCRIPTION != '' THEN 1 ELSE 0 END) as with_description
                FROM {self.discovered_urls_table}
            ''').collect()

            if result:
                total = result[0]['TOTAL']
                with_desc = result[0]['WITH_DESCRIPTION']
                stats['meta_description_coverage'] = (with_desc / total * 100) if total > 0 else 0

            return stats

        except Exception as e:
            logging.error(f"Error getting content statistics: {e}")
            return {}

    def close(self):
        """Close and flush any pending operations."""
        self._flush_batches()


class CSVStorage:
    """CSV-based storage implementation with similar functionality to SnowparkStorage."""

    def __init__(self, base_filename: str = "crawler", max_cache_size: int = 10000):
        self.base_filename = base_filename
        self.max_cache_size = max_cache_size
        self.lock = threading.RLock()
        self.memory_monitor = MemoryMonitor()

        self.table_prefix = base_filename

        # File paths
        self.discovered_urls_file = f"{base_filename}_discovered_urls.csv"
        self.visited_urls_file = f"{base_filename}_visited_urls.csv"
        self.content_hashes_file = f"{base_filename}_content_hashes.csv"
        self.crawler_state_file = f"{base_filename}_state.json"
        self.crawler_revisit_file = f"{self.base_filename}_revisit_tracking.csv"

        # In-memory cache
        self.visited_cache = set()
        self.content_hash_cache = set()
        self.url_cache = set()

        # Batch processing
        self.batch_size = 10
        self.visited_urls_batch = []
        self.content_hashes_batch = []
        self.discovered_urls_batch = []


        # Check if files exist, if not try to generate from Snowflake
        files_exist = all(os.path.exists(f) for f in [
	        self.discovered_urls_file,
	        self.visited_urls_file,
	        self.content_hashes_file
	    ])
        
        if not files_exist:
            logging.info("Local CSV files not found. Attempting to generate from Snowflake...")
            self._generate_files_from_snowflake()


        # Create files if they don't exist
        self._create_files()
        self._load_caches()

    def _create_files(self):
        """Create CSV files with headers if they don't exist."""
        with self.lock:
            # Discovered URLs file
            if not os.path.exists(self.discovered_urls_file):
                with open(self.discovered_urls_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['url', 'found_on', 'depth', 'timestamp', 'content_hash',
                                     'page_title', 'status_code', 'language', 'last_visited',
                                     'content_changed', 'previous_hash', 'visit_count',
                                     'cleaned_text', 'content_size', 'content_type',
                                     'meta_description', 'meta_keywords', 'headings',
                                     'extracted_links', 'images', 'structured_data'])

            # Visited URLs file
            if not os.path.exists(self.visited_urls_file):
                with open(self.visited_urls_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['url', 'visited_at'])

            # Content hashes file
            if not os.path.exists(self.content_hashes_file):
                with open(self.content_hashes_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['content_hash', 'first_url', 'created_at'])

    def _load_caches(self):
        """Load existing data into memory caches."""
        try:
            # Load visited URLs
            if os.path.exists(self.visited_urls_file):
                with open(self.visited_urls_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    self.visited_cache = {row['url'] for row in reader}

            # Load content hashes
            if os.path.exists(self.content_hashes_file):
                with open(self.content_hashes_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    self.content_hash_cache = {row['content_hash'] for row in reader}

            # Load discovered URLs
            if os.path.exists(self.discovered_urls_file):
                with open(self.discovered_urls_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    self.url_cache = {row['url'] for row in reader}

            # Load revisit tracking data
            if os.path.exists(self.crawler_revisit_file):
                with open(self.crawler_revisit_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    self.revisit_tracking = {
                        row['url']: {
                            'last_hash': row['last_hash'],
                            'last_revisit': row['last_revisit'],
                            'next_revisit': row['next_revisit'],
                            'revisit_interval_hours': int(row['revisit_interval_hours']),
                            'consecutive_unchanged': int(row['consecutive_unchanged'])
                        } for row in reader
                    }
            else:
                self.revisit_tracking = {}

        except Exception as e:
            logging.error(f"Error loading caches: {e}")


    def _generate_files_from_snowflake(self):
        """Generate local CSV files from existing Snowflake tables."""
        try:
            from snowflake.snowpark.context import get_active_session
            session = get_active_session()
            
            # Check if Snowflake tables exist and create corresponding CSV files
            tables_to_check = [
                (f"{self.table_prefix}_DISCOVERED_URLS", self.discovered_urls_file),
                (f"{self.table_prefix}_VISITED_URLS", self.visited_urls_file),
                (f"{self.table_prefix}_CONTENT_HASHES", self.content_hashes_file),
                (f"{self.table_prefix}_REVISIT_SCHEDULE", self.crawler_revisit_file)  # Added revisit schedule table
            ]
            
            for table_name, csv_file in tables_to_check:
                try:
                    # Check if table exists
                    check_table = f"SHOW TABLES LIKE '{table_name}'"
                    result = session.sql(check_table).collect()
                    
                    if result:
                        # Export table data to CSV
                        query = f"SELECT * FROM {table_name}"
                        df = session.sql(query).to_pandas()

                        # Convert column names to lowercase
                        df.columns = df.columns.str.lower()
                        
                        # Write to CSV file
                        df.to_csv(csv_file, index=False)
                        # logging.info(f"Generated {csv_file} from {table_name}")
                        print(f"Generated {csv_file} from {table_name}")
                        
                except Exception as e:
                    logging.warning(f"Could not generate {csv_file} from {table_name}: {e}")
                    continue
                  
        except Exception as e:
            logging.warning(f"Could not connect to Snowflake to generate local files: {e}")

    
    def _flush_batches(self):
        """Flush all pending batches to CSV files."""

        # print("Flushing batches...")

        import datetime 
        
        with self.lock:
            try:
                # Flush visited URLs
                if self.visited_urls_batch:
                    with open(self.visited_urls_file, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        for url in self.visited_urls_batch:
                            writer.writerow([url, datetime.datetime.now().isoformat()])
                    self.visited_urls_batch.clear()

                # Flush content hashes
                if self.content_hashes_batch:
                    with open(self.content_hashes_file, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        for content_hash, url in self.content_hashes_batch:
                            writer.writerow([content_hash, url, datetime.datetime.now().isoformat()])
                    self.content_hashes_batch.clear()

                # Flush discovered URLs
                if self.discovered_urls_batch:
                    with open(self.discovered_urls_file, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        for result in self.discovered_urls_batch:
                            writer.writerow([
                                result.url, result.found_on, result.depth,
                                result.timestamp, result.content_hash,
                                result.page_title, result.status_code,
                                result.language, result.last_visited,
                                result.content_changed, result.previous_hash,
                                result.visit_count, result.cleaned_text,
                                result.content_size, result.content_type,
                                result.meta_description, result.meta_keywords,
                                json.dumps(result.headings),
                                json.dumps(result.extracted_links),
                                json.dumps(result.images),
                                json.dumps(result.structured_data)
                            ])
                    self.discovered_urls_batch.clear()

            except Exception as e:
                print(f"Error flushing batches: {e}")

    def check_content_change(self, url: str, new_content_hash: str) -> Tuple[bool, str]:
        """Check if content has changed for CSV storage mode."""
        with self.lock:
            try:
                if os.path.exists(self.discovered_urls_file):
                    with open(self.discovered_urls_file, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        # Convert column names to lowercase for case-insensitive matching
                        fieldnames = {name.lower(): name for name in reader.fieldnames}
                        
                        for row in reader:
                            if row[fieldnames['url']].strip() == url:
                                previous_hash = row[fieldnames['content_hash']].strip()
                                content_changed = new_content_hash != previous_hash
                                
                                # Update revisit tracking in a separate file
                                self._update_revisit_tracking(url, new_content_hash, content_changed)
                                
                                return content_changed, previous_hash
                    
                    # URL not found, treat as new content
                    return True, ""
                
                # File doesn't exist, treat as new content
                return True, ""
                
            except Exception as e:
                print(f"*****Error checking content change: {e}")
                return True, ""



    def _update_revisit_tracking(self, url: str, new_hash: str, content_changed: bool):
        """Update revisit tracking information in CSV."""
        import datetime
        
        revisit_file = self.crawler_revisit_file

        # Create file if it doesn't exist
        if not os.path.exists(revisit_file):
            with open(revisit_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['url', 'last_hash', 'last_revisit', 'next_revisit',
                                 'revisit_interval_hours', 'consecutive_unchanged'])

        current_time = datetime.datetime.now()
        rows = []
        found = False
        # print('***************Sunny written data to revisit tracking*************')
        # Read existing data
        with open(revisit_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['url'] == url:
                    found = True
                    # Update existing entry
                    if content_changed:
                        # Reset interval on change
                        next_revisit = current_time + timedelta(hours=24)
                        row.update({
                            'last_hash': new_hash,
                            'last_revisit': current_time.isoformat(),
                            'next_revisit': next_revisit.isoformat(),
                            'revisit_interval_hours': '24',
                            'consecutive_unchanged': '0'
                        })
                    else:
                        # Increase interval if unchanged
                        consecutive = int(row.get('consecutive_unchanged', 0)) + 1
                        interval = min(int(row.get('revisit_interval_hours', 24)) * 2, 168)
                        next_revisit = current_time + timedelta(hours=interval)
                        row.update({
                            'last_revisit': current_time.isoformat(),
                            'next_revisit': next_revisit.isoformat(),
                            'revisit_interval_hours': str(interval),
                            'consecutive_unchanged': str(consecutive)
                        })
                rows.append(row)

        # Add new entry if URL not found
        if not found:
            next_revisit = current_time + timedelta(hours=24)
            rows.append({
                'url': url,
                'last_hash': new_hash,
                'last_revisit': current_time.isoformat(),
                'next_revisit': next_revisit.isoformat(),
                'revisit_interval_hours': '24',
                'consecutive_unchanged': '0'
            })

        # Write updated data
        with open(revisit_file, 'w', newline='', encoding='utf-8') as f:
            if rows:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

    def check_duplicate_content(self, url: str, content_hash: str) -> Tuple[bool, Optional[str]]:
        """Check if content is duplicate."""
        with self.lock:
            try:
                if os.path.exists(self.content_hashes_file):
                    with open(self.content_hashes_file, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            if row['content_hash'] == content_hash:
                                if row['first_url'] != url:
                                    return True, row['first_url']
                return False, None
            except Exception as e:
                logging.error(f"Error checking duplicate content: {e}")
                return False, None

    def mark_url_visited(self, url: str):
        """Mark URL as visited with batching."""
        with self.lock:
            if url not in self.visited_cache:
                self.visited_urls_batch.append(url)
                self.visited_cache.add(url)
                if len(self.visited_urls_batch) >= self.batch_size:
                    self._flush_batches()

    def add_content_hash(self, content_hash: str, url: str):
        """Add content hash with batching."""
        with self.lock:
            if content_hash not in self.content_hash_cache:
                self.content_hashes_batch.append((content_hash, url))
                self.content_hash_cache.add(content_hash)
                if len(self.content_hashes_batch) >= self.batch_size:
                    self._flush_batches()

    def insert_discovered_url_with_content(self, crawl_result: CrawlResult) -> bool:
        """Insert discovered URL with full content data."""
        with self.lock:
            if crawl_result.url not in self.url_cache:
                self.discovered_urls_batch.append(crawl_result)
                self.url_cache.add(crawl_result.url)
                if len(self.discovered_urls_batch) >= self.batch_size:
                    # print('Sunny Calling from insert_discovered_url_with_content -_flush_batches ')
                    self._flush_batches()
                return True
            return False

    def url_exists(self, url: str) -> bool:
        """Check if URL exists."""
        return url in self.url_cache

    def is_url_visited(self, url: str) -> bool:
        """Check if URL has been visited."""
        return url in self.visited_cache

    def get_discovered_urls_count(self) -> int:
        """Get count of discovered URLs."""
        return len(self.url_cache)

    def get_visited_urls_count(self) -> int:
        """Get count of visited URLs."""
        return len(self.visited_cache)

    def save_crawler_state(self, state_data: Dict):
        """Save crawler state to JSON file."""
        with self.lock:
            try:
                with open(self.crawler_state_file, 'w', encoding='utf-8') as f:
                    json.dump(state_data, f)
            except Exception as e:
                logging.error(f"Error saving crawler state: {e}")

    def load_crawler_state(self) -> Dict:
        """Load crawler state from JSON file."""
        with self.lock:
            try:
                if os.path.exists(self.crawler_state_file):
                    with open(self.crawler_state_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
                return {}
            except Exception as e:
                logging.error(f"Error loading crawler state: {e}")
                return {}

    def get_statistics(self) -> Dict:
        """Get crawling statistics."""
        stats = {
            'total_discovered': len(self.url_cache),
            'total_visited': len(self.visited_cache),
            'language_distribution': defaultdict(int),
            'depth_distribution': defaultdict(int),
            'status_code_distribution': defaultdict(int)
        }

        try:
            with open(self.discovered_urls_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    stats['language_distribution'][row['language']] += 1
                    stats['depth_distribution'][int(row['depth'])] += 1
                    stats['status_code_distribution'][int(row['status_code'])] += 1
        except Exception as e:
            logging.error(f"Error getting statistics: {e}")

        return stats

    def close(self):
        """Close and flush any pending operations."""
        self._flush_batches()


def upload_csv_to_snowflake(snowflake_config: dict, table_prefix: str) -> bool:
    """
    Upload CSV files to Snowflake tables using Snowpark DataFrame with merge operations.
    """
    try:
        from snowflake.snowpark import Session
        from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, TimestampType, \
            BooleanType, VariantType
        import pandas as pd
        import os
        from snowflake.snowpark.functions import col, lit, current_timestamp, when

        # Create Snowpark session
        try:
            session = Session.builder.configs(snowflake_config).create()
        except Exception as e:
            try:
                from snowflake.snowpark.context import get_active_session
                session = get_active_session()
            except Exception as e:
                print('FATAL Error : Unable to connect to Snowflake Environment')
                

        # Define table names
        discovered_urls_table = f"{table_prefix}_DISCOVERED_URLS"
        visited_urls_table = f"{table_prefix}_VISITED_URLS"
        content_hashes_table = f"{table_prefix}_CONTENT_HASHES"
        revisit_schedule_table = f"{table_prefix}_REVISIT_SCHEDULE"

        # Define file paths
        discovered_urls_file = f"{table_prefix}_discovered_urls.csv"
        visited_urls_file = f"{table_prefix}_visited_urls.csv"
        content_hashes_file = f"{table_prefix}_content_hashes.csv"
        revisit_tracking_file = f"{table_prefix}_revisit_tracking.csv"

        # Create tables if they don't exist
        create_discovered_urls = f"""
                CREATE TABLE IF NOT EXISTS {discovered_urls_table} (
                    URL STRING PRIMARY KEY,
                    FOUND_ON STRING,
                    DEPTH NUMBER,
                    TIMESTAMP STRING,
                    CONTENT_HASH STRING,
                    PAGE_TITLE STRING,
                    STATUS_CODE NUMBER,
                    LANGUAGE STRING,
                    LAST_VISITED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    CONTENT_CHANGED BOOLEAN DEFAULT FALSE,
                    PREVIOUS_HASH STRING,
                    VISIT_COUNT NUMBER DEFAULT 1,
                    NEXT_REVISIT_TIME TIMESTAMP_NTZ,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    CLEANED_TEXT VARCHAR,
                    CONTENT_SIZE NUMBER,
                    CONTENT_TYPE STRING,
                    EXTRACTED_LINKS VARIANT,
                    META_DESCRIPTION STRING,
                    META_KEYWORDS STRING,
                    HEADINGS VARIANT,
                    IMAGES VARIANT,
                    STRUCTURED_DATA VARIANT
                )
                """

        create_visited_urls = f"""
                CREATE TABLE IF NOT EXISTS {visited_urls_table} (
                    URL STRING PRIMARY KEY,
                    VISITED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """

        create_content_hashes = f"""
                CREATE TABLE IF NOT EXISTS {content_hashes_table} (
                    CONTENT_HASH STRING PRIMARY KEY,
                    FIRST_URL STRING,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """

        # Create revisit tracking table if it doesn't exist
        create_revisit_schedule = f"""
            CREATE TABLE IF NOT EXISTS {revisit_schedule_table} (
                URL STRING PRIMARY KEY,
                LAST_HASH STRING,
                LAST_REVISIT TIMESTAMP_NTZ,
                NEXT_REVISIT TIMESTAMP_NTZ,
                REVISIT_INTERVAL_HOURS NUMBER DEFAULT 24,
                CONSECUTIVE_UNCHANGED NUMBER DEFAULT 0,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """
        
        # Execute create table statements
        session.sql(create_revisit_schedule).collect()
        session.sql(create_discovered_urls).collect()
        session.sql(create_visited_urls).collect()
        session.sql(create_content_hashes).collect()

        # Upload discovered URLs
        if os.path.exists(discovered_urls_file):
            # Read CSV into pandas DataFrame
            df = pd.read_csv(discovered_urls_file)
            df.columns = df.columns.str.upper()

            # Convert pandas DataFrame directly to Snowpark DataFrame
            snow_df = session.create_dataframe(df)

            # # Create temp table for merge operation
            # temp_table = "TEMP_DISCOVERED_URLS"
            # snow_df.write.mode("overwrite").save_as_table(temp_table)


            # Add missing columns with default values
            snow_df = snow_df.withColumn("LAST_VISITED", current_timestamp())
            snow_df = snow_df.withColumn("CREATED_AT", current_timestamp())
            snow_df = snow_df.withColumn("UPDATED_AT", current_timestamp())
            
            # Convert empty strings to NULL for VARIANT columns
            snow_df = snow_df.withColumn("HEADINGS", 
                when(col("HEADINGS") == "{}", None)
                .when(col("HEADINGS") == "", None)
                .otherwise(col("HEADINGS")))
            
            snow_df = snow_df.withColumn("EXTRACTED_LINKS", 
                when(col("EXTRACTED_LINKS") == "[]", None)
                .when(col("EXTRACTED_LINKS") == "", None)
                .otherwise(col("EXTRACTED_LINKS")))
            
            snow_df = snow_df.withColumn("IMAGES", 
                when(col("IMAGES") == "{}", None)
                .when(col("IMAGES") == "", None)
                .otherwise(col("IMAGES")))
            
            snow_df = snow_df.withColumn("STRUCTURED_DATA", 
                when(col("STRUCTURED_DATA") == "{}", None)
                .when(col("STRUCTURED_DATA") == "", None)
                .otherwise(col("STRUCTURED_DATA")))

            # Convert empty strings to NULL for numeric columns
            snow_df = snow_df.withColumn("CONTENT_SIZE", 
                col("CONTENT_SIZE"))

            # Create temp table for merge operation
            temp_table = "TEMP_DISCOVERED_URLS"
            snow_df.write.mode("overwrite").save_as_table(temp_table)


            # Perform merge operation
            merge_query = f"""
            MERGE INTO {discovered_urls_table} target
            USING {temp_table} source
            ON target.URL = source.URL
            WHEN MATCHED THEN UPDATE SET
                CONTENT_HASH = source.CONTENT_HASH,
                LAST_VISITED = CURRENT_TIMESTAMP(),
                CONTENT_CHANGED = source.CONTENT_CHANGED,
                VISIT_COUNT = target.VISIT_COUNT + 1,
                CLEANED_TEXT = source.CLEANED_TEXT,
                CONTENT_SIZE = source.CONTENT_SIZE,
                META_DESCRIPTION = source.META_DESCRIPTION,
                META_KEYWORDS = source.META_KEYWORDS,
                HEADINGS = source.HEADINGS::VARIANT,
                EXTRACTED_LINKS = source.EXTRACTED_LINKS::VARIANT,
                IMAGES = source.IMAGES::VARIANT,
                STRUCTURED_DATA = source.STRUCTURED_DATA::VARIANT,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                URL, FOUND_ON, DEPTH, TIMESTAMP, CONTENT_HASH, PAGE_TITLE,
                STATUS_CODE, LANGUAGE, LAST_VISITED, CONTENT_CHANGED,
                PREVIOUS_HASH, VISIT_COUNT, CLEANED_TEXT, CONTENT_SIZE,
                CONTENT_TYPE, META_DESCRIPTION, META_KEYWORDS, HEADINGS,
                EXTRACTED_LINKS, IMAGES, STRUCTURED_DATA
            ) VALUES (
                source.URL, source.FOUND_ON, source.DEPTH, source.TIMESTAMP,
                source.CONTENT_HASH, source.PAGE_TITLE, source.STATUS_CODE,
                source.LANGUAGE, CURRENT_TIMESTAMP(), source.CONTENT_CHANGED,
                source.PREVIOUS_HASH, source.VISIT_COUNT, source.CLEANED_TEXT,
                source.CONTENT_SIZE, source.CONTENT_TYPE, source.META_DESCRIPTION,
                source.META_KEYWORDS, source.HEADINGS::VARIANT, source.EXTRACTED_LINKS::VARIANT,
                source.IMAGES::VARIANT, source.STRUCTURED_DATA::VARIANT
            )
            """
            session.sql(merge_query).collect()
            session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
            print(f"✓ Merged {len(df)} rows into {discovered_urls_table}")

        # Upload visited URLs
        if os.path.exists(visited_urls_file):
            df = pd.read_csv(visited_urls_file)
            df.columns = df.columns.str.upper()

            snow_df = session.create_dataframe(df)

            temp_table = "TEMP_VISITED_URLS"
            snow_df.write.mode("overwrite").save_as_table(temp_table)

            merge_query = f"""
            MERGE INTO {visited_urls_table} target
            USING {temp_table} source
            ON target.URL = source.URL
            WHEN NOT MATCHED THEN INSERT (URL, VISITED_AT)
            VALUES (source.URL, CURRENT_TIMESTAMP())
            """
            session.sql(merge_query).collect()
            session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
            print(f"✓ Merged {len(df)} rows into {visited_urls_table}")

        # Upload content hashes
        if os.path.exists(content_hashes_file):
            df = pd.read_csv(content_hashes_file)
            df.columns = df.columns.str.upper()

            snow_df = session.create_dataframe(df)

            temp_table = "TEMP_CONTENT_HASHES"
            snow_df.write.mode("overwrite").save_as_table(temp_table)

            merge_query = f"""
            MERGE INTO {content_hashes_table} target
            USING {temp_table} source
            ON target.CONTENT_HASH = source.CONTENT_HASH
            WHEN NOT MATCHED THEN INSERT (CONTENT_HASH, FIRST_URL, CREATED_AT)
            VALUES (source.CONTENT_HASH, source.FIRST_URL, CURRENT_TIMESTAMP())
            """
            session.sql(merge_query).collect()
            session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
            print(f"✓ Merged {len(df)} rows into {content_hashes_table}")


        # Upload revisit tracking data
        if os.path.exists(revisit_tracking_file):
            df = pd.read_csv(revisit_tracking_file)
            df.columns = df.columns.str.upper()

            # Convert pandas DataFrame to Snowpark DataFrame
            snow_df = session.create_dataframe(df)

            # Create temp table for merge operation
            temp_table = "TEMP_REVISIT_SCHEDULE"
            snow_df.write.mode("overwrite").save_as_table(temp_table)

            # Perform merge operation
            merge_query = f"""
            MERGE INTO {revisit_schedule_table} target
            USING {temp_table} source
            ON target.URL = source.URL
            WHEN MATCHED THEN UPDATE SET
                LAST_HASH = source.LAST_HASH,
                LAST_REVISIT = source.LAST_REVISIT::TIMESTAMP_NTZ,
                NEXT_REVISIT = source.NEXT_REVISIT::TIMESTAMP_NTZ,
                REVISIT_INTERVAL_HOURS = source.REVISIT_INTERVAL_HOURS,
                CONSECUTIVE_UNCHANGED = source.CONSECUTIVE_UNCHANGED,
                CREATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                URL, LAST_HASH, LAST_REVISIT, NEXT_REVISIT,
                REVISIT_INTERVAL_HOURS, CONSECUTIVE_UNCHANGED
            ) VALUES (
                source.URL, source.LAST_HASH,
                source.LAST_REVISIT::TIMESTAMP_NTZ,
                source.NEXT_REVISIT::TIMESTAMP_NTZ,
                source.REVISIT_INTERVAL_HOURS,
                source.CONSECUTIVE_UNCHANGED
            )
            """
            session.sql(merge_query).collect()
            session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
            print(f"✓ Merged {len(df)} rows into {revisit_schedule_table}")


        session.close()
        return True

    except Exception as e:
        print(f"Error uploading CSV files to Snowflake: {str(e)}")
        if 'session' in locals():
            session.close()
        return False


class TelemetryManager:
    """Enhanced telemetry manager with health monitoring."""

    def __init__(self, report_interval=15, killer=None):
        self.report_interval = report_interval
        self.start_time = None
        self.last_report_time = None
        self.running = True
        self.killer = killer

        # Enhanced telemetry data
        self.pages_processed = 0
        self.urls_discovered = 0
        self.errors_encountered = 0
        self.queue_size = 0
        self.active_threads = 0
        self.bytes_downloaded = 0
        self.avg_response_time = 0.0
        self.response_times = deque(maxlen=100)  # Use deque with maxlen
        self.pages_per_depth = defaultdict(int)
        self.status_codes = defaultdict(int)
        self.domains_crawled = set()
        self.non_english_pages_skipped = 0
        self.english_pages_processed = 0
        self.url_language_filtered = 0
        self.content_changes_detected = 0
        self.revisited_pages = 0
        self.duplicate_content_skipped = 0

        # Error categorization
        self.error_categories = defaultdict(int)
        self.recoverable_errors = 0
        self.permanent_errors = 0

        # Health monitoring
        self.memory_monitor = MemoryMonitor()
        self.last_health_check = time.time()

        # Thread synchronization
        self.telemetry_lock = threading.RLock()
        self.telemetry_thread = None

        # Setup telemetry logging
        self.telemetry_logger = logging.getLogger('telemetry')
        telemetry_handler = logging.FileHandler('crawler_telemetry.log')
        telemetry_formatter = logging.Formatter('%(asctime)s - TELEMETRY - %(message)s')
        telemetry_handler.setFormatter(telemetry_formatter)
        self.telemetry_logger.addHandler(telemetry_handler)
        self.telemetry_logger.setLevel(logging.ERROR)

    def start_telemetry(self):
        """Start the telemetry reporting thread."""
        self.start_time = time.time()
        self.last_report_time = self.start_time
        self.telemetry_thread = threading.Thread(target=self._telemetry_worker, name="Telemetry")
        self.telemetry_thread.daemon = True
        self.telemetry_thread.start()

    def stop_telemetry(self):
        """Stop the telemetry reporting."""
        self.running = False
        if self.telemetry_thread:
            self.telemetry_thread.join(timeout=2)

    def update_page_processed(self, depth: int, response_time: float, status_code: int, domain: str, content_size: int,
                              is_english: bool, is_revisit: bool = False, content_changed: bool = False,
                              is_duplicate: bool = False):
        """Enhanced page processing update with health monitoring."""
        with self.telemetry_lock:
            self.pages_processed += 1
            self.pages_per_depth[depth] += 1
            self.status_codes[status_code] += 1
            self.domains_crawled.add(domain)
            self.bytes_downloaded += content_size

            if is_revisit:
                self.revisited_pages += 1

            if content_changed:
                self.content_changes_detected += 1

            if is_duplicate:
                self.duplicate_content_skipped += 1

            if is_english:
                self.english_pages_processed += 1
            else:
                self.non_english_pages_skipped += 1

            # Update response time statistics with bounded collection
            self.response_times.append(response_time)
            self.avg_response_time = sum(self.response_times) / len(self.response_times)

            # Health check
            current_time = time.time()
            if current_time - self.last_health_check > 60:  # Check every minute
                self._perform_health_check()
                self.last_health_check = current_time

    def update_error_count(self, error_type: str = "general", is_recoverable: bool = True):
        """Enhanced error tracking with categorization."""
        with self.telemetry_lock:
            self.errors_encountered += 1
            self.error_categories[error_type] += 1

            if is_recoverable:
                self.recoverable_errors += 1
            else:
                self.permanent_errors += 1

    def _perform_health_check(self):
        """Perform health checks and log warnings."""
        try:
            # Memory check
            memory_usage = self.memory_monitor.get_memory_usage_mb()
            if memory_usage > 512:  # Warn if over 512MB
                logging.warning(f"High memory usage: {memory_usage:.2f} MB")

            # Error rate check
            if self.pages_processed > 0:
                error_rate = (self.errors_encountered / self.pages_processed) * 100
                if error_rate > 10:  # Warn if error rate > 10%
                    logging.warning(f"High error rate: {error_rate:.1f}%")

            # Response time check
            if self.avg_response_time > 5.0:  # Warn if avg response time > 5s
                logging.warning(f"Slow response times: {self.avg_response_time:.2f}s average")

        except Exception as e:
            logging.error(f"Health check failed: {e}")

    def update_urls_discovered(self, count: int):
        """Update the count of discovered URLs."""
        with self.telemetry_lock:
            self.urls_discovered += count

    def update_url_language_filtered(self):
        """Increment URL language filtered counter."""
        with self.telemetry_lock:
            self.url_language_filtered += 1

    def update_queue_size(self, size: int):
        """Update current queue size."""
        with self.telemetry_lock:
            self.queue_size = size

    def update_active_threads(self, count: int):
        """Update active thread count."""
        with self.telemetry_lock:
            self.active_threads = count

    def _telemetry_worker(self):
        """Worker thread for periodic telemetry reporting."""
        while self.running and (not self.killer or not self.killer.should_stop()):
            try:
                time.sleep(self.report_interval)
                if self.running and (not self.killer or not self.killer.should_stop()):
                    self._generate_progress_report()
            except Exception as e:
                logging.error(f"Telemetry worker error: {e}")

    def _generate_progress_report(self):
        """Generate enhanced progress report with health metrics."""
        current_time = time.time()

        with self.telemetry_lock:
            elapsed_time = current_time - (self.start_time or current_time)
            time_since_last_report = current_time - (self.last_report_time or current_time)

            # Calculate rates
            pages_per_second = self.pages_processed / elapsed_time if elapsed_time > 0 else 0
            pages_per_second_recent = 0
            if hasattr(self, '_last_pages_processed'):
                pages_since_last = self.pages_processed - self._last_pages_processed
                pages_per_second_recent = pages_since_last / time_since_last_report if time_since_last_report > 0 else 0

            urls_per_second = self.urls_discovered / elapsed_time if elapsed_time > 0 else 0
            mb_downloaded = self.bytes_downloaded / (1024 * 1024)
            mb_per_second = mb_downloaded / elapsed_time if elapsed_time > 0 else 0

            # Memory usage
            memory_usage = self.memory_monitor.get_memory_usage_mb()

            # Create enhanced progress report
            report = self._format_progress_report(
                elapsed_time, pages_per_second, pages_per_second_recent,
                urls_per_second, mb_downloaded, mb_per_second, memory_usage
            )

            # Display and log report
            print("\n" + "=" * 80)
            print(report)
            print("=" * 80 + "\n")

            # Enhanced telemetry logging
            self.telemetry_logger.info(
                f"Pages: {self.pages_processed}, English: {self.english_pages_processed}, "
                f"URLs: {self.urls_discovered}, Errors: {self.errors_encountered}, "
                f"Queue: {self.queue_size}, Rate: {pages_per_second:.2f} p/s, "
                f"Data: {mb_downloaded:.2f} MB, Memory: {memory_usage:.2f} MB, "
                f"Revisited: {self.revisited_pages}, Changes: {self.content_changes_detected}, "
                f"Duplicates: {self.duplicate_content_skipped}"
            )

            # Update tracking variables
            self._last_pages_processed = self.pages_processed
            self.last_report_time = current_time

    def _format_progress_report(self, elapsed_time, pages_per_second, pages_per_second_recent,
                                urls_per_second, mb_downloaded, mb_per_second, memory_usage):
        """Format enhanced progress report with health metrics."""
        import datetime
        
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Build depth distribution
        depth_info = ", ".join([f"D{d}: {count}" for d, count in sorted(self.pages_per_depth.items())])

        # Build status code distribution
        status_info = ", ".join([f"{code}: {count}" for code, count in sorted(self.status_codes.items())])

        # Build error category distribution
        error_info = ", ".join([f"{cat}: {count}" for cat, count in sorted(self.error_categories.items())])

        # Calculate efficiency metrics
        error_rate = (self.errors_encountered / max(self.pages_processed, 1)) * 100
        english_rate = (self.english_pages_processed / max(self.pages_processed, 1)) * 100
        change_rate = (self.content_changes_detected / max(self.revisited_pages, 1)) * 100
        duplicate_rate = (self.duplicate_content_skipped / max(self.pages_processed, 1)) * 100

        report = f"""
🕐 ENHANCED INCREMENTAL CRAWLER PROGRESS REPORT - {datetime.datetime.now().strftime('%H:%M:%S')}

⏱️  Runtime: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}
📊 Pages Processed: {self.pages_processed:,} ({pages_per_second:.2f}/sec avg, {pages_per_second_recent:.2f}/sec recent)
🔄 Pages Revisited: {self.revisited_pages:,}
🔄 Content Changes: {self.content_changes_detected:,} ({change_rate:.1f}% of revisited)
🔗 Duplicate Content: {self.duplicate_content_skipped:,} ({duplicate_rate:.1f}% of total)
🇬🇧 English Pages: {self.english_pages_processed:,} ({english_rate:.1f}% of total)
🌐 Non-English Skipped: {self.non_english_pages_skipped:,} (content filtering)
🔗 URL Language Filtered: {self.url_language_filtered:,} (URL path filtering)
🔗 URLs Discovered: {self.urls_discovered:,} ({urls_per_second:.2f}/sec)
❌ Errors: {self.errors_encountered:,} ({error_rate:.1f}% error rate)
📥 Queue Size: {self.queue_size:,}
🧵 Active Threads: {self.active_threads}

📈 Performance Metrics:
   • Average Response Time: {self.avg_response_time:.3f}s
   • Data Downloaded: {mb_downloaded:.2f} MB ({mb_per_second:.3f} MB/s)
   • Memory Usage: {memory_usage:.2f} MB
   • Domains Crawled: {len(self.domains_crawled)}

🔍 Error Analysis:
   • Recoverable: {self.recoverable_errors}, Permanent: {self.permanent_errors}
   • Categories: {error_info}

📊 Depth Distribution: {depth_info}
🌐 Status Codes: {status_info}

💡 Press Ctrl+C to stop gracefully or call crawler.stop() programmatically
        """.strip()

        return report

    def get_final_report(self):
        """Generate final telemetry report."""
        if not self.start_time:
            return "No telemetry data available"

        total_time = time.time() - self.start_time
        memory_usage = self.memory_monitor.get_memory_usage_mb()

        return self._format_progress_report(
            total_time,
            self.pages_processed / total_time if total_time > 0 else 0,
            0,
            self.urls_discovered / total_time if total_time > 0 else 0,
            self.bytes_downloaded / (1024 * 1024),
            (self.bytes_downloaded / (1024 * 1024)) / total_time if total_time > 0 else 0,
            memory_usage
        )


class ThreadSafeCrawler:
    def __init__(self, config: Dict):
        """Initialize the enhanced multi-threaded crawler with comprehensive improvements."""
        self.config = config


        # Initialize graceful shutdown handler with timeout
        self.killer = GracefulKiller(timeout_hours=config.get('max_runtime_hours'))


        # Initialize URL validator
        self.url_validator = URLValidator()

        # Initialize storage based on mode
        self.use_database = config.get('use_database', False)
        self.table_prefix = config.get('table_prefix', 'CRAWLER')

        if self.use_database:
            # Database-only mode
            max_cache_size = config.get('max_cache_size', 10000)
            self.storage = SnowparkStorage(self.table_prefix, max_cache_size)
            logging.info("Using Snowflake database storage with enhanced content capabilities")

        else:
            # CSV Storage configuration
            csv_config = {
                # 'output_dir': config.get('output_dir', './crawler_data'),
                # 'urls_file': config.get('urls_file', 'discovered_urls.csv'),
                # 'state_file': config.get('state_file', 'crawler_state.json'),
                # 'content_dir': config.get('content_dir', 'page_content'),
                # 'compress_content': config.get('compress_content', False)
            }
            self.storage = CSVStorage(**csv_config)
            print("Using CSV file storage with content capabilities")

        # Thread-safe data structures
        self.urls_to_visit: Queue = Queue()
        self.running = True
        self.new_urls_found = 0
        self.pages_since_last_new = 0

        # Thread synchronization with RLock for reentrant safety
        self.stats_lock = threading.RLock()
        self.session_lock = threading.RLock()

        # Enhanced configuration with validation
        self.max_depth = max(1, config.get('max_depth', 3))
        self.allowed_domains = set(config.get('allowed_domains', []))
        self.starting_urls = config.get('starting_urls', [])
        self.diminishing_returns_threshold = config.get('diminishing_returns_threshold', 10)
        self.diminishing_returns_pages = config.get('diminishing_returns_pages', 20)
        self.request_delay = max(0.1, config.get('request_delay', 1.0))
        self.timeout = max(5, config.get('timeout', 10))
        self.max_retries = max(1, config.get('max_retries', 3))
        self.enable_language_filtering = config.get('enable_language_filtering', True)
        self.enable_url_language_filtering = config.get('enable_url_language_filtering', True)
        self.max_workers = max(1, min(10, config.get('max_workers', 5)))
        self.queue_timeout = max(1, config.get('queue_timeout', 5))
        self.save_interval = max(10, config.get('save_interval', 50))
        self.telemetry_interval = max(5, config.get('telemetry_interval', 15))
        self.export_csv = config.get('export_csv', True)
        self.debug_mode = config.get('debug_mode', False)

        # Enhanced incremental crawling options
        self.enable_content_change_detection = config.get('enable_content_change_detection', True)
        self.revisit_interval_hours = max(1, config.get('revisit_interval_hours', 24))
        self.max_revisit_urls_per_run = max(10, config.get('max_revisit_urls_per_run', 50))
        self.content_change_threshold = max(0.01, config.get('content_change_threshold', 0.1))
        self.force_revisit_depth = max(0, config.get('force_revisit_depth', 2))

        # Content storage configuration
        self.store_raw_html = config.get('store_raw_html', True)
        self.store_cleaned_text = config.get('store_cleaned_text', True)
        self.store_extracted_data = config.get('store_extracted_data', True)
        self.max_content_size = config.get('max_content_size', 1000000)  # 1MB limit
        self.extract_metadata = config.get('extract_metadata', True)
        self.extract_headings = config.get('extract_headings', True)
        self.extract_images = config.get('extract_images', True)
        self.extract_links = config.get('extract_links', True)
        self.extract_structured_data = config.get('extract_structured_data', True)

        # Initialize enhanced telemetry
        self.telemetry = TelemetryManager(self.telemetry_interval, self.killer)

        # Thread-local session storage with enhanced management
        self.session_storage = threading.local()
        self.session_pool = weakref.WeakSet()

        # Statistics tracking with thread safety
        self.pages_processed = 0
        self.errors_encountered = 0
        self.start_time = None
        self._last_save = None

        # Error categorization
        self.error_categories = defaultdict(int)
        self.error_lock = threading.Lock()

        # Setup enhanced logging with thread safety
        log_level = logging.ERROR if self.debug_mode else logging.INFO
        self._setup_logging(log_level)

        # Memory monitoring
        self.memory_monitor = MemoryMonitor()

        # Load previous state
        self._load_state()

        # Initialize starting URLs with enhanced incremental support
        self._initialize_starting_urls()

    def _setup_logging(self, log_level):
        """Setup thread-safe logging configuration."""
        # Clear any existing handlers
        root = logging.getLogger()
        if root.handlers:
            for handler in root.handlers:
                root.removeHandler(handler)
            
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('crawler_debug.log'),
                # logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _process_webpage_content(self, html_content: str, url: str) -> Dict:
        """Optimized content processing with size checks."""
        # Early size check
        content_size = len(html_content)
        if content_size > self.max_content_size:
            return {
                'cleaned_text': '',
                'meta_description': '',
                'meta_keywords': '',
                'headings': {},
                'images': [],
                'extracted_links': [],
                'structured_data': {}
            }

        try:
            soup = BeautifulSoup(html_content, 'html.parser',
                                 parse_only=SoupStrainer(['meta', 'title', 'a', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']))

            # Process only essential content
            meta_description = ""
            meta_keywords = ""
            if self.extract_metadata:
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                meta_description = meta_desc.get('content', '')[:500] if meta_desc else ""

                meta_key = soup.find('meta', attrs={'name': 'keywords'})
                meta_keywords = meta_key.get('content', '')[:500] if meta_key else ""

            # Extract headings more efficiently
            headings = {}
            if self.extract_headings:
                for i in range(1, 7):
                    heading_tags = soup.find_all(f'h{i}', limit=10)
                    if heading_tags:
                        headings[f'h{i}'] = [tag.get_text().strip()[:200] for tag in heading_tags]

            # Optimize link extraction
            extracted_links = []
            if self.extract_links:
                seen_urls = set()
                for link in soup.find_all('a', href=True, limit=100):
                    href = link.get('href', '')
                    if href and href not in seen_urls:
                        absolute_url = urljoin(url, href)
                        if self._is_valid_https_url(absolute_url):
                            extracted_links.append(absolute_url)
                            seen_urls.add(href)

            # Simplified content cleaning
            cleaned_text = ' '.join(soup.stripped_strings)[:50000] if self.store_cleaned_text else ""

            return {
                'cleaned_text': cleaned_text,
                'meta_description': meta_description,
                'meta_keywords': meta_keywords,
                'headings': headings,
                'images': [],  # Simplified image processing
                'extracted_links': extracted_links,
                'structured_data': {}  # Simplified structured data
            }

        except Exception as e:
            logging.error(f"Error processing content for {url}: {e}")
            return {
                'cleaned_text': '',
                'meta_description': '',
                'meta_keywords': '',
                'headings': {},
                'images': [],
                'extracted_links': [],
                'structured_data': {}
            }

    def _get_enhanced_content_hash(self, content: str) -> str:
        """Generate enhanced hash focusing on meaningful content only."""
        try:
            if not content or not isinstance(content, str):
                return hashlib.md5(b'').hexdigest()

            # Parse HTML with error handling
            soup = BeautifulSoup(content, 'html.parser')
            if not soup:
                return hashlib.md5(content.encode('utf-8')).hexdigest()

            # Remove dynamic/irrelevant elements
            for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                tag.decompose()

            # Remove comments
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                comment.extract()

            # Focus on main content areas
            main_content = ""
            content_selectors = [
                'main', 'article', '[role="main"]',
                '.content', '.post', '.entry',
                '#content', '#main', '#post'
            ]

            for selector in content_selectors:
                elements = soup.select(selector)
                if elements:
                    main_content = ' '.join(elem.get_text() for elem in elements)
                    break

            # Fallback to body if no main content found
            if not main_content:
                body = soup.find('body')
                main_content = body.get_text() if body else soup.get_text()

            # Clean and normalize text
            cleaned = re.sub(r'\d{4}-\d{2}-\d{2}', '', main_content)
            cleaned = re.sub(r'\d{1,2}:\d{2}(:\d{2})?', '', cleaned)
            cleaned = re.sub(r'\b\d+\s+(views?|comments?|likes?|shares?)\b', '', cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r'\bposted\s+\d+\s+(minutes?|hours?|days?)\s+ago\b', '', cleaned, flags=re.IGNORECASE)

            # Normalize whitespace
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()

            # Generate hash of normalized content
            return hashlib.sha256(cleaned.encode('utf-8')).hexdigest()

        except Exception as e:
            self.logger.error(f"Error generating enhanced content hash: {e}")
            return hashlib.md5(content.encode('utf-8')).hexdigest()

    def _initialize_starting_urls(self):
        """Initialize queue with starting URLs and URLs due for revisit."""
        urls_added = 0

        # 1. Add URLs due for revisit
        if self.enable_content_change_detection and hasattr(self.storage, 'get_urls_for_revisit'):
            try:
                revisit_urls = self.storage.get_urls_for_revisit(
                    self.revisit_interval_hours,
                    self.max_revisit_urls_per_run
                )

                for url, found_on, depth in revisit_urls:
                    if self.url_validator.is_valid_https_url(url):
                        self.urls_to_visit.put((url, found_on, depth, True))  # True = is_revisit
                        urls_added += 1

                self.logger.info(f"Added {len(revisit_urls)} URLs for revisit")
            except Exception as e:
                self.logger.error(f"Error loading revisit URLs: {e}")

        # 2. Add pending URLs from previous session
        try:
            state = self.storage.load_crawler_state()
            pending_urls = state.get('pending_urls', [])

            for url_data in pending_urls:
                if isinstance(url_data, (list, tuple)) and len(url_data) >= 3:
                    url, found_on, depth = url_data[0], url_data[1], url_data[2]
                    if self.url_validator.is_valid_https_url(url) and not self.storage.is_url_visited(url):
                        self.urls_to_visit.put((url, found_on, depth, False))  # False = not revisit
                        urls_added += 1
        except Exception as e:
            self.logger.error(f"Error loading pending URLs: {e}")

        # 3. Add starting URLs if needed
        if urls_added < 5:
            for url in self.starting_urls:
                if self.url_validator.is_valid_https_url(url):
                    self.urls_to_visit.put((url, "", 0, False))
                    urls_added += 1

        # 4. Find unvisited discovered URLs
        if urls_added == 0:
            unvisited_urls = self._find_unvisited_discovered_urls()
            for url_data in unvisited_urls[:50]:
                if isinstance(url_data, (list, tuple)) and len(url_data) >= 3:
                    self.urls_to_visit.put((*url_data, False))  # Add revisit flag
                    urls_added += 1

        self.logger.info(f"Queue initialized with {urls_added} URLs (including revisits)")

    def _is_valid_https_url(self, url: str) -> bool:
        """Enhanced URL validation with security checks."""
        return self.url_validator.is_valid_https_url(url)

    def _is_english_url(self, url: str) -> bool:
        """Check if URL indicates English content."""
        if not self.enable_url_language_filtering:
            return True

        try:
            # Check if URL is in allowed domains
            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            if self.allowed_domains:
                domain_allowed = any(allowed in domain for allowed in self.allowed_domains)
                if not domain_allowed:
                    return False

            # Check URL language patterns
            detected_lang = detect_language_from_url_path(url)
            return detected_lang in [None, 'en']

        except Exception as e:
            self.logger.debug(f"Error checking URL language for {url}: {e}")
            return True

    def _detect_language(self, text: str) -> str:
        """Enhanced language detection with error handling."""
        if not text or len(text.strip()) < 50:
            return 'en'  # Default to English for short content

        try:
            # Extract text content for language detection
            soup = BeautifulSoup(text, 'html.parser')
            if soup:
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                text_content = soup.get_text()
            else:
                text_content = text

            # Clean text for language detection
            text_content = re.sub(r'\s+', ' ', text_content).strip()

            if len(text_content) < 50:
                return 'en'

            # Use langdetect with confidence threshold
            detected = detect(text_content[:1000])  # Use first 1000 chars for speed
            return detected if detected else 'en'

        except Exception as e:
            self.logger.debug(f"Language detection failed: {e}")
            return 'en'

    def _normalize_url(self, url: str) -> Optional[str]:
        """Enhanced URL normalization with error handling."""
        if not url or not isinstance(url, str):
            return None

        try:
            # Parse URL
            parsed = urlparse(url.strip())

            if not parsed.scheme or not parsed.netloc:
                return None

            # Normalize components
            scheme = parsed.scheme.lower()
            netloc = parsed.netloc.lower()
            path = parsed.path.rstrip('/')

            # Remove default ports
            if ':80' in netloc and scheme == 'http':
                netloc = netloc.replace(':80', '')
            elif ':443' in netloc and scheme == 'https':
                netloc = netloc.replace(':443', '')

            # Reconstruct URL
            normalized = urlunparse((scheme, netloc, path, '', '', ''))
            return normalized

        except Exception as e:
            self.logger.debug(f"Error normalizing URL {url}: {e}")
            return None


    def _normalize_url_with_language(self, url: str) -> str:
        """
        Normalize URL by handling language path segments and .html extensions.
        Examples:
        - https://docs.snowflake.com/en/user-guide -> https://docs.snowflake.com/user-guide
        - https://docs.snowflake.com/en/user-guide.html -> https://docs.snowflake.com/user-guide
        - https://docs.snowflake.com/user-guide.html -> https://docs.snowflake.com/user-guide
        """
        if not url:
            return url
        
        try:
            parsed = urlparse(url)
            path_parts = parsed.path.strip('/').split('/')
            
            # Common language codes in URLs
            language_codes = {'en', 'fr', 'de', 'es', 'it', 'ja', 'ko', 'pt', 'zh', 'ru'}
            
            # If first path segment is a language code, remove it
            if path_parts and path_parts[0].lower() in language_codes:
                path_parts = path_parts[1:]
            
            # Remove .html extension from the last path segment
            if path_parts and path_parts[-1].lower().endswith('.html'):
                path_parts[-1] = path_parts[-1][:-5]  # Remove .html
                
            # Reconstruct URL without language code and .html
            new_path = '/' + '/'.join(path_parts)
            if new_path == '/':
                new_path = ''
                
            normalized = urlunparse((
                parsed.scheme.lower(),
                parsed.netloc.lower(),
                new_path,
                parsed.params,
                parsed.query,
                ''  # Remove fragments
            ))
            
            return normalized
            
        except Exception as e:
            logging.error(f"Error normalizing URL {url}: {e}")
            return url

    
    def _process_single_url(self, url_data: Tuple[str, str, int, bool]) -> List[Tuple[str, str, int]]:
        """Process a single URL with enhanced content extraction and storage."""
        import datetime
        
        if len(url_data) == 4:
            current_url, found_on, depth, is_revisit = url_data
        else:
            current_url, found_on, depth = url_data
            is_revisit = False

        new_urls = []

        try:
            # Check if we should stop
            if self.killer.should_stop():
                return new_urls

            # Check depth limit
            if depth > self.max_depth:
                return new_urls

            # Normalize the current URL
            normalized_url = self._normalize_url_with_language(current_url)

            # Check if normalized URL exists before processing
            if self.storage.url_exists(normalized_url):
                return new_urls

            # For revisits, skip the "already visited" check
            if not is_revisit and self.storage.is_url_visited(current_url):
                return new_urls

            # URL language check
            if not self._is_english_url(current_url):
                self.telemetry.update_url_language_filtered()
                return new_urls

            self.logger.info(f"{'Revisiting' if is_revisit else 'Crawling'} (depth {depth}): {current_url}")

            # Fetch page content
            page_data = self._fetch_page(current_url)
            if not page_data:
                return new_urls

            html_content, page_title, status_code, response_time, content_size, detected_language = page_data

            # Check content size limit
            if content_size > self.max_content_size:
                self.logger.warning(f"Content too large ({content_size} bytes), skipping: {current_url}")
                return new_urls

            # Process webpage content for storage
            content_data = self._process_webpage_content(html_content, current_url)

            # Generate enhanced content hash
            content_hash = self._get_enhanced_content_hash(html_content)

            # Add content hash to storage
            self.storage.add_content_hash(content_hash, current_url)

            # Check for content changes and duplicates
            content_changed = True
            previous_hash = ""
            is_duplicate = False

            if self.enable_content_change_detection:
                content_changed, previous_hash = self.storage.check_content_change(current_url, content_hash)
                is_duplicate, canonical_url = self.storage.check_duplicate_content(current_url, content_hash)

                if is_revisit and not content_changed:
                    self.logger.info(f"No content change detected for: {current_url}")
                    self.storage.mark_url_visited(current_url)
                    domain = urlparse(current_url).netloc
                    is_english = detected_language == 'en'
                    self.telemetry.update_page_processed(depth, response_time, status_code, domain,
                                                         content_size, is_english, is_revisit, content_changed)
                    return new_urls

                if is_duplicate:
                    self.logger.info(f"Duplicate content detected: {current_url} -> {canonical_url}")
                    self.storage.mark_url_visited(current_url)
                    domain = urlparse(current_url).netloc
                    is_english = detected_language == 'en'
                    self.telemetry.update_page_processed(depth, response_time, status_code, domain,
                                                         content_size, is_english, is_revisit, content_changed,
                                                         True)
                    return new_urls

            # Content language check
            is_english = detected_language == 'en'
            if self.enable_language_filtering and not is_english:
                return new_urls

            # Create enhanced crawl result with content
            result = CrawlResult(
                url=current_url,
                found_on=found_on,
                depth=depth,
                timestamp=datetime.datetime.now().isoformat(),
                content_hash=content_hash,
                page_title=page_title,
                status_code=status_code,
                language=detected_language,
                last_visited=datetime.datetime.now().isoformat(),
                content_changed=content_changed,
                previous_hash=previous_hash,
                raw_html=html_content[:100000] if self.store_raw_html else "",
                cleaned_text=content_data['cleaned_text'],
                content_size=content_size,
                content_type='text/html',
                extracted_links=content_data['extracted_links'],
                meta_description=content_data['meta_description'],
                meta_keywords=content_data['meta_keywords'],
                headings=content_data['headings'],
                images=content_data['images'],
                structured_data=content_data['structured_data']
            )

            # Store the enhanced result
            if hasattr(self.storage, 'insert_discovered_url_with_content'):
                # print('************Sunny insert_discovered_url_with_content**********')
                self.storage.insert_discovered_url_with_content(result)
            else:
                # Fallback to regular storage
                is_new_url, is_unique_content = self.storage.insert_discovered_url(result)
                self._update_stats(is_unique_content)

            self.storage.mark_url_visited(current_url)
            domain = urlparse(current_url).netloc
            self.telemetry.update_page_processed(depth, response_time, status_code, domain,
                                                 content_size, is_english, is_revisit, content_changed,
                                                 is_duplicate)

            # Extract URLs from page (always do this for revisits to find new links)
            found_urls = self._extract_urls_from_page(html_content, current_url)

            for url in found_urls:

                normalized_found_url = self._normalize_url_with_language(url)
                # Only add if normalized URL hasn't been seen
                if not self.storage.is_url_visited(normalized_found_url):
                    new_urls.append((normalized_found_url, current_url, depth + 1, False))
                
                # # For revisits, add all URLs regardless of visit status
                # if is_revisit or not self.storage.is_url_visited(url):
                #     new_urls.append((url, current_url, depth + 1, False))

            self.telemetry.update_urls_discovered(len(new_urls))

            if content_changed:
                self.logger.info(f"Content changed! Found {len(new_urls)} URLs on {current_url}")
            else:
                self.logger.info(f"Found {len(new_urls)} URLs on {current_url}")

            # Periodic state saving
            with self.stats_lock:
                self.pages_processed += 1
                if self.pages_processed % self.save_interval == 0:
                    self._save_state()
                    # self.logger.info('********Sunny Saved State*******')

        except Exception as e:
            print(f"Error processing {current_url}: {e}")
            self._categorize_and_log_error(e, current_url)

        return new_urls

    def _categorize_and_log_error(self, error: Exception, url: str):
        """Categorize and log errors for better debugging."""
        error_type = type(error).__name__
        is_recoverable = True

        # Categorize error types
        if isinstance(error, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
            error_category = "network"
        elif isinstance(error, requests.exceptions.HTTPError):
            error_category = "http"
            is_recoverable = error.response.status_code < 500 if hasattr(error, 'response') else True
        elif isinstance(error, (AttributeError, TypeError)):
            error_category = "parsing"
            is_recoverable = False
        else:
            error_category = "unknown"

        with self.error_lock:
            self.error_categories[error_category] += 1

        self.telemetry.update_error_count(error_category, is_recoverable)

    def _fetch_page(self, url: str) -> Optional[Tuple[str, str, int, float, int, str]]:
        """Enhanced fetch page method with better error handling."""
        session = self._get_session()

        for attempt in range(self.max_retries):
            start_time = time.time()
            response = None

            try:
                # Check if we should stop
                if self.killer.should_stop():
                    return None

                # Add random delay to avoid overwhelming servers
                if self.request_delay > 0:
                    delay = self.request_delay + (attempt * 0.1)
                    time.sleep(delay)

                response = session.get(
                    url,
                    timeout=self.timeout,
                    allow_redirects=True,
                    stream=False
                )

                if response is None:
                    self.logger.error(f"Response is None for {url}")
                    continue

                response_time = time.time() - start_time

                # Safely get content size
                content_size = len(response.content) if hasattr(response, 'content') and response.content else 0

                if response.status_code == 200:
                    # Safely get content type
                    content_type = response.headers.get('content-type', '').lower() if hasattr(response,
                                                                                               'headers') else ''

                    if 'text/html' in content_type:
                        # Check content length
                        if content_size > 10 * 1024 * 1024:  # 10MB limit
                            self.logger.warning(f"Content too large, skipping: {url}")
                            return None

                        # Safely get response text
                        response_text = response.text if hasattr(response, 'text') and response.text else ''

                        if not response_text:
                            self.logger.warning(f"Response text is empty for {url}")
                            return None

                        # Detect language
                        detected_language = self._detect_language(response_text)
                        page_title = self._get_page_title(response_text)

                        return (response_text, page_title, response.status_code,
                                response_time, content_size, detected_language)
                    else:
                        if self.debug_mode:
                            self.logger.debug(f"Skipping non-HTML content: {url}")
                        return None
                else:
                    # self.logger.warning(f"HTTP {response.status_code} for {url}")
                    if response.status_code in [404, 403, 410, 429]:
                        return None

            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout for {url} (attempt {attempt + 1})")
            except requests.exceptions.ConnectionError:
                self.logger.warning(f"Connection error for {url} (attempt {attempt + 1})")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request error for {url} (attempt {attempt + 1}): {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error for {url}: {e}")

            if attempt < self.max_retries - 1:
                time.sleep(2 ** attempt + random.uniform(0, 1))

        return None

    def _get_session(self):
        """Get thread-local session with enhanced error handling."""
        try:
            with self.session_lock:
                if not hasattr(self.session_storage, 'session') or self.session_storage.session is None:
                    self.session_storage.session = requests.Session()

                    # Add to weak set for cleanup
                    self.session_pool.add(self.session_storage.session)

                    # Set headers safely
                    self.session_storage.session.headers.update({
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Accept-Encoding': 'gzip, deflate',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1'
                    })

                return self.session_storage.session
        except Exception as e:
            self.logger.error(f"Error creating session: {e}")
            # Return a new session as fallback
            return requests.Session()

    def _get_page_title(self, html: str) -> str:
        """Extract page title from HTML with enhanced error handling."""
        try:
            if not html:
                return ""

            soup = BeautifulSoup(html, 'html.parser')
            if not soup:
                return ""

            title_tag = soup.find('title')
            if title_tag and hasattr(title_tag, 'get_text'):
                title_text = title_tag.get_text()
                if title_text:
                    return title_text.strip()[:200]
            return ""
        except Exception as e:
            self.logger.error(f"Error extracting page title: {e}")
            return ""

    def _extract_urls_from_page(self, html: str, base_url: str) -> List[str]:
        """Extract all HTTPS URLs from a page with enhanced error handling."""
        urls = []
        try:
            if not html:
                return urls

            soup = BeautifulSoup(html, 'html.parser')
            if not soup:
                return urls

            # Extract from anchor tags
            for element in soup.find_all('a', href=True):
                if not element:
                    continue

                href = element.get('href')
                if not href:
                    continue

                # Handle different types of URLs
                absolute_url = None
                if href.startswith('http'):
                    absolute_url = href
                elif href.startswith('//'):
                    absolute_url = 'https:' + href
                elif href.startswith('/'):
                    absolute_url = urljoin(base_url, href)
                elif not href.startswith(('#', 'mailto:', 'javascript:', 'tel:')):
                    absolute_url = urljoin(base_url, href)
                else:
                    continue

                if absolute_url:
                    normalized_url = self._normalize_url(absolute_url)
                    if (normalized_url and
                            self._is_valid_https_url(normalized_url) and
                            self._is_english_url(normalized_url)):
                        urls.append(normalized_url)

            # Also check link tags
            for element in soup.find_all('link', href=True):
                if not element:
                    continue

                href = element.get('href')
                if href and href.startswith('http'):
                    normalized_url = self._normalize_url(href)
                    if (normalized_url and
                            self._is_valid_https_url(normalized_url) and
                            self._is_english_url(normalized_url)):
                        urls.append(normalized_url)

        except Exception as e:
            self.logger.error(f"Error extracting URLs from {base_url}: {e}")

        return list(set(urls))  # Remove duplicates

    def _update_stats(self, is_new: bool):
        """Thread-safe statistics update."""
        with self.stats_lock:
            if is_new:
                self.new_urls_found += 1
                self.pages_since_last_new = 0
            else:
                self.pages_since_last_new += 1

    def _worker_thread(self):
        """Enhanced worker thread with better error handling and resource management."""
        thread_name = threading.current_thread().name
        self.logger.info(f"Worker thread {thread_name} started")

        try:
            while self.running and not self.killer.should_stop():
                try:
                    # Get URL from queue with timeout
                    url_data = self.urls_to_visit.get(timeout=self.queue_timeout)

                    # Check for poison pill
                    if url_data is None:
                        break

                    # Update queue size for telemetry
                    self.telemetry.update_queue_size(self.urls_to_visit.qsize())

                    # Process the URL
                    new_urls = self._process_single_url(url_data)

                    # Add new URLs to queue
                    for new_url_data in new_urls:
                        if not self.killer.should_stop():
                            self.urls_to_visit.put(new_url_data)

                    # Check for diminishing returns
                    with self.stats_lock:
                        if (self.pages_since_last_new > self.diminishing_returns_pages and
                                self.new_urls_found < self.diminishing_returns_threshold):
                            self.logger.info(f"Diminishing returns detected in {thread_name}")
                            break

                    # Memory check
                    if self.memory_monitor.is_memory_limit_exceeded():
                        print(f"Memory limit exceeded in {thread_name}")
                        break

                except Empty:
                    # Queue is empty, check if we should continue
                    if self.urls_to_visit.empty():
                        self.logger.info(f"Queue empty in {thread_name}, checking for more work...")
                        time.sleep(1)
                        if self.urls_to_visit.empty():
                            break
                    continue

                except Exception as e:
                    self.logger.error(f"Worker thread {thread_name} error: {e}")
                    with self.error_lock:
                        self.error_categories["worker"] += 1
                    time.sleep(1)

        except Exception as e:
            self.logger.error(f"Fatal error in worker thread {thread_name}: {e}")
        finally:
            self.logger.info(f"Worker thread {thread_name} finished")

    def _find_unvisited_discovered_urls(self, limit: int = 100) -> List[Tuple[str, str, int]]:
        """Find unvisited discovered URLs for incremental crawling."""
        try:
            if hasattr(self.storage, 'get_unvisited_discovered_urls'):
                return self.storage.get_unvisited_discovered_urls(limit)
            else:
                # Fallback for basic storage
                return []
        except Exception as e:
            self.logger.error(f"Error finding unvisited URLs: {e}")
            return []

    def _load_state(self):
        """Load previous crawler state."""
        try:
            state = self.storage.load_crawler_state()
            if state:
                self.logger.info("Previous crawler state loaded successfully")
            else:
                self.logger.info("No previous state found, starting fresh")
        except Exception as e:
            self.logger.error(f"Error loading state: {e}")

    def _save_state(self):
        """Save current crawler state."""
        import datetime
        
        try:
            # Collect pending URLs from queue
            pending_urls = []
            temp_queue = Queue()

            while not self.urls_to_visit.empty():
                try:
                    url_data = self.urls_to_visit.get_nowait()
                    pending_urls.append(url_data)
                    temp_queue.put(url_data)
                except Empty:
                    break

            # Restore queue
            while not temp_queue.empty():
                try:
                    self.urls_to_visit.put(temp_queue.get_nowait())
                except Empty:
                    break

            state_data = {
                'pending_urls': pending_urls,
                'pages_processed': self.pages_processed,
                'new_urls_found': self.new_urls_found,
                'error_categories': dict(self.error_categories),
                'timestamp': datetime.datetime.now().isoformat()
            }

            self.storage.save_crawler_state(state_data)
            self.logger.info("Crawler state saved")

        except Exception as e:
            print(f"Error saving state: {e}")
            # sys.exit(0)

    def stop(self):
        """Stop the crawler gracefully."""
        self.logger.info("Stopping crawler gracefully...")
        self.killer.stop()
        self.running = False

    def crawl(self):
        """Enhanced crawl method with comprehensive content storage."""
        storage_type = "Snowflake database"
        self.logger.info(f"Starting enhanced incremental multi-threaded crawl with {self.max_workers} workers...")
        self.logger.info(f"Storage mode: {storage_type}")
        self.logger.info(
            f"Content storage: {'enabled' if self.store_raw_html or self.store_cleaned_text else 'disabled'}")

        self.logger.info(
            f"Content change detection: {'enabled' if self.enable_content_change_detection else 'disabled'}")
        self.logger.info(f"Revisit interval: {self.revisit_interval_hours} hours")
        self.logger.info(
            f"URL language filtering: {'enabled' if self.enable_url_language_filtering else 'disabled'}")
        self.logger.info(
            f"Content language filtering: {'enabled' if self.enable_language_filtering else 'disabled'}")
        self.logger.info("Press Ctrl+C to stop gracefully")

        # Test connection
        # if not test_snowflake_connection():
        #     self.logger.error("Failed to connect to Snowflake. Aborting.")
        #     return

        self.start_time = time.time()
        self._last_save = self.start_time
        initial_count = self.storage.get_discovered_urls_count()

        # Log incremental mode status
        initial_discovered = self.storage.get_discovered_urls_count()
        initial_visited = self.storage.get_visited_urls_count()

        if initial_discovered > 0:
            self.logger.info(f"INCREMENTAL MODE: Resuming with {initial_discovered} discovered URLs")
            self.logger.info(f"INCREMENTAL MODE: {initial_visited} URLs already visited")
            self.logger.info(f"INCREMENTAL MODE: {initial_discovered - initial_visited} URLs pending")
        else:
            self.logger.info("FRESH START: No previous data found")

        # Start telemetry
        self.telemetry.start_telemetry()

        # Create and start worker threads
        threads = []
        for i in range(self.max_workers):
            thread = threading.Thread(target=self._worker_thread, name=f"Worker-{i + 1}")
            thread.daemon = True
            thread.start()
            threads.append(thread)

        # Update active thread count
        self.telemetry.update_active_threads(len(threads))

        try:
            while self.running and not self.killer.should_stop():
                # Periodic state saves during main loop
                current_time = time.time()
                if current_time - self._last_save > 600:  # Save state every minute
                    self._save_state()
                    self._last_save = current_time

                # Check if queue is empty and no threads are working
                if self.urls_to_visit.empty():
                    # Wait a bit to see if new URLs are added
                    time.sleep(2)
                    if self.urls_to_visit.empty():
                        self.logger.info("No more URLs to process. Stopping crawl.")
                        break
                else:
                    time.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("Crawl interrupted by user (Ctrl+C)")
            self.killer.stop()
        finally:
            # Signal threads to stop
            self.running = False

            # Add poison pills to wake up threads
            for _ in range(self.max_workers):
                try:
                    self.urls_to_visit.put(None, timeout=1)
                except:
                    pass

            # Wait for threads to finish
            for thread in threads:
                thread.join(timeout=5)

            # Stop telemetry and show final report
            self.telemetry.stop_telemetry()
            print("\n" + "=" * 80)
            print("FINAL TELEMETRY REPORT")
            print("=" * 80)
            print(self.telemetry.get_final_report())
            print("=" * 80)

            # Final save
            self._save_state()

            # Export to CSV if requested
            print('********************Loading Tables in Snowflake*************')
            if self.export_csv:
                # self.storage.export_to_csv(csv_filename)
                upload_csv_to_snowflake({},
                                        self.table_prefix
                                        )

            # Close storage if needed
            if hasattr(self.storage, 'close'):
                self.storage.close()

            # Cleanup sessions
            self._cleanup_sessions()

            # Show enhanced final statistics
            self._show_final_statistics(initial_count, storage_type)

    def _cleanup_sessions(self):
        """Clean up HTTP sessions."""
        try:
            for session in self.session_pool:
                if session:
                    session.close()
            self.logger.info("HTTP sessions cleaned up")
        except Exception as e:
            self.logger.error(f"Error cleaning up sessions: {e}")

    def _show_final_statistics(self, initial_count: int, storage_type: str):
        """Show comprehensive final statistics."""
        final_count = self.storage.get_discovered_urls_count()
        new_urls_this_run = final_count - initial_count

        print("\n" + "=" * 60)
        print(f"FINAL STATISTICS ({storage_type})")
        print("=" * 60)
        print(f"Total discovered URLs: {final_count:,}")
        print(f"New URLs this run: {new_urls_this_run:,}")

        # Show statistics
        stats = self.storage.get_statistics()
        if stats:
            print(f"Total visited URLs: {stats.get('total_visited', 0):,}")

            lang_dist = stats.get('language_distribution', {})
            if lang_dist:
                print("\nLanguage distribution:")
                for lang, count in list(lang_dist.items())[:10]:
                    print(f"  {lang}: {count:,}")

            depth_dist = stats.get('depth_distribution', {})
            if depth_dist:
                print("\nDepth distribution:")
                for depth, count in depth_dist.items():
                    print(f"  Depth {depth}: {count:,}")

        # Show error statistics
        if self.error_categories:
            print("\nError categories:")
            for category, count in self.error_categories.items():
                print(f"  {category}: {count:,}")

        # Show content storage statistics if available
        if hasattr(self.storage, 'get_content_statistics'):
            content_stats = self.storage.get_content_statistics()
            if content_stats:
                print("\nContent storage statistics:")
                if 'content_size' in content_stats:
                    size_info = content_stats['content_size']
                    print(f"  Average content size: {size_info.get('average', 0):.0f} bytes")
                    print(f"  Total pages with content: {size_info.get('total_pages', 0):,}")
                if 'meta_description_coverage' in content_stats:
                    print(f"  Meta description coverage: {content_stats['meta_description_coverage']:.1f}%")

        print("=" * 60)


def main():
    """Enhanced main function with database-only configuration."""

    print("🕷️  Enhanced Incremental Web Crawler - Database Mode")
    print("=" * 75)
    print("Starting with predefined configuration...")
    print("=" * 75)

    # ===== ENHANCED CONFIGURATION SECTION =====
    # Modify these settings directly in the code
    # private_key_path = get_private_key()
    # Snowflake Configuration (required for database mode)
    # SNOWFLAKE_CONFIG = {
    #     'user': os.environ['SNOWFLAKE_USER'],
    #     'host': os.environ['HOST'],
    #     'port': '443',
    #     'account': os.environ['SNOWFLAKE_ACCOUNT'],
    #     'authenticator': 'externalbrowser',
    #     'warehouse': os.environ['SNOWFLAKE_WAREHOUSE'],  # Optional
    #     'database': os.environ['SNOWFLAKE_DATABASE'],
    #     'schema': os.environ['SNOWFLAKE_SCHEMA'],
    #     'session_parameters': {'ABORT_DETACHED_QUERY': 'TRUE'}
    # }

    SNOWFLAKE_CONFIG = {}

    # Crawling Targets
    STARTING_URLS = [
        'https://docs.snowflake.com/en/',
        'https://quickstarts.snowflake.com/en/',
        'https://docs.snowflake.com/en/sitemap.xml',
        # Add more starting URLs here
    ]

    ALLOWED_DOMAINS = [
        'snowflake.com',
        # 'quickstarts.snowflake.com',
        # Add more allowed domains here
    ]

    # Crawling Parameters
    MAX_DEPTH = 12
    MAX_WORKERS = 40
    REQUEST_DELAY = 1  # seconds between requests

    # Enhanced Content Storage Configuration
    STORE_RAW_HTML = False  # Store full HTML content
    STORE_CLEANED_TEXT = False  # Store cleaned text content
    STORE_EXTRACTED_DATA = False  # Store structured data
    MAX_CONTENT_SIZE = 5000000  # 5MB limit per page
    EXTRACT_METADATA = False  # Extract meta tags
    EXTRACT_HEADINGS = False  # Extract heading structure
    EXTRACT_IMAGES = False  # Extract image information
    EXTRACT_LINKS = True  # Extract all links
    EXTRACT_STRUCTURED_DATA = False  # Extract JSON-LD and microdata

    # Incremental Features
    ENABLE_CONTENT_CHANGE_DETECTION = True
    REVISIT_INTERVAL_HOURS = 24 * 7
    ENABLE_LANGUAGE_FILTERING = True
    ENABLE_URL_LANGUAGE_FILTERING = True

    # Max runtime as failsafe (Can be configured for one-time vs incremental)
    MAX_RUNTIME_HOURS = 4

    # File Configuration
    TABLE_PREFIX = 'CRAWLER'

    # Debug Mode
    DEBUG_MODE = False

    USE_DATABASE = False

    # ===== END CONFIGURATION SECTION =====

    # Build enhanced configuration dictionary
    config = {
        # Snowflake configuration
        'snowflake_config': SNOWFLAKE_CONFIG,
        'table_prefix': TABLE_PREFIX,
        'max_cache_size': 10000,

        # Storage configuration
        'use_database': USE_DATABASE,  # Set to False for CSV storage

        # CSV Storage configuration (only used if use_database=False)
        'output_dir': './crawler_data',
        'urls_file': 'discovered_urls.csv',
        'state_file': 'crawler_state.json',
        'content_dir': 'page_content',
        'compress_content': False,

        # Crawling targets
        'starting_urls': STARTING_URLS,
        'allowed_domains': ALLOWED_DOMAINS,

        # Crawling parameters
        'max_depth': MAX_DEPTH,
        'max_workers': MAX_WORKERS,
        'request_delay': REQUEST_DELAY,
        'timeout': 15,
        'max_retries': 3,
        'queue_timeout': 10,

        # Time-based kill switch
        'max_runtime_hours': MAX_RUNTIME_HOURS,

        # Enhanced content storage options
        'store_raw_html': STORE_RAW_HTML,
        'store_cleaned_text': STORE_CLEANED_TEXT,
        'store_extracted_data': STORE_EXTRACTED_DATA,
        'max_content_size': MAX_CONTENT_SIZE,
        'extract_metadata': EXTRACT_METADATA,
        'extract_headings': EXTRACT_HEADINGS,
        'extract_images': EXTRACT_IMAGES,
        'extract_links': EXTRACT_LINKS,
        'extract_structured_data': EXTRACT_STRUCTURED_DATA,

        # Incremental crawling features
        'enable_content_change_detection': ENABLE_CONTENT_CHANGE_DETECTION,
        'revisit_interval_hours': REVISIT_INTERVAL_HOURS,
        'max_revisit_urls_per_run': 50,
        'content_change_threshold': 0.1,
        'force_revisit_depth': 1,

        # Language filtering
        'enable_language_filtering': ENABLE_LANGUAGE_FILTERING,
        'enable_url_language_filtering': ENABLE_URL_LANGUAGE_FILTERING,

        # Performance and monitoring
        'save_interval': 100,
        'telemetry_interval': 360,
        'diminishing_returns_threshold': 10,
        'diminishing_returns_pages': 50,

        # Output options
        'export_csv': True,
        'debug_mode': DEBUG_MODE,
    }

    # Enhanced configuration validation
    validation_errors = []

    if not config['starting_urls']:
        validation_errors.append("No starting URLs specified")

    if not config['allowed_domains']:
        validation_errors.append("No allowed domains specified")

    # Check for valid URLs
    for url in config['starting_urls']:
        if not (url.startswith('http://') or url.startswith('https://')):
            validation_errors.append(f"Invalid URL format: {url}")

    # Validate content storage configuration
    if config['store_raw_html'] and config['max_content_size'] < 10000:
        validation_errors.append("max_content_size too small for raw HTML storage")

    # if not config['snowflake_config']:
    #     validation_errors.append("Snowflake configuration required for database mode")

    if validation_errors:
        print("\n❌ Configuration Validation Errors:")
        for error in validation_errors:
            print(f"  • {error}")
        print("\nPlease fix the configuration in the main() function.")
        return

    # Display enhanced configuration summary
    print("\n" + "=" * 70)
    print("🚀 ENHANCED CRAWLER CONFIGURATION SUMMARY")
    print("=" * 70)
    print(f"📊 Storage Mode: Snowflake Database")
    print(
        f"📁 Content Storage: {'Enabled' if config['store_raw_html'] or config['store_cleaned_text'] else 'Disabled'}")
    print(f"🔍 Raw HTML Storage: {'Enabled' if config['store_raw_html'] else 'Disabled'}")
    print(f"📝 Cleaned Text Storage: {'Enabled' if config['store_cleaned_text'] else 'Disabled'}")
    print(f"🏷️  Metadata Extraction: {'Enabled' if config['extract_metadata'] else 'Disabled'}")
    print(f"📊 Structured Data: {'Enabled' if config['extract_structured_data'] else 'Disabled'}")
    print(f"🖼️  Image Extraction: {'Enabled' if config['extract_images'] else 'Disabled'}")
    print(f"🔗 Link Extraction: {'Enabled' if config['extract_links'] else 'Disabled'}")
    print(f"📏 Max Content Size: {config['max_content_size']:,} bytes")
    print(f"🔄 Content Change Detection: {'Enabled' if config['enable_content_change_detection'] else 'Disabled'}")
    print(f"⏰ Revisit Interval: {config['revisit_interval_hours']} hours")
    print(f"🌐 Starting URLs: {len(config['starting_urls'])}")
    print(f"🏠 Allowed Domains: {len(config['allowed_domains'])}")
    print(f"🕳️  Max Depth: {config['max_depth']}")
    print(f"👥 Workers: {config['max_workers']}")
    print(f"⏱️  Request Delay: {config['request_delay']}s")
    print(f"🐛 Debug Mode: {'Enabled' if config['debug_mode'] else 'Disabled'}")

    print(f"❄️  Snowflake Account: {config['snowflake_config'].get('account', 'Not specified')}")
    print(f"🗄️  Database: {config['snowflake_config'].get('database', 'Not specified')}")
    print(f"📋 Schema: {config['snowflake_config'].get('schema', 'Not specified')}")
    print(f"🏢 Warehouse: {config['snowflake_config'].get('warehouse', 'Default')}")

    print("=" * 70)

    # Initialize and run enhanced crawler
    crawler = None
    try:
        print("\n🔄 Initializing enhanced crawler with content storage...")
        crawler = ThreadSafeCrawler(config)

        print("🕷️  Starting enhanced crawl with content extraction...")
        print("📊 Content will be stored with the following features:")
        if config['store_raw_html']:
            print("  • Raw HTML content")
        if config['store_cleaned_text']:
            print("  • Cleaned text content")
        if config['extract_metadata']:
            print("  • Meta tags (description, keywords)")
        if config['extract_headings']:
            print("  • Heading structure (H1-H6)")
        if config['extract_images']:
            print("  • Image information and metadata")
        if config['extract_links']:
            print("  • All extracted links")
        if config['extract_structured_data']:
            print("  • Structured data (JSON-LD, microdata)")

        crawler.crawl()

    except KeyboardInterrupt:
        print("\n\n⚠️  Received Ctrl+C, stopping crawler gracefully...")
        if crawler:
            crawler.stop()

        print("✅ Enhanced crawler stopped gracefully.")

    except Exception as e:
        print(f"\n❌ Unexpected error occurred: {e}")
        logging.error(f"Fatal error in main: {e}")
        import traceback
        if config.get('debug_mode'):
            traceback.print_exc()
        else:
            print("Set DEBUG_MODE=True for detailed error information.")

    finally:
        if crawler:
            print("\n🧹 Cleaning up enhanced crawler resources...")
            try:
                if hasattr(crawler, 'storage') and hasattr(crawler.storage, 'close'):
                    crawler.storage.close()
            except Exception as e:
                logging.error(f"Error during cleanup: {e}")

            if config.get('export_csv'):
                # self.storage.export_to_csv(csv_filename)
                upload_csv_to_snowflake({},'crawler')

        print("\n✅ Enhanced crawler session completed.")
        print("📊 Check your Snowflake database for enhanced content results.")

        if config['store_raw_html'] or config['store_cleaned_text']:
            print("🎉 Content storage features were enabled - check your database for rich content data!")
