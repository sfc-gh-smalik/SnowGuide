import asyncio
import aiohttp
import time
import random
import ssl
import certifi
import re
from urllib.parse import urlparse, urljoin, parse_qs
from html.parser import HTMLParser
from langdetect import detect
from bs4 import BeautifulSoup
from snowflake.snowpark.context import get_active_session
import pandas as pd
from datetime import datetime


class URLNode:
    def __init__(self, url, parent=None, depth=0):
        self.url = url
        self.parent = parent
        self.children = set()
        self.depth = depth

    def add_child(self, url):
        child = URLNode(url, parent=self, depth=self.depth + 1)
        self.children.add(child)
        return child

    def is_ancestor(self, url):
        """Check if URL exists in the path from root to this node"""
        current = self
        while current:
            if current.url == url:
                return True
            current = current.parent
        return False

    def get_path(self):
        """Get the full path from root to this node"""
        path = []
        current = self
        while current:
            path.append(current.url)
            current = current.parent
        return list(reversed(path))


def extract_article_links(html, base_url):
    """Extract links only from article tags in the webpage"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        links = set()

        # Find all article tags
        articles = soup.find_all('article')

        if articles:
            # Extract links from all article tags
            for article in articles:
                for a_tag in article.find_all('a', href=True):
                    href = a_tag.get('href')
                    if href and not href.startswith('#') and not href.startswith('javascript:'):
                        absolute = urljoin(base_url, href)
                        # Remove fragments
                        url_parts = absolute.split('#')
                        links.add(url_parts[0])

            # print(f"Extracted {len(links)} links from article tags")
        else:
            # print(f"No article tags found on {base_url}")
            None

        return list(links)
    except Exception as e:
        print(f"Error extracting article links from {base_url}: {e}")
        return []


class LinkExtractor(HTMLParser):
    def __init__(self, base_url):
        super().__init__()
        self.links = set()
        self.base_url = base_url

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href':
                    if value:
                        absolute = urljoin(self.base_url, value)
                        # Remove fragments
                        url_parts = absolute.split('#')
                        self.links.add(url_parts[0])


class URLManager:
    def __init__(self, base_domain, table_name):
        self.session = get_active_session()
        self.base_domain = base_domain
        self.known_urls = set()
        self.new_urls = {}
        self.queue = asyncio.Queue()
        self.processed_urls = set()
        self.in_queue_urls = set()
        self.discovered_subdomains = set()
        self.last_new_url_time = time.time()
        self.url_tree = {}
        self.root_nodes = []
        self.max_depth = 20
        self.max_urls_to_crawl = 50000
        
        # Create tables if they don't exist
        self._create_tables(table_name)
        self.load_existing_urls(table_name)

    def _create_tables(self,table_name):
        # Create table for storing URLs and their metadata
        self.session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                URL STRING PRIMARY KEY,
                PARENT_URL STRING,
                PATH STRING,
                DISCOVERED_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
        return

    def load_existing_urls(self, table_name):
        # Load existing URLs from Snowflake table
        existing_urls = self.session.sql(f"""
            SELECT URL, PARENT_URL, PATH 
            FROM {table_name}
        """).collect()
        
        for row in existing_urls:
            url, parent_url, path_str = row
            
            # Store URL in known set
            self.known_urls.add(url)
            
            # Extract subdomain
            parsed = urlparse(url)
            if self.base_domain in parsed.netloc:
                self.discovered_subdomains.add(parsed.netloc)
            
            # Reconstruct path if available
            if path_str:
                path = path_str.split(" -> ")
                self.reconstruct_path_in_tree(path)
        
        # Queue existing URLs for re-checking
        self.queue_existing_paths()

    
    def queue_existing_paths(self):
        """Queue up existing paths to re-check for new URLs"""
        # Create known_paths dictionary from the URL tree
        known_paths = {}
        for url, node in self.url_tree.items():
            known_paths[url] = node.get_path()
    
        # print(f"Queueing {len(known_paths)} existing paths for re-checking")
    
        # Group URLs by their path length to prioritize shorter paths
        paths_by_length = {}
        for url, path in known_paths.items():
            path_len = len(path)
            if path_len not in paths_by_length:
                paths_by_length[path_len] = []
            paths_by_length[path_len].append((url, path))
    
        # Queue paths from shortest to longest
        for length in sorted(paths_by_length.keys()):
            for url, path in paths_by_length[length]:
                # Reconstruct the tree for this path
                self.reconstruct_path_in_tree(path)
    
                # Add the URL to the queue for re-checking
                # We'll mark it as already known so we don't re-add it to CSV
                self.processed_urls.add(url)  # Mark as already processed
    
                # Find the depth of this URL in its path
                depth = len(path) - 1
    
                # Queue this URL for re-checking links
                asyncio.create_task(self.queue.put((url, depth)))
                # print(f"Queued existing URL for re-check: {url} (depth {depth})")


    def reconstruct_path_in_tree(self, path):
        """Reconstruct a path in the URL tree"""
        if not path:
            return

        # Create nodes for each URL in the path
        parent = None
        parent_url = None

        for url in path:
            if url in self.url_tree:
                # Node already exists
                node = self.url_tree[url]
                parent = node
                parent_url = url
                continue

            if parent is None:
                # This is a root node
                node = URLNode(url)
                self.url_tree[url] = node
                self.root_nodes.append(node)
            else:
                # This is a child node
                node = parent.add_child(url)
                self.url_tree[url] = node

            parent = node
            parent_url = url

    async def add_url(self, url, parent_url=None):
        # Skip if we've reached the maximum URLs
        if len(self.processed_urls) >= self.max_urls_to_crawl:
            return

        # Skip URLs with language indicators for non-English content
        if self.is_likely_non_english_url(url):
            # print(f"Skipping likely non-English URL: {url}")
            return

        # Check if URL belongs to our target domain or subdomains
        parsed_url = urlparse(url)
        if not self.is_part_of_domain(parsed_url.netloc):
            # print(f"Skipping out-of-scope URL: {url}")
            return

        # Skip URLs that follow patterns indicating potential loops
        if self.is_likely_pattern_url(url):
            # print(f"Skipping pattern URL that might cause loops: {url}")
            return

        # Process both HTTP and HTTPS for crawling, but only log HTTPS
        normalized_url = self.normalize_url(url)

        # Track new subdomains
        if parsed_url.netloc not in self.discovered_subdomains and self.base_domain in parsed_url.netloc:
            self.discovered_subdomains.add(parsed_url.netloc)
            print(f"Discovered new subdomain: {parsed_url.netloc}")

        # Check if this URL would create a cycle in the current path
        if parent_url and parent_url in self.url_tree:
            parent_node = self.url_tree[parent_url]
            if parent_node.is_ancestor(url):
                # print(f"Skipping URL that would create cycle: {url}")
                return

            # Check depth limit
            if parent_node.depth + 1 > self.max_depth:
                print(f"Skipping URL exceeding max depth: {url}")
                return

        # Check if this is a new URL we haven't seen before
        is_new_url = (url not in self.known_urls and
                      url not in self.processed_urls and
                      url not in self.in_queue_urls)

        if is_new_url:
            self.in_queue_urls.add(url)

            # Add to tree
            if parent_url and parent_url in self.url_tree:
                parent_node = self.url_tree[parent_url]
                node = parent_node.add_child(url)
                self.url_tree[url] = node
                depth = node.depth
            else:
                node = URLNode(url)
                self.url_tree[url] = node
                self.root_nodes.append(node)
                depth = 0

            # Update the last new URL time
            self.last_new_url_time = time.time()

            # Only add to new_urls if it's HTTPS and truly new
            if url.startswith('https://'):
                path = node.get_path()
                path_str = " -> ".join(path)
                self.new_urls[url] = (parent_url, path_str)
                # print(f"Found new HTTPS URL: {url} (from {parent_url})")
            else:
                print(f"Found new HTTP URL: {url} (will crawl but not log)")

            await self.queue.put((url, depth))
        elif url not in self.processed_urls:
            # URL is known but not yet processed in this run
            # We should still crawl it to find potential new links
            self.in_queue_urls.add(url)

            # Determine depth from tree if possible
            if url in self.url_tree:
                depth = self.url_tree[url].depth
            else:
                depth = 0

            await self.queue.put((url, depth))
            # print(f"Re-queuing known URL: {url} (depth {depth})")

    def normalize_url(self, url):
        """Normalize URL to better detect duplicates"""
        parsed = urlparse(url)

        # Convert to lowercase
        netloc = parsed.netloc.lower()
        path = parsed.path.lower()

        # Remove trailing slash from path
        if path.endswith('/') and len(path) > 1:
            path = path[:-1]

        # Sort query parameters
        if parsed.query:
            query_params = parse_qs(parsed.query)
            # Remove certain tracking parameters
            for param in ['utm_source', 'utm_medium', 'utm_campaign', 'fbclid', 'gclid']:
                if param in query_params:
                    del query_params[param]

            # Reconstruct query string with sorted parameters
            query_items = []
            for key in sorted(query_params.keys()):
                for value in sorted(query_params[key]):
                    query_items.append(f"{key}={value}")
            query_string = "&".join(query_items)
        else:
            query_string = ""

        # Reconstruct normalized URL
        scheme = 'https'  # Always use HTTPS for comparison
        normalized = f"{scheme}://{netloc}{path}"
        if query_string:
            normalized += f"?{query_string}"

        return normalized

    def mark_processed(self, url):
        """Mark a URL as processed and remove from in-queue tracking"""
        self.processed_urls.add(url)
        if url in self.in_queue_urls:
            self.in_queue_urls.remove(url)

    def is_part_of_domain(self, netloc):
        """Check if the netloc is part of our target domain or its subdomains"""
        return netloc == self.base_domain or netloc.endswith(f".{self.base_domain}")

    def is_likely_pattern_url(self, url):
        """Detect URLs that follow patterns indicating potential loops"""
        parsed = urlparse(url)
        path = parsed.path

        # Check for calendar/date patterns
        date_patterns = [
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'date=\d{4}-\d{2}-\d{2}'  # date=YYYY-MM-DD
        ]

        for pattern in date_patterns:
            if re.search(pattern, url):
                return True

        # Check for pagination patterns
        if re.search(r'page=\d+', url) or re.search(r'/page/\d+', url):
            # Extract page number
            match = re.search(r'page=(\d+)', url) or re.search(r'/page/(\d+)', url)
            if match and int(match.group(1)) > 5:  # Only crawl first 5 pages
                return True

        # Check for repeating path segments
        path_parts = [p for p in path.split('/') if p]
        if len(path_parts) > 3:
            # Check for repeating patterns in path
            for i in range(len(path_parts) - 2):
                if path_parts[i] == path_parts[i + 2]:
                    return True

        return False

    def is_likely_non_english_url(self, url):
        """Check if URL likely points to non-English content based on patterns"""
        url_lower = url.lower()
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        query = parse_qs(parsed_url.query)

        # Check for language codes in URL path
        non_english_indicators = [
            '/de/', '/fr/', '/es/', '/it/', '/ru/', '/zh/', '/ja/', '/ko/',
            '/ar/', '/pt/', '/nl/', '/sv/', '/fi/', '/da/', '/no/', '/pl/',
            '/tr/', '/cs/', '/hu/', '/el/', '/he/', '/th/'
        ]

        for indicator in non_english_indicators:
            if indicator in path:
                return True

        # Check for language parameters in query string
        if 'lang' in query and query['lang'][0].lower() not in ['en', 'en-us', 'en-gb']:
            return True
        if 'language' in query and query['language'][0].lower() not in ['en', 'en-us', 'en-gb']:
            return True

        # Check for language subdomains
        subdomain = parsed_url.netloc.split('.')[0].lower()
        if subdomain in ['de', 'fr', 'es', 'it', 'ru', 'zh', 'ja', 'ko', 'ar', 'pt']:
            return True

        return False

    def save_new_urls(self):
        if not self.new_urls:
            print("No new URLs to save")
            return
            
        # Convert new URLs to DataFrame
        data = []
        for url, (parent_url, path) in self.new_urls.items():
            if url.startswith('https://'):
                data.append({
                    'URL': url,
                    'PARENT_URL': parent_url,
                    'PATH': path,
                    'DISCOVERED_TIMESTAMP': datetime.utcnow()
                })
        
        if data:
            # Create DataFrame and save to Snowflake
            df = pd.DataFrame(data)
            self.session.create_dataframe(df).write.mode("append").save_as_table("TEMP.SMALIK.SNOWFLAKE_URLS")
            
            # Update known URLs with newly found ones
            self.known_urls.update(url for url in self.new_urls.keys() if url.startswith('https://'))
            print(f"Saved {len(data)} new HTTPS URLs")
        
        self.new_urls.clear()


async def extract_suggested_url(html_content):
    """Extract suggested URL from 404 error page"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for common patterns in 404 pages that suggest alternative URLs
        # This could be links with specific classes or text patterns
        possible_suggestions = []
        
        # Look for links containing "suggested" or "redirect" in text or class
        for link in soup.find_all('a'):
            link_text = link.get_text().lower()
            link_class = ' '.join(link.get('class', [])).lower()
            
            if any(word in link_text or word in link_class 
                  for word in ['suggested', 'redirect', 'try this', 'moved to']):
                href = link.get('href')
                if href:
                    possible_suggestions.append(href)
                    
        # Look for text patterns like "The page has moved to..."
        move_patterns = [
            r'moved (?:permanently )?to ["\']?(https?://[^"\'<>\s]+)',
            r'redirected to ["\']?(https?://[^"\'<>\s]+)',
            r'available at ["\']?(https?://[^"\'<>\s]+)',
            r'you mean ["\']?(https?://[^"\'<>\s]+)'
        ]
        
        for pattern in move_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            possible_suggestions.extend(matches)
            
        return possible_suggestions[0] if possible_suggestions else None
        
    except Exception as e:
        print(f"Error extracting suggested URL: {e}")
        return None



async def discover_subdomains(domain):
    """Discover subdomains using common patterns"""
    subdomains = set()

    # Add the main domain
    subdomains.add(domain)

    # Common subdomains to check
    common_subdomains = [
        'www', 'quickstarts', 'community', 'developers', 'status', 'docs'
    ]

    for subdomain in common_subdomains:
        hostname = f"{subdomain}.{domain}"
        subdomains.add(hostname)

    return subdomains


def extract_text_from_html(html_content):
    """Extract readable text from HTML content"""
    # Remove script and style elements
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL)

    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html_content)

    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def is_english_content(html_content):
    """Detect if the content is in English"""
    try:
        # Extract text content from HTML
        text = extract_text_from_html(html_content)

        # Get a sample of text for language detection
        sample = text[:4000]  # Use first 4000 chars for detection

        if not sample or len(sample.strip()) < 100:
            return True  # Not enough text to detect, assume English

        # Detect language
        language = detect(sample)
        return language == 'en'
    except Exception as e:
        # If detection fails, assume it's English to avoid false negatives
        return True


async def fetch_with_retry(session, url, max_retries=3, backoff_factor=2):
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    for attempt in range(max_retries):
        try:
            async with session.get(url, timeout=5, ssl=ssl_context) as response:
                content = await response.text()
                
                if response.status == 200:
                    return content
                elif response.status == 404:
                    # Try to extract suggested URL from 404 page
                    suggested_url = await extract_suggested_url(content)
                    if suggested_url:
                        print(f"Found suggested URL for {url}: {suggested_url}")
                        # Return special marker to indicate redirect
                        return {'redirect_to': suggested_url}
                elif response.status == 429:  # Too many requests
                    wait_time = backoff_factor ** attempt
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    print(f"Failed with status {response.status}: {url}")
                    return None
                    
        except (aiohttp.ClientError, asyncio.TimeoutError, ssl.SSLError) as e:
            if attempt == max_retries - 1:
                print(f"Failed to fetch {url}: {e}")
                return None
            await asyncio.sleep(backoff_factor ** attempt)
    return None



async def worker(session, url_manager, stop_event):
    while not stop_event.is_set():
        try:
            url_info = await asyncio.wait_for(url_manager.queue.get(), timeout=5)
            url, depth = url_info
        except asyncio.TimeoutError:
            if url_manager.queue.empty():
                print("Queue empty, worker exiting")
                break
            continue

        try:
            await asyncio.sleep(random.uniform(0.5, 2.0))

            if any(ext in url.lower() for ext in ['.pdf', '.jpg', '.png', '.gif', '.zip']):
                url_manager.mark_processed(url)
                url_manager.queue.task_done()
                continue

            result = await fetch_with_retry(session, url)

            if isinstance(result, dict) and 'redirect_to' in result:
                # Handle suggested URL from 404 page
                suggested_url = result['redirect_to']
                await url_manager.add_url(suggested_url, parent_url=url)
                print(f"Added suggested URL to queue: {suggested_url}")
            elif result:
                # Process normal page content
                if not is_english_content(result):
                    url_manager.mark_processed(url)
                    url_manager.queue.task_done()
                    continue

                links = extract_article_links(result, url)
                links = [link for link in links if url_manager.is_part_of_domain(urlparse(link).netloc)]

                for link in links:
                    await url_manager.add_url(link, parent_url=url)

            url_manager.mark_processed(url)

            current_time = time.time()
            if current_time - url_manager.last_new_url_time > 600:
                print("No new URL added in last 600 seconds, stopping crawl.")
                stop_event.set()
                while not url_manager.queue.empty():
                    try:
                        _ = url_manager.queue.get_nowait()
                        url_manager.queue.task_done()
                    except asyncio.QueueEmpty:
                        break
                break

            if len(url_manager.new_urls) >= 10:
                url_manager.save_new_urls()

            url_manager.queue.task_done()

        except Exception as e:
            print(f"Error processing {url}: {e}")
            url_manager.mark_processed(url)
            url_manager.queue.task_done()



async def monitor_crawl_progress(url_manager, stop_event, stats):
    """Monitor crawl progress and stop if necessary"""
    last_count = 0

    while not stop_event.is_set():
        await asyncio.sleep(60)  # Check every minute

        # Calculate URLs processed in the last minute
        current_count = len(url_manager.processed_urls)
        urls_this_minute = current_count - last_count
        stats['urls_per_minute'].append(urls_this_minute)
        last_count = current_count

        print(f"Progress: {current_count} URLs processed, {urls_this_minute} in the last minute")
        print(f"New URLs found: {len(url_manager.new_urls)}")

        # Check if we should stop based on various conditions

        # 1. Check runtime
        if time.time() - stats['start_time'] > stats['max_runtime']:
            print("Maximum runtime reached, stopping crawl.")
            stop_event.set()
            break

        # 2. Check processing rate
        if len(stats['urls_per_minute']) >= 3:  # Have at least 3 data points
            # If processing less than 10 URLs per minute for 3 consecutive minutes, stop
            if all(rate < 10 for rate in stats['urls_per_minute'][-3:]):
                print("Processing rate too low, stopping crawl.")
                stop_event.set()
                break

        # 3. Check for diminishing returns
        if len(url_manager.new_urls) < 5 and urls_this_minute < 20:
            print("Diminishing returns detected, stopping crawl.")
            stop_event.set()
            break


async def crawl(base_domain, table_name, max_workers=10, starting_url=None):
    start_time = time.time()

    # Remove protocol if present for the base domain
    base_domain = base_domain.replace('http://', '').replace('https://', '')
    base_domain = base_domain.split('/')[0]  # Remove any paths

    print(f"Starting crawl for domain and all subdomains: {base_domain}")
    print(f"Only logging HTTPS URLs")
    print(f"Only extracting links from article tags")

    # Initialize URL manager with just the base domain
    url_manager = URLManager(base_domain, table_name)

    # Create a stop event to signal workers to stop
    stop_event = asyncio.Event()

    # Add crawl statistics tracking
    stats = {
        'start_time': time.time(),
        'max_runtime': 3600,  # 1 hour maximum runtime
        'urls_per_minute': [],
        'active_workers': max_workers
    }

    # Check if we have a specific starting URL
    if starting_url:
        print(f"Using custom starting URL: {starting_url}")
        # Verify the starting URL belongs to our target domain
        parsed_url = urlparse(starting_url)
        if url_manager.is_part_of_domain(parsed_url.netloc):
            # Use the add_url method instead of directly adding to queue
            # This ensures the URL is properly tracked for saving to CSV
            await url_manager.add_url(starting_url, parent_url="custom_starting_point")
            print(f"Added starting URL to queue: {starting_url}")
        else:
            print(f"Warning: Starting URL {starting_url} is not part of the target domain {base_domain}")
            print("Falling back to standard domain discovery")
            starting_url = None

    # If no starting URL provided or it was invalid, use standard discovery
    if not starting_url and url_manager.queue.empty():
        # Check if we need to discover initial subdomains
        # Only do this if we don't have existing URLs or a specific starting point
        if not url_manager.known_urls:
            print("No existing URLs found. Discovering initial subdomains...")
            subdomains = await discover_subdomains(base_domain)
            print(f"Initially discovered {len(subdomains)} subdomains")

            # Add all subdomain URLs to the queue - try HTTPS first, then HTTP
            for subdomain in subdomains:
                # Prioritize HTTPS
                await url_manager.add_url(f"https://{subdomain}/", parent_url="initial_discovery")
                # Also try HTTP for crawling (but won't be logged)
                await url_manager.add_url(f"http://{subdomain}/", parent_url="initial_discovery")
        else:
            print(f"Using {len(url_manager.known_urls)} existing URLs from previous runs")

    # Wait a moment for the queue to be populated
    await asyncio.sleep(1)

    # Check if queue is empty after all our attempts to add URLs
    if url_manager.queue.empty():
        print("No URLs to crawl. Exiting.")
        return

    print(f"Restricting to English content only")
    print(f"Will stop if no new URLs found for 600 seconds")
    print(f"Maximum crawl depth: {url_manager.max_depth}")
    print(f"Maximum URLs to crawl: {url_manager.max_urls_to_crawl}")

    # Configure session with appropriate headers - specify English language preference
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',  # Explicitly request English content
    }

    # Create a client session with connection pooling
    conn = aiohttp.TCPConnector(limit=max_workers, ttl_dns_cache=300)

    # Create a monitoring task
    monitor_task = asyncio.create_task(monitor_crawl_progress(url_manager, stop_event, stats))

    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        # Create worker tasks
        workers = []
        for i in range(max_workers):
            worker_task = asyncio.create_task(
                worker(session, url_manager, stop_event)
            )
            workers.append(worker_task)

        # Wait for all workers to complete
        await asyncio.gather(*workers, monitor_task)

    # Save any remaining discovered URLs
    url_manager.save_new_urls()

    elapsed_time = time.time() - start_time
    print(f"Crawling complete in {elapsed_time:.2f} seconds")
    print(f"Found {len(url_manager.new_urls)} new HTTPS URLs")
    print(f"Total URLs in database: {len(url_manager.known_urls)}")
    print(f"Discovered {len(url_manager.discovered_subdomains)} subdomains")

    # Print tree statistics
    print(f"URL tree has {len(url_manager.root_nodes)} root nodes")
    max_tree_depth = max(node.depth for node in url_manager.url_tree.values()) if url_manager.url_tree else 0
    print(f"Maximum tree depth reached: {max_tree_depth}")


async def main():
    # Configuration
    base_domain = "snowflake.com"  # Replace with your target domain
    table_name = '<your fully qualified table name>'
    max_workers = 10  # Adjust based on your system capabilities
    starting_url = ""

    # Set a specific starting URL (set to None to use default behavior)
    # starting_url = "https://docs.snowflake.com/en/guides-overview-secure/"
    # starting_url = "https://docs.snowflake.com/en/user-guide/authentication-policies/"
    # starting_url = "https://docs.snowflake.com/en/release-notes/overview/"
    # starting_url = "https://docs.snowflake.com/en/guides-overview-performance/"
    # starting_url = None  # Use this to fall back to standard domain discovery

    await crawl(base_domain, table_name, max_workers, starting_url)


# run the main to starting crwaling the URLs
if __name__ == '__main__':
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
