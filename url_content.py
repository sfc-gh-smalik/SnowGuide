import concurrent.futures
from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse, urljoin
import pandas as pd
from datetime import datetime

def scrape_url(url, metadata):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'en-US,en;q=0.5'
        }
        
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        
        if 'text/html' not in response.headers.get('Content-Type', '').lower():
            raise ValueError("Not an HTML page")
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Remove unwanted elements
        for element in soup.select('header, footer, nav, .menu, .sidebar, .navigation, script, style'):
            element.decompose()
            
        # Find the main content area with fallbacks
        content_areas = [
            soup.find('article'),
        ]
        
        content = None
        for area in content_areas:
            if area:
                # Process hyperlinks before getting text
                for link in area.find_all('a'):
                    href = link.get('href')
                    if href:
                        # Convert relative URLs to absolute
                        if not href.startswith(('http://', 'https://')):
                            href = urljoin(url, href)
                        # Replace the link text with both text and URL
                        link_text = link.get_text(strip=True)
                        if link_text:
                            link.replace_with(f"{link_text} [{href}]")
                
                content = area.get_text(separator=' ', strip=True)
                if content:
                    break
                    
        if not content:
            raise ValueError("No content found")
            
        return {
            'status': 'success',
            'data': (url, content[:10000000], datetime.utcnow(), 
                    metadata['PATH'], metadata['PARENT_URL'])
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'url': url,
            'error': str(e)
        }


def scrape_content(session, url_table , content_table, errors_table, batch_size, max_workers):
    # Validate input parameters
    batch_size = max(1, min(int(batch_size), 1000))
    max_workers = max(1, min(int(max_workers), 50))
    
    # Create tables if they don't exist
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {content_table} (
            URL STRING PRIMARY KEY,
            CONTENT STRING,
            PROCESSED_TIMESTAMP TIMESTAMP_LTZ,
            PATH STRING,
            PARENT_URL STRING
        )
    """).collect()
    
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {errors_table} (
            URL STRING,
            ERROR_MESSAGE STRING,
            ERROR_TIMESTAMP TIMESTAMP_LTZ,
            RETRY_COUNT NUMBER DEFAULT 0
        )
    """).collect()
    
    # Get unprocessed URLs with metadata
    unscraped = session.sql(f"""
        SELECT u.URL, u.PATH, u.PARENT_URL
        FROM {url_table} u
        LEFT JOIN {content_table} d ON u.URL = d.URL
        LEFT {errors_table} e ON u.URL = e.URL
        WHERE d.URL IS NULL 
        AND (e.URL IS NULL OR e.RETRY_COUNT < 3)
        AND u.URL like '%docs.snowflake.com%'
        AND u.PARENT_URL not like 'initial_discovery'
        AND u.url not like '%.png'
        AND u.url not like '%.svg'
        AND u.url not like '%.zip'
        AND u.url not like '%.js'
        AND error_message not in ('Not an HTML page','No content found')
        limit 1000000
    """).collect()
    
    urls_with_metadata = [{
        'url': row[0],
        'metadata': {
            'PATH': row[1],
            'PARENT_URL': row[2]
        }
    } for row in unscraped]
    
    successful_results = []
    error_results = []
    processed_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(scrape_url, item['url'], item['metadata']): item
            for item in urls_with_metadata
        }
        
        for future in concurrent.futures.as_completed(future_to_url):
            result = future.result()
            url_item = future_to_url[future]
            
            if result['status'] == 'success':
                successful_results.append(result['data'])
                processed_count += 1
            else:
                error_results.append({
                    'URL': url_item['url'],
                    'ERROR_MESSAGE': result['error'],
                    'ERROR_TIMESTAMP': datetime.utcnow(),
                    'RETRY_COUNT' : 1
                })
            
            # Process successful results in batches
            if len(successful_results) >= batch_size:
                df = pd.DataFrame(successful_results, 
                                columns=["URL", "CONTENT", "PROCESSED_TIMESTAMP", "PATH", "PARENT_URL"])
                session.create_dataframe(df).write.mode("append").save_as_table(content_table)
                successful_results = []
            
            # Process error results in batches
            if len(error_results) >= batch_size:
                error_df = pd.DataFrame(error_results)
                session.create_dataframe(error_df).write.mode("append").save_as_table(errors_table)
                error_results = []
    
    # Process remaining results
    if successful_results:
        df = pd.DataFrame(successful_results, 
                         columns=["URL", "CONTENT", "PROCESSED_TIMESTAMP", "PATH", "PARENT_URL"])
        session.create_dataframe(df).write.mode("append").save_as_table(content_table)
    
    if error_results:
        error_df = pd.DataFrame(error_results)
        session.create_dataframe(error_df).write.mode("append").save_as_table(errors_table)
    
    return f"Processed {processed_count} URLs successfully with batch_size={batch_size} and max_workers={max_workers}"


# currently only looking for content in specific section of the webpage 
# can be extended to other sections as required
from snowflake.snowpark.context import get_active_session
session = get_active_session()
url_table = "<fully_qualified_url_crawler_table_name>"
content_table = "<fully_qualified_url_content_table_name>"
errors_table = "<fully_qualified_error_log_table_name>"
scrape_content(session, 50, 5)
