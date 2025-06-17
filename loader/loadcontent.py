def post_process_missing_content(batch_size: int = 50, max_retries: int = 3):
    """
    Post-process URLs with missing content in the discovered_urls table.
    
    Args:
        batch_size: Number of URLs to process in each batch
        max_retries: Maximum number of retry attempts for failed requests
    """
    from snowflake.snowpark.context import get_active_session
    import requests
    from bs4 import BeautifulSoup
    import time
    import json
    from snowflake.snowpark.functions import lit
    
    session = get_active_session()
    
    # Find URLs with missing content
    missing_content_query = """
    SELECT URL 
    FROM CRAWLER_DISCOVERED_URLS 
    WHERE CLEANED_TEXT = '' OR CLEANED_TEXT IS NULL
        AND STATUS_CODE = 200 
        AND URL ILIKE 'https://%'
    ORDER BY LAST_VISITED DESC
    """
    
    urls_to_process = session.sql(missing_content_query).collect()
    total_urls = len(urls_to_process)
    
    if total_urls == 0:
        print("No URLs with missing content found.")
        return
    
    print(f"Found {total_urls} URLs with missing content")
    
    # Initialize request session
    req_session = requests.Session()
    req_session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
        'Accept-Language': 'en-US,en;q=0.5'
    })
    
    # Process URLs in batches
    for i in range(0, total_urls, batch_size):
        batch = urls_to_process[i:i + batch_size]
        content_data = []
        
        print(f"\nProcessing batch {i//batch_size + 1} of {(total_urls + batch_size - 1)//batch_size}")
        
        for row in batch:
            url = row['URL']
            retries = 0
            
            while retries < max_retries:
                try:
                    response = req_session.get(url, timeout=10)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Extract content
                        for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                            tag.decompose()
                            
                        # Get cleaned text
                        cleaned_text = ' '.join(soup.stripped_strings)
                        cleaned_text = cleaned_text[:50000]

                        # print('cleaned_text', cleaned_text)
                        
                        # Extract meta description
                        meta_desc = soup.find('meta', attrs={'name': 'description'})
                        meta_description = meta_desc.get('content', '')[:500] if meta_desc else ""
                        
                        # Extract headings
                        headings = {}
                        for i in range(1, 7):
                            heading_tags = soup.find_all(f'h{i}')
                            if heading_tags:
                                headings[f'h{i}'] = [tag.get_text().strip()[:200] for tag in heading_tags[:10]]
                        
                        content_data.append({
                            'URL': url,
                            'CLEANED_TEXT': cleaned_text,
                            'META_DESCRIPTION': meta_description,
                            'HEADINGS': json.dumps(headings)
                        })
                        
                        # print(f"✓ Processed {url}")
                        break
                    else:
                        print(f"✗ Failed to fetch {url}: HTTP {response.status_code}")
                        break
                        
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        print(f"✗ Failed to process {url} after {max_retries} attempts: {str(e)}")
                    time.sleep(1)
            
            time.sleep(0.5)
        
        # Bulk update the database
        if content_data:
            try:
                # Create a DataFrame from the content data
                update_df = session.create_dataframe(content_data)
                update_df.write.mode("overwrite").save_as_table("TEMP_UPDATES_TABLE")
            
                
                # Perform the merge using Snowpark DataFrame operations
                merge_query = f"""
                MERGE INTO CRAWLER_DISCOVERED_URLS target
                USING (SELECT * FROM TEMP_UPDATES_TABLE) source
                ON target.URL = source.URL
                WHEN MATCHED THEN UPDATE SET
                    CLEANED_TEXT = source.CLEANED_TEXT,
                    META_DESCRIPTION = source.META_DESCRIPTION,
                    HEADINGS = PARSE_JSON(source.HEADINGS),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                """
                
                session.sql(merge_query).collect()
                print(f"\n✓ Successfully updated {len(content_data)} URLs in batch")
            except Exception as e:
                print(f"\n✗ Failed to update batch: {str(e)}")
        
        print(f"\nProgress: {min(i + batch_size, total_urls)}/{total_urls} URLs processed")

    print("\nContent post-processing completed!")


if (__name__ == "__main__"):
    post_process_missing_content
