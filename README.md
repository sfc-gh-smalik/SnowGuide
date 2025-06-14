# SnowGuide

A Snowflake Docs and Knowledge Chatbot/Slackbot

**Usage**

# Enhanced Incremental Web Crawler

A multi-threaded, incremental web crawler with content storage capabilities and Snowflake integration.

## Features

### Core Functionality
- Multi-threaded crawling with configurable worker count
- Incremental crawling with content change detection
- URL language filtering and content language detection
- Comprehensive content extraction and storage
- Real-time telemetry and progress monitoring
- Graceful shutdown handling

### Storage Options
- Snowflake database storage with optimized batch operations
- CSV file storage with automatic upload to Snowflake
- Content storage with configurable options

### Content Processing
- HTML content cleaning and text extraction
- Meta tag extraction
- Heading structure analysis
- Link extraction and validation
- Content change detection
- Duplicate content detection

### Performance Features
- Batch processing for database operations
- Memory usage monitoring
- Configurable request delays and timeouts
- Automatic retry mechanism
- Queue-based URL management

## Configuration

Key configuration options include:

python 

config = { 
	# Storage configuration 
	'use_database': True, 
	
	# Use Snowflake storage 
	'table_prefix': 'CRAWLER',
	
	# Crawling parameters
	'max_depth': 12,
	'max_workers': 40,
	'request_delay': 1,
	
	# Content storage options
	'store_raw_html': False,
	'store_cleaned_text': False,
	'max_content_size': 5000000,  # 5MB limit
	
	# Incremental features
	'enable_content_change_detection': True,
	'revisit_interval_hours': 24 * 7,
	
	# Language filtering
	'enable_language_filtering': True,
	'enable_url_language_filtering': True
 }


## Usage
1.Configure the crawler settings in the `main()` function 
2.Set up starting URLs and allowed domains 
3.Run the crawler:

python 
if name == "main": main()

## Post-Processing
After crawling, you can process missing content using:

python 
post_process_missing_content(batch_size=100, max_retries=3)


## Data Storage
The crawler creates the following tables in Snowflake: 

- `{prefix}_DISCOVERED_URLS`: Main table for crawled URLs and content
- `{prefix}_VISITED_URLS`: Track visited URLs
- `{prefix}_CONTENT_HASHES`: Track content changes
- `{prefix}_STATE`: Store crawler state
- `{prefix}_REVISIT_SCHEDULE`: Manage incremental crawling

- ## Requirements
- Python 3.9 + - Snowflake account with appropriate permissions
- Required Python packages:
- requests - beautifulsoup4 - langdetect - snowflake - snowpark - python ##


## Best Practices
1.Start with a small set of URLs for testing
2.Monitor memory usage and adjust worker count accordingly
3.Use appropriate delays to avoid overwhelming target servers
4.Enable content storage only if needed 
5.Configure appropriate timeouts and retry limits ## Monitoring 

The crawler provides real - time telemetry including: - Pages processed per second - Memory usage - Error rates - Content change statistics - Language distribution - Queue status 

## Error Handling
- Automatic retry for recoverable errors
- Error categorization and logging
- Graceful shutdown on critical errors
- Memory monitoring and protection

## Limitations 
- Only processes HTTPS URLs
- Content size limits apply
- Memory usage increases with worker count
- Database performance depends on Snowflake warehouse size


## License
This code is provided as - is. Use at your own risk.


**Contributing**
  - Feel free to submit issues and enhancement requests.

