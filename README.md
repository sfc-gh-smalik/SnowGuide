# SnowGuide

A Snowflake Docs and Knowledge Chatbot/Slackbot

**Usage**

**URL Crawler Configuration**
Configure domains and extraction modes:

		  domain_extract_modes = {
		      "docs.snowflake.com": "article",
		      "quickstarts.snowflake.com": "full",
		      "snowflake.com": "article",
		      "community.snowflake.com": "full",
		      "developers.snowflake.com": "article"
		  }


**Starting Points Configuration**

**Define starting URLs:**

		  starting_urls = [
		      "https://docs.snowflake.com/en/user-guide/",
		      "https://quickstarts.snowflake.com/guide/",
		      "https://developers.snowflake.com/"
		  ]


**Running the Services**

**Start URL Crawler:**

		  await crawl(
		      base_domain="snowflake.com",
		      max_workers=10,
		      starting_urls=starting_urls,
		      domain_extract_modes=domain_extract_modes
		  )


**Process Content:**

		scrape_content(
		    session=session,
		    url_table="SNOWFLAKE_URLS",
		    content_table="DOCUMENT_CONTENT",
		    errors_table="URL_ERRORS",
		    batch_size=50,
		    max_workers=5
		)


**Features**
  - Intelligent URL discovery and tracking
  - Support for multiple content extraction strategies
  - Handling of redirects and 404 errors
  - English content filtering
  - Automatic subdomain discovery
  - Batch processing of content
  - Error tracking and retry mechanism
  - Semantic search capabilities
  - Natural language query processing
  
**Limitations**
  - Only processes English content
  - Limited to specified Snowflake domains
  - Maximum crawl depth and URL limits
  - Content size limitations
  - Rate limiting for polite crawling

**Contributing**
  - Feel free to submit issues and enhancement requests.

**License**
  - This project is licensed under the terms of the MIT license.

