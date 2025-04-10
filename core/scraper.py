from .config import BASE_URL, logger

def resolve_url(href, base_url=BASE_URL):
    """
    Resolves a URL by making it absolute using the provided base URL.

    If the href is already an absolute URL (starts with "http"), it is returned as is.
    Otherwise, it is concatenated with the base_url to form a fully-qualified URL.

    Args:
        href (str): The URL or path to resolve.
        base_url (str, optional): The base URL to prepend if href is relative. Defaults to BASE_URL.

    Returns:
        str: The fully-qualified URL.
    """
    if href.startswith("http"):
        return href
    return base_url + href

def extract_tree_data(tree_data, base_url=BASE_URL):
    """
    Extracts navigation data from a hierarchical 'tree' structure into a flat list of dictionaries.

    The 'tree' structure is a nested JSON object from the Snowflake documentation site,
    containing navigation links. This function recursively processes the tree, extracting relevant
    fields (id, url, label, type, depth, parent_url) for each node, and resolves relative URLs.

    Args:
        tree_data (dict): The 'tree' object from the JSON response, typically containing a 'children' key.
        base_url (str, optional): The base URL to resolve relative hrefs. Defaults to BASE_URL.

    Returns:
        list: A list of dictionaries, each containing navigation data:
              {'id', 'url', 'label', 'type', 'depth', 'parent_url'}.
    """
    def process_node(node, result_list):
        """
        Recursively processes a single node in the tree, extracting its data and adding it to result_list.

        Args:
            node (dict): The current node in the tree.
            result_list (list): The list to append extracted data to.
        """
        # Skip if node is not a dictionary (e.g., None or unexpected type)
        if not isinstance(node, dict):
            return
        # Check if the node has all required fields to be a valid navigation entry
        if all(key in node for key in ['id', 'href', 'label', 'type', 'depth', 'parentRef']):
            # Create an entry with the node's data, resolving URLs as needed
            entry = {
                'id': node['id'],
                'url': resolve_url(node['href'], base_url),  # Resolve the href to a full URL
                'label': node['label'],
                'type': node['type'],
                'depth': node['depth'],
                'parent_url': resolve_url(node['parentRef'], base_url)  # Resolve the parent reference
            }
            result_list.append(entry)
        # Recursively process any children of this node
        if 'children' in node and isinstance(node['children'], list):
            for child in node['children']:
                process_node(child, result_list)
    
    result = []  # Initialize the list to store all extracted navigation entries
    # Check if the tree_data has a 'children' key and it's a list, then process each child
    if 'children' in tree_data and isinstance(tree_data['children'], list):
        for child in tree_data['children']:
            process_node(child, result)
    return result

def scrape_navigation(client, url, params):
    """
    Scrapes navigation data from a given URL using the Spider client.

    This function sends a request to the specified URL, expecting a JSON response containing
    a 'tree' structure with navigation data. It extracts the tree and processes it into a list
    of navigation entries using extract_tree_data.

    Args:
        client (Spider): The Spider client instance for making HTTP requests.
        url (str): The URL to scrape.
        params (dict): Parameters to pass to the Spider client's scrape_url method.

    Returns:
        list: A list of navigation entries extracted from the tree data.

    Raises:
        ValueError: If the response format is unexpected (not a list).
    """
    logger.info(f"Scraping navigation data from {url}")
    # Make the HTTP request to scrape the URL
    result_list = client.scrape_url(url, params=params)
    # Validate that the response is a list as expected
    if not result_list or not isinstance(result_list, list):
        raise ValueError("Unexpected response format: Expected a list")
    result = result_list[0]  # Take the first item from the response list
    # Navigate the nested JSON structure to find the 'tree' data
    tree_location = (
        result.get('json_data', {})
              .get('other_scripts', [{}])[0]
              .get('props', {})
              .get('pageProps', {})
              .get('tree')
    )
    # Extract navigation data from the tree, or return an empty list if tree is not found
    return extract_tree_data(tree_location) if tree_location else []

def scrape_text_content(client, urls, params, batch_size=50, max_retries=3):
    """
    Scrapes text content from a list of URLs in batches, with retry logic for failed requests.

    This function processes URLs in batches to avoid overwhelming the server, retrying failed
    requests up to max_retries times. It returns a list of dictionaries containing the URL and
    its scraped content. Failed URLs (after all retries) are included with empty content.

    Args:
        client (Spider): The Spider client instance for making HTTP requests.
        urls (list): List of URLs to scrape.
        params (dict): Parameters to pass to the Spider client's scrape_url method.
        batch_size (int, optional): Number of URLs to process in each batch. Defaults to 50.
        max_retries (int, optional): Number of retry attempts for failed URLs. Defaults to 3.

    Returns:
        list: A list of dictionaries, each with 'url' and 'content' keys.
    """
    # Remove duplicates from the URL list to avoid redundant scraping
    all_unique_urls = set(urls)
    fails = set(all_unique_urls)  # Initially, all URLs are considered "failed" until successfully scraped
    success_map = {}  # Dictionary to store successful results: {url: content}
    attempt = 1
    # Continue retrying until max_retries is reached or all URLs are successfully scraped
    while attempt <= max_retries and fails:
        logger.info(f"Scrape attempt {attempt} for {len(fails)} total URLs.")
        fail_list = list(fails)
        # Process URLs in batches of size batch_size
        for i in range(0, len(fail_list), batch_size):
            batch_urls = fail_list[i:i+batch_size]
            # Concatenate URLs into a comma-separated string as required by the Spider API
            concatenated_urls = ','.join(batch_urls)
            # Scrape the batch of URLs
            results = client.scrape_url(concatenated_urls, params=params)
            # Process each result in the batch
            for res in results:
                # Check if the request was successful (status 200, content exists, no error)
                if res.get('status') == 200 and res.get('content') and not res.get('error'):
                    success_map[res['url']] = res['content']
        # Remove successfully scraped URLs from the fails set
        fails = fails - set(success_map.keys())
        if fails:
            logger.info(f"After attempt {attempt}, {len(fails)} URLs remain unsatisfied.")
        attempt += 1
    # If any URLs still failed after all retries, include them with empty content
    if fails:
        logger.error(f"After {max_retries} attempts, {len(fails)} URLs still failed. Adding with empty content.")
        for url in fails:
            success_map[url] = ""
    # Convert the success map to a list of dictionaries
    return [{"url": u, "content": c} for u, c in success_map.items()]

def scrape_section(client, url, section_name):
    """
    Scrapes navigation data for a specific section of the Snowflake documentation.

    This function configures parameters for scraping navigation data, calls scrape_navigation
    to retrieve the data, and removes duplicates based on URLs before returning the results.

    Args:
        client (Spider): The Spider client instance for making HTTP requests.
        url (str): The URL of the section to scrape.
        section_name (str): The name of the section (e.g., 'guides', 'developer').

    Returns:
        list: A list of unique navigation entries for the section.
    """
    # Define parameters for the Spider client to scrape navigation data
    crawler_params = {
        'proxy_enabled': True,  # Use a proxy for the request
        'store_data': False,  # Do not store raw data on the Spider server
        'metadata': True,  # Include metadata in the response
        'return_page_links': True,  # Include links found on the page
        'return_json_data': True,  # Include JSON data in the response
        'readability': True,  # Use readability parsing to extract clean content
        'return_format': 'text'  # Return the content as text
    }
    # Scrape the navigation data for the section
    tree_data = scrape_navigation(client, url, crawler_params)
    # Remove duplicates by URL to avoid redundant entries
    unique_tree_data = []
    seen_urls = set()
    for row in tree_data:
        if row['url'] not in seen_urls:
            seen_urls.add(row['url'])
            unique_tree_data.append(row)
    return unique_tree_data

def scrape_text_for_section(client, tree_data):
    """
    Scrapes text content for all URLs found in the navigation data of a section.

    This function extracts URLs from the navigation data (tree_data) and scrapes their text content
    using scrape_text_content with specific parameters for text extraction.

    Args:
        client (Spider): The Spider client instance for making HTTP requests.
        tree_data (list): List of navigation entries containing URLs to scrape.

    Returns:
        list: A list of dictionaries with 'url' and 'content' for each scraped URL.
    """
    # Extract all URLs from the navigation data
    all_urls = [item['url'] for item in tree_data]
    # Define parameters for scraping text content
    text_crawler_params = {
        'proxy_enabled': True,  # Use a proxy for the request
        'store_data': False,  # Do not store raw data on the Spider server
        'readability': True,  # Use readability parsing to extract clean content
        'return_format': 'text'  # Return the content as text
    }
    # Scrape text content for all URLs, using a larger batch size for efficiency
    return scrape_text_content(client, all_urls, text_crawler_params, batch_size=500, max_retries=3)