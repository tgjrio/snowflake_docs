from core.config import API_KEY, logger
from core.snowflake_utils import create_session, setup_snowflake_tables, upload_navigation_data, upload_text_data
from core.scraper import  scrape_section, scrape_text_for_section
from spider import Spider

if __name__ == "__main__":
    session = create_session()
    setup_snowflake_tables(session)
    
    client = Spider(api_key=API_KEY)
    
    sections = [
        {"url": "https://docs.snowflake.com/en/guides", "basename": "guides"},
        {"url": "https://docs.snowflake.com/en/developer", "basename": "developer"},
        {"url": "https://docs.snowflake.com/en/reference", "basename": "reference"},
        {"url": "https://docs.snowflake.com/en/release-notes/overview", "basename": "releases"},
    ]
    
    for section in sections:
        url = section["url"]
        section_name = section["basename"]
        logger.info(f"Processing section: {section_name} ({url})")
        
        try:
            tree_data = scrape_section(client, url, section_name)
            upload_navigation_data(session, tree_data, section_name)
            
            txt_data = scrape_text_for_section(client, tree_data)
            upload_text_data(session, txt_data, section_name)
            
            logger.info(f"Completed processing {url} (section={section_name})")
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
    
    session.close()
    logger.info("Scraping and uploading completed for all sections.")