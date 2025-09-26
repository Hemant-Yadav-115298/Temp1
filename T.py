"""
Business Directory Web Scraper
------------------------------
This script scrapes business data for:
1. Kansas, USA
2. Nunavut, Canada

Categories:
 - Clothing & Apparel
 - Electronics & Gadgets
 - Beauty & Personal Care
 - Jewelry & Accessories
 - Furniture & Décor
 - Legal Services
 - Accounting & Tax
 - Consulting
 - Real Estate Agency
 - Financial Planning
 - Hospitals & Clinics
 - Fitness Centers
 - Restaurants
 - Cafes
 - Catering Services
 - Coaching Institutes

Mandatory Fields:
- Business Name (skip if missing)
- Email (skip if missing)
- Phone
- Website
- Address
- Category

Output:
- kansas_businesses.xlsx
- nunavut_businesses.xlsx

Notes:
- If fewer than 10 businesses found for a category, add a row:
  "Less than 10 records available for this category"
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
from urllib.parse import urljoin, quote_plus
import logging

# -----------------------------
# CONFIGURATION
# -----------------------------

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CATEGORIES = [
    "Clothing & Apparel", "Electronics & Gadgets", "Beauty & Personal Care",
    "Jewelry & Accessories", "Furniture & Décor", "Legal Services",
    "Accounting & Tax", "Consulting", "Real Estate Agency",
    "Financial Planning", "Hospitals & Clinics", "Fitness Centers",
    "Restaurants", "Cafes", "Catering Services", "Coaching Institutes"
]

REGIONS = {
    "kansas": {
        "name": "Kansas",
        "country": "United States",
        "state_code": "KS",
        "sources": [
            "https://www.yellowpages.com",
            "https://www.yelp.com",
            "https://www.manta.com"
        ]
    },
    "nunavut": {
        "name": "Nunavut",
        "country": "Canada",
        "province_code": "NU",
        "sources": [
            "https://www.yellowpages.ca",
            "https://www.yelp.ca",
            "https://www.canada411.ca"
        ]
    }
}

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------

def get_headers():
    """Return random user agent headers"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
    ]
    return {"User-Agent": random.choice(user_agents)}

def fetch_page(url, timeout=10):
    """Fetch HTML content with headers + delay"""
    try:
        headers = get_headers()
        time.sleep(random.uniform(1, 3))  # Polite delay
        
        logger.info(f"Fetching: {url}")
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.error(f"Error fetching {url}: {e}")
        return None

def extract_email_from_text(text):
    """Extract first email found in text"""
    if not text:
        return None
    
    # Common email patterns
    email_patterns = [
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        r"mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})"
    ]
    
    for pattern in email_patterns:
        emails = re.findall(pattern, text, re.IGNORECASE)
        if emails:
            return emails[0] if isinstance(emails[0], str) else emails[0][0]
    return None

def extract_phone_from_text(text):
    """Extract phone number from text"""
    if not text:
        return None
    
    # Phone patterns for US/Canada
    phone_patterns = [
        r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",
        r"\+1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",
        r"\d{3}[-.\s]\d{3}[-.\s]\d{4}"
    ]
    
    for pattern in phone_patterns:
        phones = re.findall(pattern, text)
        if phones:
            return phones[0]
    return None

def clean_text(text):
    """Clean and normalize text"""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text.strip())

def scrape_yellowpages_us(category, location):
    """Scrape YellowPages.com for US businesses"""
    businesses = []
    try:
        # Format search terms for URL
        search_terms = quote_plus(category)
        geo_location = quote_plus(f"{location}, United States")
        
        url = f"https://www.yellowpages.com/search?search_terms={search_terms}&geo_location_terms={geo_location}"
        html = fetch_page(url)
        
        if not html:
            return businesses
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # YellowPages structure - look for business listings
        listings = soup.find_all('div', class_='info')
        
        for listing in listings[:15]:  # Get more than 10 to filter
            try:
                # Extract business name
                name_elem = listing.find('a', class_='business-name') or listing.find('h2') or listing.find('h3')
                if not name_elem:
                    continue
                
                business_name = clean_text(name_elem.get_text())
                if not business_name:
                    continue
                
                # Extract phone
                phone_elem = listing.find('div', class_='phones phone primary') or listing.find('div', class_='phone')
                phone = clean_text(phone_elem.get_text()) if phone_elem else None
                
                # Extract website
                website_elem = listing.find('a', class_='track-visit-website') or listing.find('a', href=True)
                website = website_elem.get('href') if website_elem else None
                
                # Extract address
                address_elem = listing.find('div', class_='street-address') or listing.find('span', class_='street-address')
                address = clean_text(address_elem.get_text()) if address_elem else None
                
                # Try to get email from website or listing
                email = None
                if website:
                    try:
                        website_html = fetch_page(website)
                        if website_html:
                            email = extract_email_from_text(website_html)
                    except:
                        pass
                
                # If no email from website, try to find in listing itself
                if not email:
                    listing_text = listing.get_text()
                    email = extract_email_from_text(listing_text)
                
                # Skip if no email (mandatory field)
                if not email:
                    continue
                
                businesses.append({
                    "Business Name": business_name,
                    "Email": email,
                    "Phone": phone or "",
                    "Website": website or "",
                    "Address": address or "",
                    "Category": category
                })
                
                if len(businesses) >= 10:
                    break
                    
            except Exception as e:
                logger.error(f"Error parsing listing: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error scraping YellowPages US for {category}: {e}")
    
    return businesses

def scrape_yellowpages_ca(category, location):
    """Scrape YellowPages.ca for Canadian businesses"""
    businesses = []
    try:
        # Format search terms for URL
        search_terms = quote_plus(category)
        geo_location = quote_plus(f"{location}, Canada")
        
        url = f"https://www.yellowpages.ca/search/si/1/{search_terms}/{geo_location}"
        html = fetch_page(url)
        
        if not html:
            return businesses
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # YellowPages.ca structure
        listings = soup.find_all('div', class_='listing') or soup.find_all('div', class_='listing__content')
        
        for listing in listings[:15]:  # Get more than 10 to filter
            try:
                # Extract business name
                name_elem = listing.find('h3', class_='listing__name') or listing.find('a', class_='listing__name--link')
                if not name_elem:
                    continue
                
                business_name = clean_text(name_elem.get_text())
                if not business_name:
                    continue
                
                # Extract phone
                phone_elem = listing.find('a', class_='phone') or listing.find('div', class_='listing__phone')
                phone = clean_text(phone_elem.get_text()) if phone_elem else None
                
                # Extract website
                website_elem = listing.find('a', class_='listing__website') or listing.find('a', string=re.compile(r'website', re.I))
                website = website_elem.get('href') if website_elem else None
                
                # Extract address
                address_elem = listing.find('div', class_='listing__address') or listing.find('span', class_='address')
                address = clean_text(address_elem.get_text()) if address_elem else None
                
                # Try to get email
                email = None
                if website:
                    try:
                        website_html = fetch_page(website)
                        if website_html:
                            email = extract_email_from_text(website_html)
                    except:
                        pass
                
                if not email:
                    listing_text = listing.get_text()
                    email = extract_email_from_text(listing_text)
                
                # Skip if no email (mandatory field)
                if not email:
                    continue
                
                businesses.append({
                    "Business Name": business_name,
                    "Email": email,
                    "Phone": phone or "",
                    "Website": website or "",
                    "Address": address or "",
                    "Category": category
                })
                
                if len(businesses) >= 10:
                    break
                    
            except Exception as e:
                logger.error(f"Error parsing listing: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error scraping YellowPages CA for {category}: {e}")
    
    return businesses

def scrape_yelp(category, location, is_canada=False):
    """Scrape Yelp for businesses"""
    businesses = []
    try:
        base_url = "https://www.yelp.ca" if is_canada else "https://www.yelp.com"
        search_url = f"{base_url}/search?find_desc={quote_plus(category)}&find_loc={quote_plus(location)}"
        
        html = fetch_page(search_url)
        if not html:
            return businesses
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Yelp business listings
        listings = soup.find_all('div', {'data-testid': 'serp-ia-card'}) or soup.find_all('li', class_='regular-search-result')
        
        for listing in listings[:15]:
            try:
                # Business name
                name_elem = listing.find('a', class_='css-1m051bw') or listing.find('span', class_='css-1egxyvc')
                if not name_elem:
                    continue
                    
                business_name = clean_text(name_elem.get_text())
                if not business_name:
                    continue
                
                # Try to get business page for more details
                business_link = name_elem.get('href')
                if business_link:
                    if business_link.startswith('/'):
                        business_link = base_url + business_link
                    
                    business_html = fetch_page(business_link)
                    if business_html:
                        business_soup = BeautifulSoup(business_html, 'html.parser')
                        
                        # Extract email from business page
                        email = extract_email_from_text(business_html)
                        if not email:
                            continue
                        
                        # Extract phone
                        phone_elem = business_soup.find('p', string=re.compile(r'\(\d{3}\)')) or business_soup.find('span', string=re.compile(r'\d{3}-\d{3}-\d{4}'))
                        phone = clean_text(phone_elem.get_text()) if phone_elem else ""
                        
                        # Extract website
                        website_elem = business_soup.find('a', string=re.compile(r'website', re.I)) or business_soup.find('a', href=re.compile(r'biz_redir'))
                        website = website_elem.get('href') if website_elem else ""
                        
                        # Extract address
                        address_elem = business_soup.find('address') or business_soup.find('p', string=re.compile(r'\d+.*(?:St|Ave|Rd|Blvd|Dr|Ln)'))
                        address = clean_text(address_elem.get_text()) if address_elem else ""
                        
                        businesses.append({
                            "Business Name": business_name,
                            "Email": email,
                            "Phone": phone,
                            "Website": website,
                            "Address": address,
                            "Category": category
                        })
                        
                        if len(businesses) >= 10:
                            break
                            
            except Exception as e:
                logger.error(f"Error parsing Yelp listing: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error scraping Yelp for {category}: {e}")
    
    return businesses

def scrape_category_for_region(category, region_key, region_config):
    """Scrape a specific category for a region using multiple sources"""
    logger.info(f"Scraping {category} in {region_config['name']}...")
    all_businesses = []
    
    location = region_config['name']
    is_canada = region_config['country'] == 'Canada'
    
    # Try YellowPages first
    if is_canada:
        businesses = scrape_yellowpages_ca(category, location)
    else:
        businesses = scrape_yellowpages_us(category, location)
    
    all_businesses.extend(businesses)
    
    # If we don't have enough, try Yelp
    if len(all_businesses) < 10:
        yelp_businesses = scrape_yelp(category, location, is_canada)
        all_businesses.extend(yelp_businesses)
    
    # Remove duplicates based on business name and email
    unique_businesses = []
    seen = set()
    
    for business in all_businesses:
        identifier = (business['Business Name'].lower(), business['Email'].lower())
        if identifier not in seen:
            seen.add(identifier)
            unique_businesses.append(business)
    
    # Limit to 10 businesses per category
    unique_businesses = unique_businesses[:10]
    
    logger.info(f"Found {len(unique_businesses)} businesses for {category} in {region_config['name']}")
    
    return unique_businesses

def scrape_region(region_key, region_config):
    """Scrape all categories for a given region"""
    all_results = []
    
    logger.info(f"Starting to scrape {region_config['name']}, {region_config['country']}")
    
    for category in CATEGORIES:
        businesses = scrape_category_for_region(category, region_key, region_config)
        
        # Validation: Check if we have at least 10 businesses
        if len(businesses) < 10:
            # Add note about insufficient records
            businesses.append({
                "Business Name": f"Less than 10 records available for {category}",
                "Email": "",
                "Phone": "",
                "Website": "",
                "Address": "",
                "Category": category
            })
            logger.warning(f"Only found {len(businesses)-1} businesses for {category} in {region_config['name']}")
        
        all_results.extend(businesses)
        
        # Small delay between categories
        time.sleep(random.uniform(2, 4))
    
    return all_results

def save_to_excel(data, filename):
    """Save scraped data to Excel with proper formatting"""
    if not data:
        logger.warning(f"No data to save for {filename}")
        return
    
    try:
        df = pd.DataFrame(data)
        
        # Reorder columns to match requirements
        column_order = ["Business Name", "Email", "Phone", "Website", "Address", "Category"]
        df = df[column_order]
        
        # Save to Excel
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Businesses', index=False)
            
            # Auto-adjust column widths
            worksheet = writer.sheets['Businesses']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.info(f"Successfully saved {len(data)} records to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving to Excel: {e}")

# -----------------------------
# MAIN SCRIPT
# -----------------------------

def main():
    """Main function to run the scraper"""
    logger.info("Starting Business Directory Web Scraper")
    
    for region_key, region_config in REGIONS.items():
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing region: {region_config['name']}, {region_config['country']}")
        logger.info(f"{'='*50}")
        
        try:
            # Scrape all data for the region
            region_data = scrape_region(region_key, region_config)
            
            # Save to Excel
            filename = f"{region_key}_businesses.xlsx"
            save_to_excel(region_data, filename)
            
            logger.info(f"Completed scraping for {region_config['name']}")
            
        except Exception as e:
            logger.error(f"Error processing region {region_key}: {e}")
            continue
    
    logger.info("\nScraping completed! Check the generated Excel files:")
    logger.info("- kansas_businesses.xlsx")
    logger.info("- nunavut_businesses.xlsx")

if __name__ == "__main__":
    main()
