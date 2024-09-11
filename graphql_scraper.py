import json
import logging
import time
import os
import random
import signal
from typing import List, Dict, Any
from urllib.parse import urlparse
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle
import time

from json_to_csv import process_graphql_responses

# Load environment variables
try:
    load_dotenv()
except Exception as e:
    print(f"Error loading .env file: {e}")
    exit(1)

# Configuration
HEADLESS_MODE = os.getenv('HEADLESS_MODE', 'True').lower() == 'true'
SCROLL_PAUSE_TIME = float(os.getenv('SCROLL_PAUSE_TIME', '2.0'))
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '60000'))
RATE_LIMIT_DELAY = float(os.getenv('RATE_LIMIT_DELAY', '1.0'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RAW_DATA_FILE = os.getenv('RAW_DATA_FILE', 'raw_responses.json')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '5'))
AGE_CONFIRMATION_TIMEOUT = int(os.getenv('AGE_CONFIRMATION_TIMEOUT', '5000'))
AGE_CONFIRMATION_SELECTOR = os.getenv('AGE_CONFIRMATION_SELECTOR', 'button#age-confirmation')


def validate_config():
    if SCROLL_PAUSE_TIME <= 0:
        raise ValueError("SCROLL_PAUSE_TIME must be positive")
    if REQUEST_TIMEOUT <= 0:
        raise ValueError("REQUEST_TIMEOUT must be positive")
    if RATE_LIMIT_DELAY < 0:
        raise ValueError("RATE_LIMIT_DELAY must be non-negative")
    if MAX_RETRIES <= 0:
        raise ValueError("MAX_RETRIES must be positive")
    if MAX_WORKERS <= 0:
        raise ValueError("MAX_WORKERS must be positive")
    if AGE_CONFIRMATION_TIMEOUT <= 0:
        raise ValueError("AGE_CONFIRMATION_TIMEOUT must be positive")


validate_config()

# User Agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0'
]


def setup_logging():
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    log_file = os.getenv('LOG_FILE', 'scraper.log')
    log_max_bytes = int(os.getenv('LOG_MAX_BYTES', 10485760))
    log_backup_count = int(os.getenv('LOG_BACKUP_COUNT', 5))

    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    file_handler = RotatingFileHandler(log_file, maxBytes=log_max_bytes, backupCount=log_backup_count)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(file_handler)


def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def rate_limit():
    time.sleep(RATE_LIMIT_DELAY)


def save_raw_responses(responses: List[Dict[str, Any]], filename: str):
    try:
        with open(filename, 'w') as f:
            json.dump(responses, f)
    except IOError as e:
        logging.error(f"Error saving raw responses: {e}")


def load_raw_responses(filename: str) -> List[Dict[str, Any]]:
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        logging.error(f"Error loading raw responses: {e}")
    return []


def intercept_graphql(request):
    if (request.resource_type == "fetch" or request.resource_type == "xhr") and "graphql" in request.url.lower():
        logging.info(f"GraphQL Request URL: {request.url}")
        logging.info(f"GraphQL Request Method: {request.method}")
        logging.info(f"GraphQL Request Headers: {request.headers}")
        try:
            body = request.post_data
            if body:
                json_body = json.loads(body)
                logging.info(f"GraphQL Request Body: {json.dumps(json_body, indent=2)}")
            else:
                logging.info("GraphQL Request Body: Empty")
        except json.JSONDecodeError:
            logging.warning(f"Failed to parse JSON request body: {body}")
        except Exception as e:
            logging.error(f"Error processing request body: {str(e)}")


def scroll_to_bottom(page, max_scroll_time=30, scroll_pause=1.0):
    start_time = time.time()
    last_height = page.evaluate("document.body.scrollHeight")
    while time.time() - start_time < max_scroll_time:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(scroll_pause)
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def performance_monitor(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"Function {func.__name__} took {end_time - start_time:.2f} seconds to execute.")
        return result

    return wrapper


def handle_age_confirmation(page):
    try:
        confirm_button = page.wait_for_selector(AGE_CONFIRMATION_SELECTOR, timeout=AGE_CONFIRMATION_TIMEOUT)
        if confirm_button:
            logging.info("Age confirmation dialog detected. Attempting to click confirmation button.")
            confirm_button.click()
            page.wait_for_load_state("networkidle", timeout=5000)
            logging.info("Age confirmation button clicked successfully.")
        else:
            logging.info("No age confirmation dialog detected.")
    except PlaywrightTimeoutError:
        logging.info("No age confirmation dialog detected (timeout).")
    except Exception as e:
        logging.error(f"Error handling age confirmation: {str(e)}")


def custom_timeout_handler(page, timeout):
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PlaywrightTimeoutError:
        logging.warning(f"Networkidle not reached, checking if page is usable")
        if page.query_selector('body'):
            logging.info("Page body found, continuing with scraping")
        else:
            raise PlaywrightTimeoutError("Page body not found after timeout")


def retry_on_network_error(func):
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((PlaywrightTimeoutError, ConnectionError))
    )
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def save_progress(url, responses, graphql_url):
    progress_file = f"progress_{url.replace('https://', '').replace('http://', '').replace('/', '_')}.pkl"
    with open(progress_file, 'wb') as f:
        pickle.dump({'responses': responses, 'graphql_url': graphql_url}, f)
    logging.info(f"Progress saved for URL: {url}")


def load_progress(url):
    progress_file = f"progress_{url.replace('https://', '').replace('http://', '').replace('/', '_')}.pkl"
    if os.path.exists(progress_file):
        with open(progress_file, 'rb') as f:
            progress = pickle.load(f)
        logging.info(f"Progress loaded for URL: {url}")
        return progress['responses'], progress['graphql_url']
    return None, None


def sanitize_filename(filename):
    return "".join([c for c in filename if c.isalpha() or c.isdigit() or c in [' ', '-', '_']]).rstrip()


@performance_monitor
@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=1, min=4, max=10))
def scrape_url(url: str):
    if not is_valid_url(url):
        logging.error(f"Invalid URL: {url}")
        return

    sanitized_url = sanitize_filename(url)
    progress_file = f"progress_{sanitized_url}.pkl"

    url_responses, saved_graphql_url = load_progress(progress_file)
    if url_responses:
        logging.info(f"Resuming scraping for URL: {url}")
    else:
        url_responses = []

    timeout_retries = 0
    max_timeout_retries = 3  # You can adjust this value

    while timeout_retries <= max_timeout_retries:
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=HEADLESS_MODE)
                context = browser.new_context(user_agent=random.choice(USER_AGENTS))
                page = context.new_page()

                graphql_url = saved_graphql_url  # Use saved GraphQL URL if available

                def handle_response_for_url(response):
                    nonlocal graphql_url
                    if (
                            response.request.resource_type == "fetch" or response.request.resource_type == "xhr") and "graphql" in response.url.lower():
                        graphql_url = response.url  # Capture the GraphQL URL
                        try:
                            json_response = response.json()
                            if json_response not in url_responses:
                                url_responses.append(json_response)
                                save_progress(progress_file, url_responses, graphql_url)
                        except json.JSONDecodeError:
                            logging.warning(f"Failed to parse JSON response for URL: {url}")
                        except Exception as e:
                            logging.error(f"Error processing response for URL {url}: {str(e)}")

                page.on("request", intercept_graphql)
                page.on("response", handle_response_for_url)

                page.goto(url)
                try:
                    custom_timeout_handler(page, REQUEST_TIMEOUT)
                    logging.info(f"Loaded: {url}")

                    handle_age_confirmation(page)

                    # Scroll and wait for dynamic content to load
                    scroll_to_bottom(page)

                    # Wait for any final asynchronous operations
                    page.wait_for_timeout(5000)

                    # Ensure we've captured all GraphQL responses
                    page.reload()
                    page.wait_for_load_state("networkidle", timeout=REQUEST_TIMEOUT)

                    # If we've reached this point without a timeout, break the retry loop
                    break

                except PlaywrightTimeoutError:
                    timeout_retries += 1
                    logging.warning(
                        f"Timeout occurred for URL: {url}. Retry attempt {timeout_retries} of {max_timeout_retries}")
                    save_progress(progress_file, url_responses, graphql_url)
                    if timeout_retries > max_timeout_retries:
                        logging.error(f"Max timeout retries reached for URL: {url}. Moving to next URL.")
                        return
                    time.sleep(10)  # Wait for 10 seconds before retrying
                    continue

            except PlaywrightError as e:
                logging.error(f"Playwright error while scraping {url}: {str(e)}")
                save_progress(progress_file, url_responses, graphql_url)
                raise
            except Exception as e:
                logging.error(f"Unexpected error while scraping {url}: {str(e)}")
                save_progress(progress_file, url_responses, graphql_url)
                raise
            finally:
                if 'browser' in locals():
                    browser.close()

    # Process and save CSV for this URL
    if url_responses:
        output_file = f"output_{sanitized_url}_{int(time.time())}.csv"
        try:
            process_graphql_responses(url_responses, output_file, graphql_url)
            logging.info(f"Data written to {output_file} for URL: {url}")
            # Remove progress file after successful scraping
            if os.path.exists(progress_file):
                os.remove(progress_file)
        except IOError as e:
            logging.error(f"IOError while writing CSV for URL {url}: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error while processing data for URL {url}: {str(e)}")
    else:
        logging.warning(f"No GraphQL responses collected for URL: {url}")


def scrape_urls_parallel(urls: List[str]):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(scrape_url, url): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
            except Exception as exc:
                logging.error(f'{url} generated an exception: {exc}')
            finally:
                rate_limit()


def signal_handler(signum, frame):
    logging.info("Received interrupt signal. Shutting down gracefully...")
    exit(0)


def load_urls_from_file(filename: str) -> List[str]:
    try:
        with open(filename, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except IOError as e:
        logging.error(f"Error loading URLs from file: {e}")
        return []


def main(urls: List[str]):
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        scrape_urls_parallel(urls)
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    setup_logging()

    urls_file = os.getenv('URLS_FILE', 'urls_to_scrape.txt')
    urls_to_scrape = load_urls_from_file(urls_file)

    if urls_to_scrape:
        main(urls_to_scrape)
    else:
        logging.error("No URLs to scrape. Exiting.")
