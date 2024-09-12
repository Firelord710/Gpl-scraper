import csv
import json
import logging
from urllib.parse import urlparse
import os
from typing import Union, List, Dict, Any


def is_dutchie_menu(responses: List[Dict[str, Any]], graphql_url: str) -> bool:
    try:
        logging.debug(f"Checking if {graphql_url} is a Dutchie menu")
        parsed_url = urlparse(graphql_url)
        if 'dutchie' in parsed_url.netloc:
            logging.info(f"{graphql_url} is a Dutchie menu")
            return True
        else:
            logging.info(f"{graphql_url} is not a Dutchie menu")
            return False
    except Exception as e:
        logging.error(f"Error in is_dutchie_menu: {str(e)}")
        return False


def clean_dutchie_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned_data = []
    for item in data:
        if 'data' in item and 'menu' in item['data']:
            for product in item['data']['menu']:
                try:
                    variants = product.get('variants', [])
                    prices = [variant.get('option', {}).get('price') for variant in variants if variant.get('option')]
                    price_range = f"${min(prices)}-${max(prices)}" if prices else "N/A"
                    if prices and min(prices) == max(prices):
                        price_range = f"${min(prices)}"

                    cleaned_product = {
                        'Name': product.get('name', ''),
                        'Category': product.get('category', {}).get('name', ''),
                        'SubCategory': product.get('subcategory', {}).get('name', ''),
                        'THC': product.get('potency', {}).get('thc', ''),
                        'CBD': product.get('potency', {}).get('cbd', ''),
                        'Price': price_range,
                        'Strain': product.get('strain', {}).get('name', ''),
                        'Brand': product.get('brand', {}).get('name', ''),
                        'Weight': ', '.join(
                            [f"{v.get('option', {}).get('label')}: ${v.get('option', {}).get('price')}" for v in
                             variants if v.get('option')]),
                        'Type': product.get('type', ''),
                        'Description': product.get('description', '').replace('\n', ' ').replace('\r', '')
                    }
                    cleaned_data.append(cleaned_product)
                except Exception as e:
                    logging.error(f"Error cleaning product data: {str(e)}")
                    logging.error(f"Product data: {product}")
    return cleaned_data


def flatten_json(data: Any, prefix: str = '') -> Dict[str, Any]:
    result = {}
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{prefix}.{key}" if prefix else key
            result.update(flatten_json(value, new_key))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            result.update(flatten_json(item, f"{prefix}[{i}]"))
    else:
        result[prefix] = data
    return result


def process_api_responses(responses: List[Union[Dict[str, Any], List[Any]]], output_file_cleaned: str,
                          output_file_generic: str, output_file_unflattened: str, api_url: str) -> None:
    if not responses:
        logging.warning("No API responses to process.")
        return

    try:
        is_iheartjane = "iheartjane.com" in api_url.lower() or "x-algolia-agent" in api_url.lower()

        logging.info(f"API type detected: {'iHeartJane' if is_iheartjane else 'Unknown'}")
        logging.info(f"API URL: {api_url}")
        logging.info(f"Number of responses: {len(responses)}")
        logging.info(f"First response type: {type(responses[0])}")
        logging.debug(
            f"First response preview: {json.dumps(responses[0])[:500]}...")  # Log the first 500 characters of the first response

        if is_iheartjane:
            logging.info("Using iHeartJane data cleaning.")
            cleaned_data = clean_iheartjane_data(responses)
        else:
            logging.info("Using generic data cleaning.")
            cleaned_data = clean_generic_data(responses)

        if cleaned_data:
            fieldnames_cleaned = list(cleaned_data[0].keys())
            logging.info(f"Number of cleaned items: {len(cleaned_data)}")
            logging.info(f"Cleaned fieldnames: {fieldnames_cleaned}")
        else:
            logging.warning("No cleaned data to process.")
            return

        logging.info(f"Writing cleaned CSV to: {output_file_cleaned}")
        with open(output_file_cleaned, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_cleaned)
            writer.writeheader()
            for item in cleaned_data:
                writer.writerow({k: str(v).replace('\n', ' ').replace('\r', '') for k, v in item.items()})

        logging.info(f"Successfully wrote cleaned data to {output_file_cleaned}")
        logging.info(f"File size: {os.path.getsize(output_file_cleaned)} bytes")
        logging.info(f"File exists: {os.path.exists(output_file_cleaned)}")

        # Generic data flattening
        logging.info("Flattening generic data...")
        flattened_data = [flatten_json(response) for response in responses]
        fieldnames_generic = sorted(set().union(*(d.keys() for d in flattened_data if isinstance(d, dict))))

        # Remove blank or whitespace-only columns from generic data
        cleaned_flattened_data = [{k: v for k, v in item.items() if str(v).strip()} for item in flattened_data if
                                  isinstance(item, dict)]
        cleaned_fieldnames_generic = sorted(set().union(*(d.keys() for d in cleaned_flattened_data)))

        logging.info(f"Writing generic CSV to: {output_file_generic}")
        with open(output_file_generic, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=cleaned_fieldnames_generic)
            writer.writeheader()
            for item in cleaned_flattened_data:
                writer.writerow({k: str(v).replace('\n', ' ').replace('\r', '') for k, v in item.items()})

        logging.info(f"Successfully wrote generic data to {output_file_generic}")
        logging.info(f"File size: {os.path.getsize(output_file_generic)} bytes")
        logging.info(f"File exists: {os.path.exists(output_file_generic)}")

        # Unflattened data
        logging.info("Writing unflattened data...")
        fieldnames_unflattened = sorted(set().union(*(d.keys() for d in responses if isinstance(d, dict))))

        logging.info(f"Writing unflattened CSV to: {output_file_unflattened}")
        with open(output_file_unflattened, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_unflattened)
            writer.writeheader()
            for response in responses:
                if isinstance(response, dict):
                    writer.writerow({k: str(v).replace('\n', ' ').replace('\r', '') for k, v in response.items()})
                else:
                    logging.warning(f"Skipping non-dict response: {type(response)}")

        logging.info(f"Successfully wrote unflattened data to {output_file_unflattened}")
        logging.info(f"File size: {os.path.getsize(output_file_unflattened)} bytes")
        logging.info(f"File exists: {os.path.exists(output_file_unflattened)}")
    except Exception as e:
        logging.error(f"Error in process_api_responses: {str(e)}")
        logging.error(f"API URL: {api_url}")
        logging.error(f"Responses type: {type(responses)}")
        logging.error(
            f"Responses preview: {json.dumps(responses[:2])[:1000]}...")  # Log the first two responses, up to 1000 characters
        raise


def process_graphql_responses(responses: List[Dict[str, Any]], output_file_dutchie: str, output_file_generic: str,
                              output_file_unflattened: str, graphql_url: str) -> None:
    if not responses:
        logging.warning("No GraphQL responses to process.")
        return

    try:
        is_dutchie = "dutchie.com" in graphql_url.lower()
        logging.info(f"Dutchie menu detected: {is_dutchie}")

        if is_dutchie:
            logging.info("Using Dutchie data cleaning.")
            cleaned_data = clean_dutchie_data(responses)
            fieldnames_dutchie = ['Name', 'Category', 'SubCategory', 'THC', 'CBD', 'Price', 'Strain', 'Brand', 'Weight',
                                  'Type', 'Description']
        else:
            logging.warning("Unexpected GraphQL response structure. Falling back to generic data flattening.")
            cleaned_data = [flatten_json(response) for response in responses]
            fieldnames_dutchie = sorted(set().union(*(d.keys() for d in cleaned_data)))

        logging.info(f"Writing Dutchie/cleaned CSV to: {output_file_dutchie}")
        with open(output_file_dutchie, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_dutchie)
            writer.writeheader()
            for item in cleaned_data:
                writer.writerow({k: str(v).replace('\n', ' ').replace('\r', '') for k, v in item.items()})

        logging.info(f"Successfully wrote Dutchie/cleaned data to {output_file_dutchie}")
        logging.info(f"File size: {os.path.getsize(output_file_dutchie)} bytes")
        logging.info(f"File exists: {os.path.exists(output_file_dutchie)}")

        # Generic data flattening
        logging.info("Flattening generic data...")
        flattened_data = [flatten_json(response) for response in responses]
        fieldnames_generic = sorted(set().union(*(d.keys() for d in flattened_data)))

        # Remove blank or whitespace-only columns from generic data
        cleaned_flattened_data = [{k: v for k, v in item.items() if str(v).strip()} for item in flattened_data]
        cleaned_fieldnames_generic = sorted(set().union(*(d.keys() for d in cleaned_flattened_data)))

        logging.info(f"Writing generic CSV to: {output_file_generic}")
        with open(output_file_generic, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=cleaned_fieldnames_generic)
            writer.writeheader()
            for item in cleaned_flattened_data:
                writer.writerow({k: str(v).replace('\n', ' ').replace('\r', '') for k, v in item.items()})

        logging.info(f"Successfully wrote generic data to {output_file_generic}")
        logging.info(f"File size: {os.path.getsize(output_file_generic)} bytes")
        logging.info(f"File exists: {os.path.exists(output_file_generic)}")

        # Unflattened data
        logging.info("Writing unflattened data...")
        fieldnames_unflattened = sorted(set().union(*(d.keys() for d in responses)))

        logging.info(f"Writing unflattened CSV to: {output_file_unflattened}")
        with open(output_file_unflattened, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_unflattened)
            writer.writeheader()
            for response in responses:
                writer.writerow({k: str(v).replace('\n', ' ').replace('\r', '') for k, v in response.items()})

        logging.info(f"Successfully wrote unflattened data to {output_file_unflattened}")
        logging.info(f"File size: {os.path.getsize(output_file_unflattened)} bytes")
        logging.info(f"File exists: {os.path.exists(output_file_unflattened)}")
    except Exception as e:
        logging.error(f"Error in process_graphql_responses: {str(e)}")
        raise


def clean_iheartjane_data(responses: List[Union[Dict[str, Any], List[Any]]]) -> List[Dict[str, Any]]:
    cleaned_data = []
    for response in responses:
        if isinstance(response, dict) and 'hits' in response:
            hits = response['hits']
        elif isinstance(response, list):
            hits = response
        else:
            logging.warning(f"Unexpected response structure: {type(response)}")
            continue

        for hit in hits:
            if isinstance(hit, dict):
                cleaned_item = {
                    'Name': hit.get('name', ''),
                    'Category': hit.get('type', ''),
                    'SubCategory': hit.get('subcategory', ''),
                    'THC': hit.get('thc', {}).get('range', '') if isinstance(hit.get('thc'), dict) else '',
                    'CBD': hit.get('cbd', {}).get('range', '') if isinstance(hit.get('cbd'), dict) else '',
                    'Price': f"${hit.get('price', {}).get('price', '')}" if isinstance(hit.get('price'), dict) else '',
                    'Strain': hit.get('strainType', ''),
                    'Brand': hit.get('brand', {}).get('name', '') if isinstance(hit.get('brand'), dict) else '',
                    'Weight': hit.get('weight', {}).get('label', '') if isinstance(hit.get('weight'), dict) else '',
                    'Type': hit.get('type', ''),
                    'Description': str(hit.get('description', '')).replace('\n', ' ').replace('\r', '')
                }
                cleaned_data.append(cleaned_item)
            else:
                logging.warning(f"Unexpected hit type: {type(hit)}")
    return cleaned_data


def clean_generic_data(responses: List[Union[Dict[str, Any], List[Any]]]) -> List[Dict[str, Any]]:
    cleaned_data = []
    for response in responses:
        if isinstance(response, dict):
            cleaned_item = {}
            for key, value in response.items():
                if isinstance(value, (str, int, float, bool)):
                    cleaned_item[key] = value
                elif isinstance(value, (list, dict)):
                    cleaned_item[key] = json.dumps(value)
            cleaned_data.append(cleaned_item)
        elif isinstance(response, list):
            for item in response:
                if isinstance(item, dict):
                    cleaned_item = {}
                    for key, value in item.items():
                        if isinstance(value, (str, int, float, bool)):
                            cleaned_item[key] = value
                        elif isinstance(value, (list, dict)):
                            cleaned_item[key] = json.dumps(value)
                    cleaned_data.append(cleaned_item)
                else:
                    logging.warning(f"Unexpected item type in list: {type(item)}")
        else:
            logging.warning(f"Unexpected response type: {type(response)}")
    return cleaned_data
