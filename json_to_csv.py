import csv
import json
from typing import List, Dict, Any
import logging
from urllib.parse import urlparse


def is_dutchie_menu(data: List[Dict[str, Any]], graphql_url: str) -> bool:
    parsed_url = urlparse(graphql_url)
    return parsed_url.netloc.endswith('dutchie.com')


def clean_dutchie_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned_data = []
    for item in data:
        if 'data' in item and 'menu' in item['data']:
            for product in item['data']['menu']:
                variants = product.get('variants', [])
                prices = [variant.get('price') for variant in variants if variant.get('price') is not None]
                price_range = f"${min(prices)}-${max(prices)}" if prices else "N/A"
                if prices and min(prices) == max(prices):
                    price_range = f"${min(prices)}"

                cleaned_product = {
                    'Name': product.get('name', ''),
                    'Category': product.get('category', {}).get('name', ''),
                    'SubCategory': product.get('subCategory', {}).get('name', ''),
                    'THC': product.get('thc', {}).get('range', ''),
                    'CBD': product.get('cbd', {}).get('range', ''),
                    'Price': price_range,
                    'Strain': product.get('strain', {}).get('name', ''),
                    'Brand': product.get('brand', {}).get('name', ''),
                    'Weight': ', '.join([f"{variant.get('option')} - ${variant.get('price')}" for variant in variants if
                                         variant.get('option') and variant.get('price')]),
                    'Type': product.get('strainType', ''),
                    'Description': product.get('description', '').replace('\n', ' ').replace('\r', '')
                }
                cleaned_data.append(cleaned_product)
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


def process_graphql_responses(responses: List[Dict[str, Any]], output_file: str, graphql_url: str) -> None:
    if not responses:
        logging.warning("No GraphQL responses to process.")
        return

    is_dutchie = is_dutchie_menu(responses, graphql_url)
    logging.info(f"Dutchie menu detected: {is_dutchie}")

    if is_dutchie:
        logging.info("Using Dutchie data cleaning.")
        cleaned_data = clean_dutchie_data(responses)
        fieldnames = ['Name', 'Category', 'SubCategory', 'THC', 'CBD', 'Price', 'Strain', 'Brand', 'Weight', 'Type',
                      'Description']
    else:
        logging.info("Using generic data flattening.")
        cleaned_data = [flatten_json(response) for response in responses]
        fieldnames = sorted(set().union(*(d.keys() for d in cleaned_data)))

    if not cleaned_data:
        logging.warning("No data to write to CSV after cleaning.")
        return

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in cleaned_data:
                writer.writerow(item)
        logging.info(f"Successfully wrote data to {output_file}")
    except IOError as e:
        logging.error(f"IOError while writing CSV: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error while writing CSV: {str(e)}")
