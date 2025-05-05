# Inside main.py
import os
import sys
import requests
import json
import time  # Import time for potential delays
from dotenv import load_dotenv

# --- Configuration Loading ---

def load_config():
    """Loads required configuration from the .env file."""
    load_dotenv()
    config = {
        "moralis_api_key": os.getenv("MORALIS_API_KEY"),
        "eth_wallet": os.getenv("ETH_WALLET_ADDRESS"),
        "op_wallet": os.getenv("OP_WALLET_ADDRESS"),
***REMOVED***
    if not all(config.values()):
        print("Error: Missing configuration in .env file.")
        print("Please ensure MORALIS_API_KEY, ETH_WALLET_ADDRESS, and OP_WALLET_ADDRESS are set.")
        sys.exit(1)
    print("Configuration loaded successfully.")
    return config

# --- Moralis API Interaction ---

MORALIS_API_BASE_URL = "https://deep-index.moralis.io/api/v2.2"

def get_wallet_history(api_key, wallet_address, chain):
    """
    Fetches complete wallet transaction history from Moralis API, handling pagination.

    Args:
        api_key (str): Your Moralis API key.
        wallet_address (str): The wallet address to query.
        chain (str): The chain identifier ('eth' for Ethereum, 'optimism' for Optimism).

    Returns:
        list: A list containing all transaction results, or None if an error occurs.
    """
    all_results = ***REMOVED******REMOVED***
    cursor = None
    headers = {
        "accept": "application/json",
        "X-API-Key": api_key,
***REMOVED***
    endpoint = f"{MORALIS_API_BASE_URL}/wallets/{wallet_address}/history"

    print(f"Fetching history for {wallet_address} on {chain}...")

    while True:
        params = {
            "chain": chain,
            "order": "DESC",  # Or "ASC" if you prefer chronological order
            # Add other params like 'from_date', 'to_date' if needed
    ***REMOVED***
        if cursor:
            params***REMOVED***"cursor"***REMOVED*** = cursor

        try:
            response = requests.get(endpoint, headers=headers, params=params)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

            data = response.json()
            page_results = data.get("result", ***REMOVED******REMOVED***)
            if page_results:
                all_results.extend(page_results)
                print(f"  Fetched {len(page_results)} transactions (Total: {len(all_results)})")

            # Check for the next cursor
            cursor = data.get("cursor")
            if not cursor:
                print(f"  No more pages for {wallet_address} on {chain}. Fetch complete.")
                break  # Exit loop if no cursor is provided

            # Optional: Add a small delay to avoid hitting rate limits agressively
            # time.sleep(0.2) # Sleep for 200 milliseconds

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {wallet_address} on {chain}: {e}")
            if response is not None:
                 print(f"Response status: {response.status_code}")
                 print(f"Response text: {response.text***REMOVED***:500***REMOVED***}...") # Print first 500 chars of error
            return None # Indicate error
        except json.JSONDecodeError:
            print(f"Error decoding JSON response for {wallet_address} on {chain}.")
            print(f"Response text: {response.text***REMOVED***:500***REMOVED***}...")
            return None # Indicate error
        except Exception as e:
            print(f"An unexpected error occurred for {wallet_address} on {chain}: {e}")
            return None # Indicate error


    return all_results

# --- Data Saving ---

def save_data_to_json(data, filename):
    """Saves the provided data structure to a JSON file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Successfully saved data to {filename}")
    except IOError as e:
        print(f"Error saving data to {filename}: {e}")
    except TypeError as e:
        print(f"Error serializing data to JSON for {filename}: {e}")


# --- Main Execution ---

if __name__ == "__main__":
    print("Starting DAO Accountant - Data Fetching...")
    config = load_config()

    # Define output filenames
    eth_output_file = "eth_wallet_history.json"
    op_output_file = "op_wallet_history.json"

    # --- Fetch Ethereum Wallet History ---
    eth_history = get_wallet_history(
        config***REMOVED***"moralis_api_key"***REMOVED***,
        config***REMOVED***"eth_wallet"***REMOVED***,
        "eth"  # Moralis chain identifier for Ethereum Mainnet
    )
    if eth_history is not None:
        save_data_to_json(eth_history, eth_output_file)

    print("-" * 20) # Separator

    # --- Fetch Optimism Wallet History ---
    op_history = get_wallet_history(
        config***REMOVED***"moralis_api_key"***REMOVED***,
        config***REMOVED***"op_wallet"***REMOVED***,
        "optimism"  # Moralis chain identifier for Optimism
    )
    if op_history is not None:
        save_data_to_json(op_history, op_output_file)

    print("-" * 20)
    print("Data fetching complete. Raw data saved to JSON files.")