import os
import sys
import requests
import json
import time
import pandas as pd
from datetime import datetime
from decimal import Decimal, getcontext # Use Decimal for precision
from dotenv import load_dotenv
import pathlib  # Import for directory creation

# Set Decimal precision (adjust if needed)
getcontext().prec = 50

# --- Configuration Loading ---

def load_config():
    """Loads required configuration from the .env file."""
    load_dotenv()
    # **FIX 1: Store addresses in lowercase**
    eth_wallet_addr = os.getenv("ETH_WALLET_ADDRESS")
    op_wallet_addr = os.getenv("OP_WALLET_ADDRESS")

    config = {
        "moralis_api_key": os.getenv("MORALIS_API_KEY"),
        "eth_wallet": eth_wallet_addr.lower() if eth_wallet_addr else None,
        "op_wallet": op_wallet_addr.lower() if op_wallet_addr else None,
    }
    # Check after potential lowercasing
    if not config["moralis_api_key"] or not config["eth_wallet"] or not config["op_wallet"]:
        print("Error: Missing configuration in .env file.")
        print("Please ensure MORALIS_API_KEY, ETH_WALLET_ADDRESS, and OP_WALLET_ADDRESS are set.")
        sys.exit(1)

    print("Configuration loaded successfully.")
    print(f"  ETH Wallet (lowercase): {config['eth_wallet']}")
    print(f"  OP Wallet (lowercase): {config['op_wallet']}")
    return config

# --- Moralis API Interaction (Keep as is) ---
MORALIS_API_BASE_URL = "https://deep-index.moralis.io/api/v2.2"

def get_wallet_history(api_key, wallet_address, chain):
    """ Fetches complete wallet transaction history from Moralis API, handling pagination. """
    all_results = []
    cursor = None
    headers = { "accept": "application/json", "X-API-Key": api_key }
    endpoint = f"{MORALIS_API_BASE_URL}/wallets/{wallet_address}/history"
    print(f"Fetching history for {wallet_address} on {chain}...")
    page_count = 0
    while True:
        page_count += 1
        params = { "chain": chain, "order": "DESC", "include_internal_transactions": "true" }
        if cursor:
            params["cursor"] = cursor
        try:
            print(f"  Fetching page {page_count} for {wallet_address} on {chain} (Cursor: {'Yes' if cursor else 'No'})...")
            response = requests.get(endpoint, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            page_results = data.get("result", [])
            if page_results:
                all_results.extend(page_results)
                print(f"    Fetched {len(page_results)} transactions (Total: {len(all_results)})")
            cursor = data.get("cursor")
            if not cursor:
                print(f"  No more pages for {wallet_address} on {chain}. Fetch complete.")
                break
            time.sleep(0.25) # Small delay
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {wallet_address} on {chain}: {e}")
            if response is not None: print(f"Response status: {response.status_code}\nResponse text: {response.text[:500]}...")
            return None
        except json.JSONDecodeError:
            print(f"Error decoding JSON for {wallet_address} on {chain}.\nResponse text: {response.text[:500]}...")
            return None
        except Exception as e:
            print(f"An unexpected error occurred for {wallet_address} on {chain}: {e}")
            return None
    print(f"Finished fetching {len(all_results)} total transactions for {wallet_address} on {chain}.")
    return all_results


# --- Data Saving/Loading (Keep as is) ---
def save_data_to_json(data, filename):
    """Saves the provided data structure to a JSON file."""
    try:
        pathlib.Path(os.path.dirname(filename)).mkdir(parents=True, exist_ok=True) # Ensure directory exists
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Successfully saved data to {filename}")
    except Exception as e:
        print(f"Error saving data to {filename}: {e}")

def load_data_from_json(filename):
    """Loads data from a JSON file."""
    if not os.path.exists(filename):
        print(f"Info: Data file {filename} not found.")
        return None
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not data: # Check if the file loaded but was empty
            print(f"Info: Data file {filename} is empty.")
            return None
        print(f"Successfully loaded data from {filename}")
        return data
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {filename}.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred loading {filename}: {e}")
        return None


# --- Parsing Logic ---

# Define known Aave contracts (Example - Add actual Optimism addresses)
AAVE_CONTRACTS_OPTIMISM = {
    "0x6ab707aca953edaefbc4fd23ba73294241490620": "Aave v3 Pool", # Example aOptUSDT token
    "0x794a61358d6845594f94dc1db02a252b5b4814ad": "Aave v3 Pool Proxy" # Main Pool contract on OP
}
AAVE_CONTRACTS_ETHEREUM = {
    # "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9": "Aave v2 Lending Pool"
}
KNOWN_AAVE_CONTRACTS = {
    'eth': AAVE_CONTRACTS_ETHEREUM,
    'optimism': AAVE_CONTRACTS_OPTIMISM
}

def parse_transactions(transactions, wallet_address, chain):
    """ Parses raw transaction data from Moralis into a structured format for CSV. """
    parsed_data = []
    native_symbol = "ETH" if chain == "eth" else "OP"
    aave_contracts_on_chain = KNOWN_AAVE_CONTRACTS.get(chain, {})
    processed_hashes = set() # Keep track of hashes we added rows for

    if not transactions:
        print(f"No transactions provided to parse for {wallet_address} on {chain}.")
        return []

    print(f"Parsing {len(transactions)} transactions for {wallet_address} on {chain}...")

    for tx_index, tx in enumerate(transactions):
        tx_hash = tx.get("hash")
        timestamp_str = tx.get("block_timestamp")

        # Simple progress indicator
        if (tx_index + 1) % 100 == 0:
            print(f"  Processing transaction {tx_index + 1}/{len(transactions)} (Hash: {tx_hash[:10]}...)")

        if not tx_hash or not timestamp_str:
            # print(f"  Skipping transaction at index {tx_index} due to missing hash or timestamp.")
            continue

        try:
            dt_object = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except Exception as e:
            print(f"Warning: Could not parse timestamp '{timestamp_str}' for tx {tx_hash}. Skipping. Error: {e}")
            continue

        date_formatted = dt_object.strftime("%Y-%m-%d %H:%M:%S")

        tx_fee_str = tx.get("transaction_fee")
        fee_native = Decimal(0)
        try:
            if tx_fee_str and tx_fee_str.lower() != "nan":
                fee_native = Decimal(tx_fee_str)
            else:
                gas_used = Decimal(tx.get("receipt_gas_used", 0))
                gas_price = Decimal(tx.get("gas_price", 0))
                if gas_used > 0 and gas_price > 0:
                    fee_native = (gas_used * gas_price) / Decimal(10**18)
        except Exception as e:
            print(f"Warning: Could not calculate fee for tx {tx_hash}. Setting fee to 0. Error: {e}")
            fee_native = Decimal(0)

        # --- Process ERC20 Transfers ---
        for erc20 in tx.get("erc20_transfers", []):
            token_symbol = erc20.get("token_symbol")
            # **FIX 2: Check for None before lower()**
            from_addr_raw = erc20.get("from_address")
            to_addr_raw = erc20.get("to_address")
            from_addr = from_addr_raw.lower() if from_addr_raw else None
            to_addr = to_addr_raw.lower() if to_addr_raw else None

            value_str = erc20.get("value")
            decimals_str = erc20.get("token_decimals")
            is_spam = erc20.get("possible_spam", False)
            # is_verified = erc20.get("verified_contract", False) # Keep for potential stricter filtering

            if from_addr != wallet_address and to_addr != wallet_address:
                 continue # Skip if wallet not involved

            # --- Spam Filtering ---
            if is_spam:
                # print(f"  Skipping spam ERC20: {token_symbol} in tx {tx_hash}")
                continue

            if not value_str or decimals_str is None: # Check decimals explicitly for None
                 # print(f"  Skipping ERC20 {token_symbol} due to missing value/decimals in tx {tx_hash}")
                 continue

            try:
                value_raw = Decimal(value_str)
                decimals = int(decimals_str)
                # Handle potential zero decimals correctly
                amount_raw = value_raw / (Decimal(10) ** decimals) if decimals >= 0 else value_raw
            except (ValueError, TypeError, ArithmeticError) as e:
                 print(f"  Warning: Could not parse value/decimals for ERC20 {token_symbol} in tx {tx_hash}. Value='{value_str}', Decimals='{decimals_str}'. Skipping transfer. Error: {e}")
                 continue

            direction = None
            counterparty = None
            description = ""

            if from_addr == wallet_address:
                direction = "OUT"
                counterparty = to_addr if to_addr else "Unknown"
                description = f"Send {amount_raw:.8f} {token_symbol} to {counterparty}"
            elif to_addr == wallet_address:
                direction = "IN"
                counterparty = from_addr if from_addr else "Unknown"
                description = f"Receive {amount_raw:.8f} {token_symbol} from {counterparty}"

            # Check for Aave interaction (only if direction was determined)
            if direction:
                aave_label = None
                if counterparty in aave_contracts_on_chain:
                    aave_label = aave_contracts_on_chain[counterparty]
                    description += f" ({aave_label})"
                    if "aOpt" in token_symbol or "aeth" in token_symbol: # Sending aToken usually means Withdraw
                        description = f"Aave Withdrawal of {amount_raw:.8f} underlying, receiving {token_symbol}"
                    else: # Sending base token to Aave means Deposit
                        description = f"Aave Deposit of {amount_raw:.8f} {token_symbol}"
                elif from_addr in aave_contracts_on_chain:
                    aave_label = aave_contracts_on_chain[from_addr]
                    description += f" (from {aave_label})"
                    if "aOpt" in token_symbol or "aeth" in token_symbol: # Receiving aToken means Deposit finished
                        description = f"Aave Deposit resulting in {amount_raw:.8f} {token_symbol}"
                    else: # Receiving base token from Aave means Withdraw finished
                        description = f"Aave Withdrawal of {amount_raw:.8f} {token_symbol}"

                parsed_data.append({
                    "Date": date_formatted, "WalletAddress": wallet_address, "Chain": chain,
                    "Direction": direction, "Amount_Raw": float(amount_raw), "Currency": token_symbol,
                    "Amount_USDT_Estimate": None, "TransactionHash": tx_hash,
                    "Fee_Native": float(fee_native), "Description": description,
                    "Counterparty": counterparty
                })
                processed_hashes.add(tx_hash)

        # --- Process Native Transfers ---
        for native in tx.get("native_transfers", []):
             # **FIX 2: Check for None before lower()**
            from_addr_raw = native.get("from_address")
            to_addr_raw = native.get("to_address")
            from_addr = from_addr_raw.lower() if from_addr_raw else None
            to_addr = to_addr_raw.lower() if to_addr_raw else None

            value_str = native.get("value")

            if from_addr != wallet_address and to_addr != wallet_address:
                continue # Skip if wallet not involved

            if not value_str: continue

            try:
                amount_raw = Decimal(value_str) / (Decimal(10)**18)
            except (ValueError, TypeError) as e:
                print(f"  Warning: Could not parse value for native transfer in tx {tx_hash}. Skipping. Error: {e}")
                continue

            if amount_raw == 0: continue

            direction = None
            counterparty = None
            description = ""

            if from_addr == wallet_address:
                direction = "OUT"
                counterparty = to_addr if to_addr else "Unknown"
                description = f"Send {amount_raw:.8f} {native_symbol} to {counterparty}"
            elif to_addr == wallet_address:
                direction = "IN"
                counterparty = from_addr if from_addr else "Unknown"
                description = f"Receive {amount_raw:.8f} {native_symbol} from {counterparty}"

            if direction:
                if counterparty in aave_contracts_on_chain:
                     description += f" ({aave_contracts_on_chain[counterparty]})"

                parsed_data.append({
                   "Date": date_formatted, "WalletAddress": wallet_address, "Chain": chain,
                   "Direction": direction, "Amount_Raw": float(amount_raw), "Currency": native_symbol,
                   "Amount_USDT_Estimate": None, "TransactionHash": tx_hash,
                   "Fee_Native": float(fee_native), "Description": description,
                   "Counterparty": counterparty
                })
                processed_hashes.add(tx_hash)

        # --- Fallback for Contract Interactions initiated by wallet ---
        # Only add if no other transfers for this hash were added AND wallet initiated
        main_from_addr_raw = tx.get("from_address")
        main_from_addr = main_from_addr_raw.lower() if main_from_addr_raw else None

        if tx_hash not in processed_hashes and main_from_addr == wallet_address:
            to_addr_raw = tx.get("to_address")
            to_addr = to_addr_raw.lower() if to_addr_raw else "Unknown Contract"
            description = f"Contract Interaction with {to_addr}"
            category = tx.get("category", "contract interaction") # Use Moralis category

            aave_label = aave_contracts_on_chain.get(to_addr)
            if aave_label:
                description += f" ({aave_label})"
                category = "Aave Interaction"

            # Add specific checks for common methods if needed (e.g., approvals)
            # method_label = tx.get("method_label")
            # if method_label == "approve": description = f"Approve token for {to_addr}"

            parsed_data.append({
                "Date": date_formatted, "WalletAddress": wallet_address, "Chain": chain,
                "Direction": "OUT", "Amount_Raw": 0.0, "Currency": "-",
                "Amount_USDT_Estimate": None, "TransactionHash": tx_hash,
                "Fee_Native": float(fee_native), "Description": description,
                "Counterparty": to_addr
            })
            # We don't add to processed_hashes here, as this is a fallback

    print(f"Finished parsing. Generated {len(parsed_data)} data rows for {wallet_address} on {chain}.")
    return parsed_data


# --- Main Execution ---

if __name__ == "__main__":
    config = load_config()
    data_dir = "data"
    eth_output_file = os.path.join(data_dir, "eth_wallet_history.json")
    op_output_file = os.path.join(data_dir, "op_wallet_history.json")

    # Ensure data directory exists
    pathlib.Path(data_dir).mkdir(exist_ok=True)

    # --- Load or Fetch Data ---
    eth_history = load_data_from_json(eth_output_file)
    # **FIX 3: Fetch if load failed or returned empty**
    if eth_history is None:
        print(f"Fetching Ethereum wallet history as {eth_output_file} was missing or empty...")
        eth_history = get_wallet_history(config["moralis_api_key"], config["eth_wallet"], "eth")
        if eth_history is not None:
            save_data_to_json(eth_history, eth_output_file)
        else:
            print("Failed to fetch Ethereum history. Continuing without it.")
            eth_history = [] # Use empty list to avoid errors later
    print("-" * 20)

    op_history = load_data_from_json(op_output_file)
    # **FIX 3: Fetch if load failed or returned empty**
    if op_history is None:
        print(f"Fetching Optimism wallet history as {op_output_file} was missing or empty...")
        op_history = get_wallet_history(config["moralis_api_key"], config["op_wallet"], "optimism")
        if op_history is not None:
            save_data_to_json(op_history, op_output_file)
        else:
            print("Failed to fetch Optimism history. Continuing without it.")
            op_history = [] # Use empty list
    print("-" * 20)

    # --- Parse the data ---
    print("\nParsing transaction data...")
    # Pass the lowercase addresses from config
    eth_parsed = parse_transactions(eth_history, config["eth_wallet"], "eth")
    op_parsed = parse_transactions(op_history, config["op_wallet"], "optimism")

    # --- Create separate reports for each chain ---
    eth_output_csv = os.path.join(data_dir, "eth_dao_transactions_report.csv")
    op_output_csv = os.path.join(data_dir, "op_dao_transactions_report.csv")
    
    # Process Ethereum transactions
    print("\nProcessing Ethereum transactions report...")
    if not eth_parsed:
        print("No relevant Ethereum transactions were found. CSV will be empty.")
        eth_df = pd.DataFrame(columns=[
            "Date", "WalletAddress", "Chain", "Direction", "Amount_Raw", "Currency",
            "Amount_USDT_Estimate", "TransactionHash", "Fee_Native", "Description", "Counterparty"
        ])
    else:
        eth_df = pd.DataFrame(eth_parsed)
        eth_df['Date'] = pd.to_datetime(eth_df['Date'])
        eth_df = eth_df.sort_values(by="Date", ascending=True)
        eth_df['Date'] = eth_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        eth_df = eth_df[[
            "Date", "WalletAddress", "Chain", "Direction", "Amount_Raw", "Currency",
            "Amount_USDT_Estimate", "TransactionHash", "Fee_Native", "Description", "Counterparty"
        ]]
        print(f"\nGenerated Ethereum DataFrame with {len(eth_df)} rows.")

    # Process Optimism transactions
    print("\nProcessing Optimism transactions report...")
    if not op_parsed:
        print("No relevant Optimism transactions were found. CSV will be empty.")
        op_df = pd.DataFrame(columns=[
            "Date", "WalletAddress", "Chain", "Direction", "Amount_Raw", "Currency",
            "Amount_USDT_Estimate", "TransactionHash", "Fee_Native", "Description", "Counterparty"
        ])
    else:
        op_df = pd.DataFrame(op_parsed)
        op_df['Date'] = pd.to_datetime(op_df['Date'])
        op_df = op_df.sort_values(by="Date", ascending=True)
        op_df['Date'] = op_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        op_df = op_df[[
            "Date", "WalletAddress", "Chain", "Direction", "Amount_Raw", "Currency",
            "Amount_USDT_Estimate", "TransactionHash", "Fee_Native", "Description", "Counterparty"
        ]]
        print(f"\nGenerated Optimism DataFrame with {len(op_df)} rows.")

    # --- Export to CSVs ---
    try:
        eth_df.to_csv(eth_output_csv, index=False, encoding='utf-8')
        print(f"\nSuccessfully exported Ethereum report to {eth_output_csv}")
    except Exception as e:
        print(f"\nError writing Ethereum CSV file: {e}")
        
    try:
        op_df.to_csv(op_output_csv, index=False, encoding='utf-8')
        print(f"\nSuccessfully exported Optimism report to {op_output_csv}")
    except Exception as e:
        print(f"\nError writing Optimism CSV file: {e}")