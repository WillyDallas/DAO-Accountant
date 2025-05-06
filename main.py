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
        "eth_wallet": eth_wallet_addr if eth_wallet_addr else None,
        "op_wallet": op_wallet_addr if op_wallet_addr else None,
    }
    # Check after potential lowercasing
    if not config["eth_wallet"] or not config["op_wallet"]:
        print("Error: Missing configuration in .env file.")
        print("Please ensure ETH_WALLET_ADDRESS, and OP_WALLET_ADDRESS are set.")
        sys.exit(1)

    print("Configuration loaded successfully.")
    print(f"  ETH Wallet: {config['eth_wallet']}")
    print(f"  OP Wallet: {config['op_wallet']}")
    return config

# Define base URLs for Safe Transaction Service APIs
SAFE_TRANSACTION_SERVICE_BASE_URLS = {
    "optimism": "https://safe-transaction-optimism.safe.global/api/v2/safes/",
    "eth": "https://safe-transaction-mainnet.safe.global/api/v2/safes/"
}

def get_safe_transactions_url(wallet_address, chain):
    """Constructs the URL for fetching Safe transactions."""
    base_url = SAFE_TRANSACTION_SERVICE_BASE_URLS.get(chain)
    if not base_url:
        return None
    return f"{base_url}{wallet_address}/all-transactions/"

def get_safe_transactions(wallet_address, chain):
    """
    Fetches transactions from the Safe Transaction Service API.
    
    Args:
        wallet_address (str): The wallet address to query.
        chain (str): The chain identifier ('eth' or 'optimism').
        
    Returns:
        list: A list containing all transaction results, or an empty list if an error occurs.
    """
    url = get_safe_transactions_url(wallet_address, chain)
    if not url:
        print(f"Error: No base URL defined for chain '{chain}'")
        return []
    
    all_results = []
    next_page = url
    page_count = 0
    
    print(f"Fetching Safe transactions for {wallet_address} on {chain}...")
    
    headers = {
        "accept": "application/json"
    }
    
    while next_page:
        page_count += 1
        try:
            print(f"  Fetching page {page_count} from {next_page}...")
            response = requests.get(next_page, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            page_results = data.get("results", [])
            
            if page_results:
                all_results.extend(page_results)
                print(f"    Fetched {len(page_results)} transactions (Total: {len(all_results)})")
            
            # Check for next page
            next_page = data.get("next")
            
            # Small delay to avoid overwhelming the API
            if next_page:
                time.sleep(0.5)
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {wallet_address} on {chain}: {e}")
            if response is not None: 
                print(f"Response status: {response.status_code}\nResponse text: {response.text[:500]}...")
            return all_results  # Return what we have so far
        except json.JSONDecodeError:
            print(f"Error decoding JSON for {wallet_address} on {chain}.\nResponse text: {response.text[:500]}...")
            return all_results  # Return what we have so far
        except Exception as e:
            print(f"An unexpected error occurred for {wallet_address} on {chain}: {e}")
            return all_results  # Return what we have so far
    
    print(f"Finished fetching {len(all_results)} total transactions for {wallet_address} on {chain}.")
    return all_results

# --- Data Saving/Loading (Keep as is) ---
def save_data_to_json(data, filename):
    """Saves the provided data structure to a JSON file."""
    try:
        # Remove the 'data' field from each transaction as it's unnecessary
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and 'data' in item:
                    item.pop('data', None)
        
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

def parse_safe_transactions(transactions, wallet_address, chain):
    """
    Parses raw Safe transactions into a structured format for accounting.

    Args:
        transactions (list): A list of raw transaction dicts from Safe Transaction Service.
        wallet_address (str): The address of the Safe wallet being analyzed.
        chain (str): The chain identifier (e.g., 'eth', 'optimism').

    Returns:
        list: A list of parsed transaction dicts.
    """
    parsed_txs = []
    native_currency_symbols = {
        "eth": "ETH",
        "optimism": "ETH"  # Optimism's native token for gas is ETH
    }
    # Ensure wallet_address is lowercase for consistent comparisons
    wallet_address_lower = wallet_address.lower()

    for tx in transactions:
        tx_hash_main = tx.get('transactionHash') or tx.get('txHash')
        
        execution_date_raw = tx.get('executionDate')
        if not execution_date_raw:
            # print(f"Info: Missing executionDate for transaction {tx_hash_main}. Skipping.")
            continue
        try:
            # The API returns UTC dates like "2025-05-05T06:19:11Z"
            execution_date_dt = datetime.strptime(execution_date_raw, "%Y-%m-%dT%H:%M:%SZ")
            execution_date_str = execution_date_dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            # print(f"Warning: Invalid executionDate format '{execution_date_raw}' for transaction {tx_hash_main}. Skipping.")
            continue
            
        tx_type = tx.get('txType')
        # isSuccessful: True if successful, False if reverted. Can be None if not executed.
        # Default to True if field is missing, as some incoming tx might not have it.
        is_tx_successful = tx.get('isSuccessful', True) 

        tx_fee_native_decimal = Decimal(0)
        # Fees are typically associated with transactions initiated by the Safe (MULTISIG, MODULE)
        if tx_type in ["MULTISIG_TRANSACTION", "MODULE_TRANSACTION"] and tx.get('fee'):
            try:
                # Fee is in native token (e.g., ETH) and needs 10^18 division
                tx_fee_native_decimal = Decimal(tx['fee']) / (Decimal(10) ** 18)
            except (TypeError, ValueError, InvalidOperation) as e:
                print(f"Warning: Could not parse fee '{tx['fee']}' for tx {tx_hash_main}: {e}")

        processed_any_transfer_for_this_wallet = False

        # --- Process Transfers (ERC20, ETHER, ERC721, etc.) ---
        # Only process transfers if the main transaction itself was successful,
        # as a failed Safe-initiated tx shouldn't result in asset movement *from* the Safe.
        # Incoming transfers (txType: ETHEREUM_TRANSACTION) are usually successful by definition of being on-chain.
        if tx.get('transfers') and (is_tx_successful or tx_type == "ETHEREUM_TRANSACTION"):
            for transfer_index, transfer in enumerate(tx['transfers']):
                transfer_from_raw = transfer.get('from')
                transfer_to_raw = transfer.get('to')
                
                if not transfer_from_raw or not transfer_to_raw:
                    # print(f"Info: Skipping transfer with missing from/to in tx {tx_hash_main}")
                    continue
                
                transfer_from_lower = transfer_from_raw.lower()
                transfer_to_lower = transfer_to_raw.lower()

                direction = None
                counterparty_raw = None

                if transfer_from_lower == wallet_address_lower:
                    direction = "OUT"
                    counterparty_raw = transfer_to_raw
                elif transfer_to_lower == wallet_address_lower:
                    direction = "IN"
                    counterparty_raw = transfer_from_raw
                else:
                    # This transfer doesn't directly involve the Safe wallet as the primary sender or receiver.
                    # This can happen with complex interactions or if the API includes related internal transfers.
                    # For the Safe's direct accounting, these are typically ignored unless a deeper analysis is needed.
                    continue
                
                processed_any_transfer_for_this_wallet = True
                counterparty = counterparty_raw.lower() if counterparty_raw else 'N/A'

                amount_value_str = transfer.get('value', '0')
                if amount_value_str is None: amount_value_str = '0'
                
                current_transfer_amount_decimal = Decimal(0)
                currency_symbol = "N/A"
                # Use a more specific base description if possible
                transfer_type_str = transfer.get('type', 'UNKNOWN_TRANSFER').replace('_', ' ')
                description_base = transfer_type_str


                if transfer.get('type') == "ERC20_TRANSFER":
                    token_info = transfer.get('tokenInfo')
                    if token_info:
                        decimals = token_info.get('decimals')
                        currency_symbol = token_info.get('symbol', 'UnknownERC20')
                        description_base = f"ERC20 Transfer {currency_symbol}"
                        if decimals is not None:
                            try:
                                current_transfer_amount_decimal = Decimal(amount_value_str) / (Decimal(10) ** int(decimals))
                            except (ValueError, InvalidOperation, TypeError):
                                print(f"Warning: Could not parse ERC20 value '{amount_value_str}' (decimals: {decimals}) for {currency_symbol} in tx {tx_hash_main or transfer.get('transactionHash') }")
                        else: # decimals is None
                             current_transfer_amount_decimal = Decimal(amount_value_str) # Cannot adjust
                             print(f"Warning: ERC20 token {currency_symbol} (addr: {token_info.get('address')}) missing decimals. Amount_Raw will be unadjusted.")
                    else: # No tokenInfo
                        currency_symbol = transfer.get('tokenAddress', 'UnknownERC20Addr')
                        description_base = f"ERC20 Transfer {currency_symbol}"
                        try: # Try to convert value, but can't adjust for decimals
                            current_transfer_amount_decimal = Decimal(amount_value_str)
                        except (ValueError, InvalidOperation, TypeError): pass
                
                elif transfer.get('type') == "ETHER_TRANSFER":
                    currency_symbol = native_currency_symbols.get(chain, "ETH")
                    description_base = f"Native Transfer {currency_symbol}"
                    try:
                        current_transfer_amount_decimal = Decimal(amount_value_str) / (Decimal(10) ** 18)
                    except (ValueError, InvalidOperation, TypeError): pass
                
                elif transfer.get('type') in ["ERC721_TRANSFER", "ERC1155_TRANSFER"]:
                    token_info = transfer.get('tokenInfo')
                    currency_symbol = token_info.get('symbol', 'NFT') if token_info else 'NFT'
                    try: 
                        current_transfer_amount_decimal = Decimal(amount_value_str) # 'value' for ERC1155 is amount, for ERC721 it's often 0 or tokenId itself (if numeric)
                        if transfer.get('type') == "ERC721_TRANSFER" and current_transfer_amount_decimal == 0 : # For ERC721, amount is typically 1 unit
                            current_transfer_amount_decimal = Decimal(1)
                    except (ValueError, InvalidOperation, TypeError):
                         current_transfer_amount_decimal = Decimal(1) # Default for NFT count if value is not numeric
                    
                    token_id_str = transfer.get('tokenId', '')
                    description_base = f"{transfer_type_str} {currency_symbol} (ID: {token_id_str})"
                else:
                    # print(f"Info: Skipping unknown transfer type: {transfer.get('type')} in tx {tx_hash_main or transfer.get('transactionHash')}")
                    continue

                description_final = description_base
                # Append method from dataDecoded if it exists, to provide more context
                has_transfer_method = False
                if tx.get('dataDecoded') and tx['dataDecoded'].get('method'):
                    method_name = tx['dataDecoded']['method']
                    description_final += f" (Method: {method_name})"
                    has_transfer_method = method_name == "transfer"
                
                # Fee association:
                # For ETHEREUM_TRANSACTION, the Safe doesn't pay the fee from its assets.
                # For MULTISIG/MODULE, the fee is paid by the Safe.
                fee_for_this_record_str = "0.0"
                if tx_type in ["MULTISIG_TRANSACTION", "MODULE_TRANSACTION"]:
                    fee_for_this_record_str = str(tx_fee_native_decimal)
                
                # Get token info for further checks
                token_info = None
                token_name = ""
                if transfer.get('type') == "ERC20_TRANSFER" and transfer.get('tokenInfo'):
                    token_info = transfer.get('tokenInfo')
                    token_name = token_info.get('name', '')
                
                # Check if it looks like USDT
                looks_like_usdt = "USD" in currency_symbol.upper() or "USD" in token_name.upper()
                exact_match_usdt = currency_symbol == "USDT"
                is_zero_fee = fee_for_this_record_str == "0.0"
                
                # More reliable detection of fake tokens - use a whitelist approach
                is_authentic_usdt = False
                
                # Define the exact standard USDT token addresses we trust (lowercase)
                trusted_usdt_addresses = {
                    # Standard USDT on Ethereum and Optimism
                    "0xdac17f958d2ee523a2206206994597c13d831ec7", # ETH Mainnet USDT
                    "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58"  # Optimism USDT
                }
                
                # Check if this is from a trusted token address
                if token_info and token_info.get('address'):
                    token_address = token_info.get('address', '').lower()
                    if token_address in trusted_usdt_addresses:
                        is_authentic_usdt = True
                
                # Store original debug info calculation here, but the main print block will be later
                _debug_contains_non_ascii_symbol = False
                _debug_contains_non_ascii_name = False
                _debug_symbol_char_codes = []
                _debug_name_char_codes = []

                if looks_like_usdt and is_zero_fee: # Only populate these if the main debug log will trigger
                    try:
                        currency_symbol.encode('ascii')
                    except UnicodeEncodeError:
                        _debug_contains_non_ascii_symbol = True
                    if token_name:
                        try:
                            token_name.encode('ascii')
                        except UnicodeEncodeError:
                            _debug_contains_non_ascii_name = True
                    for char_s in currency_symbol: _debug_symbol_char_codes.append(ord(char_s))
                    if token_name:
                        for char_n in token_name: _debug_name_char_codes.append(ord(char_n))
                
                # General non-ASCII checks for filtering logic (these are distinct from debug-specific ones)
                contains_non_ascii_symbol_filter = False
                try:
                    currency_symbol.encode('ascii')
                except UnicodeEncodeError:
                    contains_non_ascii_symbol_filter = True
                
                contains_non_ascii_name_filter = False
                if token_name:
                    try:
                        token_name.encode('ascii')
                    except UnicodeEncodeError:
                        contains_non_ascii_name_filter = True
                
                # IMPROVED FILTERING LOGIC
                is_fake_token = False 

                if not is_authentic_usdt:
                    # Criterion 1: Contains non-ASCII characters in a short symbol (potential homoglyph attack)
                    if contains_non_ascii_symbol_filter or contains_non_ascii_name_filter:
                        if 3 <= len(currency_symbol) <= 5: # Symbol length typical for "USDT" fakes
                            is_fake_token = True
                            # print(f"DEBUG Filter: Flagged fake by Criterion 1 (non-ASCII in short symbol) for '{currency_symbol}'")

                    # Criterion 2: Uses characters from suspicious Unicode blocks
                    if not is_fake_token:
                        for char_to_check in currency_symbol + token_name:
                            code = ord(char_to_check)
                            if ((0x0400 <= code <= 0x04FF) or  # Cyrillic
                                (0x0370 <= code <= 0x03FF) or  # Greek
                                (0x0500 <= code <= 0x052F) or  # Cyrillic Supplement
                                (0x2000 <= code <= 0x206F) or  # General Punctuation (many spaces, zero-width chars)
                                (0x1D400 <= code <= 0x1D7FF) or # Mathematical Alphanumeric Symbols
                                (0xFF00 <= code <= 0xFFEF)):   # Halfwidth and Fullwidth Forms
                                normalized_alnum_symbol = "".join(filter(str.isalnum, currency_symbol)).upper()
                                # If suspicious char found, and it generally looks like USDT by length or normalized content
                                if (len(currency_symbol) <= 5 and ("USD" in normalized_alnum_symbol or normalized_alnum_symbol == "USDT")):
                                    is_fake_token = True
                                    # print(f"DEBUG Filter: Flagged fake by Criterion 2 (Unicode block '{chr(code)}') for '{currency_symbol}'")
                                    break 
                    
                    # Criterion 3: Symbol string is "USDT" but uses non-ASCII char codes
                    if not is_fake_token and currency_symbol == "USDT": # Naive string comparison
                        expected_ascii_codes = [ord('U'), ord('S'), ord('D'), ord('T')]
                        actual_codes = [ord(c) for c in currency_symbol]
                        if actual_codes != expected_ascii_codes:
                            is_fake_token = True
                            # print(f"DEBUG Filter: Flagged fake by Criterion 3 ('USDT' string with non-ASCII chars) for '{currency_symbol}'")

                    # Criterion 4: Normalizes to "USDT" but original string is different
                    if not is_fake_token:
                        # 4a: After removing all non-alphanumeric and uppercasing
                        normalized_alnum_upper = "".join(filter(str.isalnum, currency_symbol)).upper()
                        if normalized_alnum_upper == "USDT" and currency_symbol != "USDT":
                            is_fake_token = True
                            # print(f"DEBUG Filter: Flagged fake by Criterion 4a (alnum normalization) for '{currency_symbol}'")
                        
                        # 4b: After basic space removal (ASCII space U+0020) and uppercasing
                        if not is_fake_token: # check only if not already flagged by 4a
                            normalized_no_std_space_upper = currency_symbol.replace(" ", "").upper()
                            if normalized_no_std_space_upper == "USDT" and currency_symbol != "USDT":
                                is_fake_token = True
                                # print(f"DEBUG Filter: Flagged fake by Criterion 4b (std space normalization) for '{currency_symbol}'")
                    
                    # Criterion 5: Zero-fee transfers for USDT-like tokens that are not authentic
                    if not is_fake_token and is_zero_fee and looks_like_usdt and not exact_match_usdt:
                        # `looks_like_usdt` is true if "USD" in symbol.upper() or name.upper()
                        # `exact_match_usdt` is true if symbol is exactly "USDT" (naive)
                        # This catches things like "USDTX", "SUSDT" that are zero fee & not the plain "USDT" string.
                        is_fake_token = True
                        # print(f"DEBUG Filter: Flagged fake by Criterion 5 (zero_fee for non-exact but similar USDT) for '{currency_symbol}'")

                # Moved and enhanced debug print block, triggered if it looks like USDT and has zero fee (common for fakes)
                if looks_like_usdt and is_zero_fee:
                    print("\n======= SUSPICIOUS USDT DEBUG INFO (POST-FILTERING) =======")
                    print(f"Transaction Hash: {tx_hash_main or transfer.get('transactionHash')}")
                    print(f"Currency Symbol: '{currency_symbol}' (Length: {len(currency_symbol)})")
                    print(f"Token Name: '{token_name}'")
                    print(f"Token Address: {token_info.get('address') if token_info else 'N/A'}")
                    print(f"Fee: {fee_for_this_record_str}")
                    
                    print("Symbol Character codes: ", end="")
                    for char_code in _debug_symbol_char_codes: print(f"'{chr(char_code)}':{char_code} ", end="")
                    print()
                    if token_name:
                        print("Token Name Character codes: ", end="")
                        for char_code in _debug_name_char_codes: print(f"'{chr(char_code)}':{char_code} ", end="")
                        print()
                    
                    # Use the debug-specific non-ASCII flags calculated earlier for this print
                    print(f"Symbol can encode as ASCII (debug check): {'NO' if _debug_contains_non_ascii_symbol else 'YES'}")
                    if token_name: print(f"Name can encode as ASCII (debug check): {'NO' if _debug_contains_non_ascii_name else 'YES'}")
                    
                    print(f"--- Filtering Inputs ---")
                    print(f"  `looks_like_usdt` (orig check): {looks_like_usdt}")
                    print(f"  `exact_match_usdt` (orig check): {exact_match_usdt}")
                    print(f"  `is_authentic_usdt` (whitelisted): {is_authentic_usdt}")
                    print(f"  `contains_non_ascii_symbol_filter`: {contains_non_ascii_symbol_filter}")
                    print(f"  `contains_non_ascii_name_filter`: {contains_non_ascii_name_filter}")
                    print(f"--- FINAL DECISION ---")
                    print(f"  `is_fake_token`: {is_fake_token}")
                    if is_fake_token:
                        print("  STATUS: This token was FLAGGED AS FAKE and will be filtered.")
                    else:
                        print("  STATUS: This token was NOT FLAGGED AS FAKE by current logic.")
                    print("===========================================================\n")
                        
                if is_fake_token:
                    print(f"Filtering out FAKE/SUSPICIOUS token transfer: Symbol='{currency_symbol}', Name='{token_name}', TxHash={transfer.get('transactionHash', tx_hash_main)}")
                    continue
                
                parsed_txs.append({
                    "Date": execution_date_str,
                    "WalletAddress": wallet_address_lower,
                    "Chain": chain,
                    "Direction": direction,
                    "Amount_Raw": str(current_transfer_amount_decimal),
                    "Currency": currency_symbol,
                    "Description": description_final,
                    "TransactionHash": transfer.get('transactionHash', tx_hash_main), # Prefer transfer-specific hash if available
                    "Fee_Native": fee_for_this_record_str,
                    "Counterparty": counterparty
                })

        # --- Handle Non-Transfer Transactions by the Safe (e.g., approvals, settings changes) OR Failed Safe Txs with Fees ---
        if not processed_any_transfer_for_this_wallet and tx_type in ["MULTISIG_TRANSACTION", "MODULE_TRANSACTION"]:
            # This is a Safe-initiated operation that didn't result in a direct transfer TO/FROM this wallet
            # (e.g., an approval, a setting change) OR it's a failed transaction by the Safe that incurred a fee.
            
            description = "Safe Operation"
            method_name = "" # Default method name
            if tx.get('dataDecoded') and tx['dataDecoded'].get('method'):
                method_name = tx['dataDecoded']['method']
                description = f"Contract Interaction: {method_name}"
                if method_name.lower() == 'approve' and tx.get('to') and tx['dataDecoded'].get('parameters'):
                    params = tx['dataDecoded']['parameters']
                    spender_param = params[0].get('value', 'Unknown Spender') if len(params) > 0 else 'Unknown Spender'
                    # approved_value_raw = params[1].get('value', 'Unknown Value') if len(params) > 1 else 'Unknown Value'
                    description = f"Token Approval for {tx['to']} to {spender_param}" # Token address is tx['to']
            
            if not is_tx_successful: # If the transaction failed
                 description = f"Failed {description}"

            # Record this if:
            # 1. It's a failed transaction with a fee.
            # 2. It's a successful non-transfer operation (like an approval).
            if tx_fee_native_decimal > 0 or is_tx_successful:
                 counterparty_raw = tx.get('to') # The contract interacted with by the Safe
                 counterparty = counterparty_raw.lower() if counterparty_raw else 'N/A'
                 parsed_txs.append({
                    "Date": execution_date_str,
                    "WalletAddress": wallet_address_lower,
                    "Chain": chain,
                    "Direction": "OUT", # Safe initiated, implies an outgoing action / fee payment
                    "Amount_Raw": "0.0",
                    "Currency": "N/A", # No direct value transfer in this specific entry
                    "Description": description,
                    "TransactionHash": tx_hash_main,
                    "Fee_Native": str(tx_fee_native_decimal), # Fee is relevant
                    "Counterparty": counterparty
                })
        
        # Note: ETHEREUM_TRANSACTION types that are direct native ETH deposits *to* the Safe
        # are expected to be covered by the `transfers` loop (as ETHER_TRANSFER).
        # If there's a scenario where they are not, specific logic would be needed here.
        # The Safe Transaction Service data usually includes an ETHER_TRANSFER for such cases.
    
    return parsed_txs

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
    if eth_history is None:
        print(f"Fetching Ethereum wallet history as {eth_output_file} was missing or empty...")
        eth_history = get_safe_transactions(config["eth_wallet"], "eth")
        if eth_history:
            save_data_to_json(eth_history, eth_output_file)
        else:
            print("Failed to fetch Ethereum history. Continuing without it.")
            eth_history = []
            
    # Display Safe Transaction Service URL for reference
    eth_safe_url = get_safe_transactions_url(config["eth_wallet"], "eth")
    if eth_safe_url:
        print(f"Ethereum Safe Transaction Service URL: {eth_safe_url}")
    
    print("-" * 20)

    op_history = load_data_from_json(op_output_file)
    if op_history is None:
        print(f"Fetching Optimism wallet history as {op_output_file} was missing or empty...")
        op_history = get_safe_transactions(config["op_wallet"], "optimism")
        if op_history:
            save_data_to_json(op_history, op_output_file)
        else:
            print("Failed to fetch Optimism history. Continuing without it.")
            op_history = []
            
    # Display Safe Transaction Service URL for reference
    op_safe_url = get_safe_transactions_url(config["op_wallet"], "optimism")
    if op_safe_url:
        print(f"Optimism Safe Transaction Service URL: {op_safe_url}")
        
    print("-" * 20)

    # # --- Parse the data ---
    print("\nParsing transaction data...")
    eth_parsed = parse_safe_transactions(eth_history, config["eth_wallet"], "eth")
    op_parsed = parse_safe_transactions(op_history, config["op_wallet"], "optimism")

    # # --- Create separate reports for each chain ---
    eth_output_csv = os.path.join(data_dir, "eth_dao_transactions_report.csv")
    op_output_csv = os.path.join(data_dir, "op_dao_transactions_report.csv")
    
    # # Process Ethereum transactions
    print("\nProcessing Ethereum transactions report...")
    if not eth_parsed:
        print("No relevant Ethereum transactions were found. CSV will be empty.")
        eth_df = pd.DataFrame(columns=[
            "Date", "WalletAddress", "Chain", "Direction", "Amount_Raw", "Currency",
            "TransactionHash", "Fee_Native", "Description", "Counterparty"
        ])
    else:
        eth_df = pd.DataFrame(eth_parsed)
        eth_df['Date'] = pd.to_datetime(eth_df['Date'])
        eth_df = eth_df.sort_values(by="Date", ascending=True)
        eth_df['Date'] = eth_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        eth_df = eth_df[[
            "Date", "WalletAddress", "Chain", "Direction", "Amount_Raw", "Currency",
            "TransactionHash", "Fee_Native", "Description", "Counterparty"
        ]]
        print(f"\nGenerated Ethereum DataFrame with {len(eth_df)} rows.")

    # # Process Optimism transactions
    print("\nProcessing Optimism transactions report...")
    if not op_parsed:
        print("No relevant Optimism transactions were found. CSV will be empty.")
        op_df = pd.DataFrame(columns=[
            "Date", "WalletAddress", "Chain", "Direction", "Amount_Raw", "Currency",
            "TransactionHash", "Fee_Native", "Description", "Counterparty"
        ])
    else:
        op_df = pd.DataFrame(op_parsed)
        op_df['Date'] = pd.to_datetime(op_df['Date'])
        op_df = op_df.sort_values(by="Date", ascending=True)
        op_df['Date'] = op_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        op_df = op_df[[
            "Date", "WalletAddress", "Chain", "Direction", "Amount_Raw", "Currency",
            "TransactionHash", "Fee_Native", "Description", "Counterparty"
        ]]
        print(f"\nGenerated Optimism DataFrame with {len(op_df)} rows.")

    # # --- Export to CSVs ---
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