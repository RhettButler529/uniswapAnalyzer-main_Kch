import json
import time
from json import JSONDecodeError

import pandas as pd
from requests import post
from datetime import datetime, timedelta, timezone
import math
from collections import defaultdict

from tqdm import tqdm

from Web3Client import Web3Client
from humanfriendly import format_timespan


class BitqueryClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.headers = {'X-API-KEY': self.api_key}
        infura_url = 'https://mainnet.infura.io/v3/5ba458236d754f25a0e08414fdac066a'

        self.web3 = Web3Client(infura_url)

    def make_request_grapql(self, query: str, variables=None):
        if variables is None:
            variables = {}
        while True:
            try:
                request = post('https://graphql.bitquery.io/',
                               json={'query': query, 'variables': variables}, headers=self.headers)

                return request.json()
            except JSONDecodeError:
                print(query)
                time.sleep(1)
            except KeyError:
                print(query)
                time.sleep(1)

    def get_ether_price(self):
        today = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')
        query = '''
query ($today:ISO8601DateTime!)

{
  ethereum(network: ethereum) {
    dexTrades(
      date: {since: $today}
      baseCurrency: {is: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"}
      quoteCurrency: {is: "0xdac17f958d2ee523a2206206994597c13d831ec7"}
options:{limit:1, desc:"block.height"}
      
    ) {
      timeInterval {
        minute(format: "%FT%TZ", count: 10)
      }
      block{
        height
      }
      price: quotePrice(calculate: average)
    }
  }
}
        '''
        price = self.make_request_grapql(query, variables={'today': today})['data']['ethereum']['dexTrades'][0]['price']
        return price

    def get_n_days_ago(self, n: int):
        utc_now = datetime.now(tz=timezone.utc)
        n_days_ago = utc_now - timedelta(days=n)
        n_days_ago_str = n_days_ago.strftime('%Y-%m-%d')
        return n_days_ago_str

    def get_addresses(self, n_days: int, limit: int):
        n_day_ago = self.get_n_days_ago(n_days)
        query_template = '''
        {
          ethereum(network: ethereum) {
            transactions(
              date: {since: "%s"}
              txTo:{is:"0x7a250d5630b4cf539739df2c5dacb4c659f2488d"}
              options:{desc:"count", limit:25000, offset:%s}
               success:true

            ) {
              count
            
              sender{
                address
              }      
            maximum(of: time)

            }
          }
        }
              '''
        offset = 0
        all_trades = []
        while True:
            query = query_template % (n_day_ago, offset)
            trades = self.make_request_grapql(query)['data']['ethereum']['transactions']
            all_trades.extend(trades)
            offset += 25000
            if trades[-1]['count'] < limit:
                break
        address_dict = {}
        for trade in all_trades:
            address = trade['sender']['address']
            n_trades = trade['count']
            last_transaction_time = datetime.strptime(trade['maximum'], "%Y-%m-%d %H:%M:%S UTC").timestamp()
            address_dict[address] = (n_trades, last_transaction_time)
            if n_trades < limit:
                break

        return address_dict

    def fetch_in_transfers(self, addresses: list, n_days: str):

        query = """
query ($addresses: [String!], $days_ago: ISO8601DateTime!, $offset: Int!) 

{
  ethereum(network: ethereum) {
    transactions(txTo: {in:$addresses}
    date:{since:$days_ago}
    options:{limit:25000, offset:$offset}
    ){
      hash
    }
  }
}

        """
        transaction_hashes = set()
        BULK_SIZE = 100
        N_BULKS = math.ceil(len(addresses) / BULK_SIZE)
        for i in tqdm(range(N_BULKS)):

            addresses_slice = addresses[i * BULK_SIZE:(i + 1) * BULK_SIZE]
            offset = 0

            while True:

                variables = {'addresses': addresses_slice, 'offset': offset, 'days_ago': n_days}
                res = self.make_request_grapql(query, variables)
                transactions = res['data']['ethereum']['transactions']
                for transaction in transactions:
                    transaction_hashes.add(transaction['hash'])
                if len(transactions) < 25000:
                    break
                offset += 25000

        query = """
query ($addresses: [String!], $days_ago: ISO8601DateTime!, $offset: Int!) 

{
  ethereum(network: ethereum) {
    transfers(receiver: {in:$addresses}
            currency: {in: ["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "ETH", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","0xdac17f958d2ee523a2206206994597c13d831ec7","0xa47c8bf37f92abed4a126bda807a7b7498661acd","0x6b175474e89094c44da98b954eedeac495271d0f"]}

    date:{since:$days_ago}
      options:{limit:25000, offset:$offset}
    ){
      transaction{
        hash
      }
      currency{
        address
        symbol
      }
            receiver{
        address
      }
      amount
    }
  }
}

        """

        BULK_SIZE = 100
        N_BULKS = math.ceil(len(addresses) / BULK_SIZE)
        all_transfers  = []
        for i in tqdm(range(N_BULKS)):
            addresses_slice = addresses[i * BULK_SIZE:(i + 1) * BULK_SIZE]
            offset = 0

            while True:
                variables = {'addresses': addresses_slice, 'offset': offset, 'days_ago': n_days}
                res = self.make_request_grapql(query, variables)
                transfers = res['data']['ethereum']['transfers']
                all_transfers.extend(transfers)
                if len(transfers) < 25000:
                    break
                offset += 25000
        filtered_transfers = []


        for transfer in all_transfers:
            tx_hash = transfer['transaction']['hash']
            if tx_hash not in transaction_hashes:
                continue
            filtered_transfers.append(transfer)
        currencies = {'-': 'ETH', '0xdac17f958d2ee523a2206206994597c13d831ec7': 'USDT'
            , '0xa47c8bf37f92abed4a126bda807a7b7498661acd': 'UST', '0x6b175474e89094c44da98b954eedeac495271d0f': 'DAI',
                      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'USDC',
                      '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 'WETH'}

        in_transfers_dict = {i: {} for i in addresses}
        # print(len(filtered_transfers), len(transaction_hashes), len(all_transfers))
        with open('in_transfers.json', 'w') as f:
            json.dump(filtered_transfers, f)
        for transfer in filtered_transfers:
            currency = transfer['currency']['symbol']
            currency_address = transfer['currency']['address']
            receiver = transfer['receiver']['address']
            amount = float(transfer['amount'])
            if currency_address not in currencies or currencies[currency_address] != currency:
                continue
            if receiver not in in_transfers_dict:
                continue
            if currency_address not in in_transfers_dict[receiver]:
                in_transfers_dict[receiver][currency_address] = 0
            in_transfers_dict[receiver][currency_address] += amount

        eth_price = self.get_ether_price()
        ether_amount = {}
        for address, amounts in in_transfers_dict.items():
            ether_amount[address] = 0
            dollar_amount = 0
            for currency, amount in amounts.items():

                if currency == '-' or currency == '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2':
                    ether_amount[address] += amount
                else:
                    dollar_amount += amount
            ether_amount[address] += dollar_amount / eth_price

        return ether_amount

    def fetch_balance_difference(self, addresses: list, n_days: str):
        query = """
                query ($addresses:[String!],$days_ago:ISO8601DateTime!,$offset:Int!)

        {
  ethereum {
    transfers(
      currency: {in: ["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "ETH", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","0xdac17f958d2ee523a2206206994597c13d831ec7","0xa47c8bf37f92abed4a126bda807a7b7498661acd","0x6b175474e89094c44da98b954eedeac495271d0f"]}
      date: {since: $days_ago}
      success: true
      sender:{in:$addresses}
      options:{limit:25000, offset:$offset}
    ) {
      sum_out: amount(calculate: sum)
	    sender{
        address
      }
      gasValue
      currency{
        name
      }
    }
  }
}

        """

        BULK_SIZE = 100
        N_BULKS = math.ceil(len(addresses) / BULK_SIZE)
        all_transfers = []

        for i in tqdm(range(N_BULKS)):
            addresses_slice = addresses[i * BULK_SIZE:(i + 1) * BULK_SIZE]
            offset = 0
            while True:
                variables = {'addresses': addresses_slice, 'offset': offset, 'days_ago': n_days}

                transfers = self.make_request_grapql(query, variables)['data']['ethereum']['transfers']
                all_transfers.extend(transfers)
                if len(transfers) < 25000:
                    break
                offset += 25000

        total_outs = {}
        for trade in all_transfers:
            sender = trade['sender']['address']
            token = trade['currency']['name']
            amount = trade['sum_out']
            if sender not in total_outs:
                total_outs[sender] = {}
            if token not in total_outs[sender]:
                total_outs[sender][token] = 0
            total_outs[sender][token] -= amount

        query = """
                        query ($addresses:[String!],$days_ago:ISO8601DateTime!,$offset:Int!)

                {
          ethereum {
            transfers(
              currency: {in: ["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "ETH", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","0xdac17f958d2ee523a2206206994597c13d831ec7","0xa47c8bf37f92abed4a126bda807a7b7498661acd","0x6b175474e89094c44da98b954eedeac495271d0f"]}
              date: {since: $days_ago}
              success: true
              receiver:{in:$addresses}
              options:{limit:25000, offset:$offset}
            ) {
              sum_out: amount(calculate: sum)
        	receiver{
                address
              }
gasValue
              currency{
                name
              }
            }
          }
        }

                """
        BULK_SIZE = 100
        N_BULKS = math.ceil(len(addresses) / BULK_SIZE)
        all_transfers = []
        for i in tqdm(range(N_BULKS)):
            addresses_slice = addresses[i * BULK_SIZE:(i + 1) * BULK_SIZE]
            offset = 0
            while True:
                variables = {'addresses': addresses_slice, 'offset': offset, 'days_ago': n_days}
                transfers = self.make_request_grapql(query, variables)['data']['ethereum']['transfers']
                all_transfers.extend(transfers)
                if len(transfers) < 25000:
                    break
                offset += 25000
        for trade in all_transfers:
            receiver = trade['receiver']['address']
            token = trade['currency']['name']
            amount = trade['sum_out']
            if receiver not in total_outs:
                total_outs[receiver] = {}
            if token not in total_outs[receiver]:
                total_outs[receiver][token] = 0
            total_outs[receiver][token] += amount
        gas_query = """
            query ($addresses:[String!],$days_ago:ISO8601DateTime!)

        {
  ethereum(network: ethereum) {
    transactions(
    date:{since:$days_ago}
    txSender:{in:$addresses}
    
    ){
      gasValue
      
       sender{
        address
      }
    }
  }
}

        """
        for i in tqdm(range(N_BULKS)):
            addresses_slice = addresses[i * BULK_SIZE:(i + 1) * BULK_SIZE]

            variables = {'addresses': addresses_slice, 'days_ago': n_days}
            transactions = self.make_request_grapql(gas_query, variables)['data']['ethereum']['transactions']
            for transaction in transactions:
                sender = transaction['sender']['address']
                gas = transaction['gasValue']
                if sender not in total_outs:
                    total_outs[sender] = {'Ether': 0}
                if 'Ether' not in total_outs[sender]:
                    total_outs[sender]['Ether'] = 0
                total_outs[sender]['Ether'] -= gas

        ether_difference = {}
        ether_price = self.get_ether_price()
        for address, balance in total_outs.items():
            ether_difference[address] = 0
            stablecoin_amount = 0
            for token, amount in balance.items():
                if token == 'Ether' or token == 'Wrapped Ether':
                    ether_difference[address] += amount
                else:
                    stablecoin_amount += amount
            ether_difference[address] += stablecoin_amount / ether_price
        return ether_difference

    def fetch_out_transfers(self, addresses: list, n_days: str):

        query = """
        query ($addresses:[String!],$days_ago:ISO8601DateTime!,$offset:Int!)


{
  ethereum(network: ethereum) {
    transfers(
      txFrom: {in: $addresses}
      date: {since: $days_ago}    
            success:true
  
      options: {limit: 25000, asc: "count", offset: $offset}
    ) {
      transaction {
        hash
      }
	sender:any(of:sender)
      count
      currency_out: any(of: currency_symbol)
      currency_address: any(of: currency_address)
      amount: maximum(of: amount)
    }
  }
}

        """
        all_transfers = []
        BULK_SIZE = 100
        N_BULKS = math.ceil(len(addresses) / BULK_SIZE)
        for i in tqdm(range(N_BULKS)):
            addresses_slice = addresses[i * BULK_SIZE:(i + 1) * BULK_SIZE]
            offset = 0

            while True:

                variables = {'addresses': addresses_slice, 'offset': offset, 'days_ago': n_days}
                res = self.make_request_grapql(query, variables)
                transfers = res['data']['ethereum']['transfers']
                all_transfers.extend(transfers)
                if len(transfers) < 25000 or transfers[-1]['count'] != 1:
                    break
                offset += 25000
        currencies = {'-': 'ETH', '0xdac17f958d2ee523a2206206994597c13d831ec7': 'USDT'
            , '0xa47c8bf37f92abed4a126bda807a7b7498661acd': 'UST', '0x6b175474e89094c44da98b954eedeac495271d0f': 'DAI',
                      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'USDC',
                      '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 'WETH'}

        out_transfers_dict = {i: {} for i in addresses}

        for transfer in all_transfers:
            currency = transfer['currency_out']
            currency_address = transfer['currency_address']
            sender = transfer['sender']
            amount = float(transfer['amount'])
            count = transfer['count']
            if currency_address not in currencies or currencies[currency_address] != currency or count != 1:
                continue
            if sender not in out_transfers_dict:
                continue
            if currency_address not in out_transfers_dict[sender]:
                out_transfers_dict[sender][currency_address] = 0
            out_transfers_dict[sender][currency_address] += amount

        eth_price = self.get_ether_price()
        ether_amount = {}
        for address, amounts in out_transfers_dict.items():
            ether_amount[address] = 0
            dollar_amount = 0
            for currency, amount in amounts.items():

                if currency == '-' or currency == '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2':
                    ether_amount[address] += amount
                else:
                    dollar_amount += amount
            ether_amount[address] += dollar_amount / eth_price

        return ether_amount

    def get_non_weth_trades_count(self, addresses: list, n_days: str):
        query = """
        
        query ($addresses:[String!],$days_ago:ISO8601DateTime!,$offset:Int!)

{
  ethereum(network: ethereum) {
    dexTrades(
      txSender: {in: $addresses}
     buyCurrency:{not:"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"}
      sellCurrency:{not:"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"}
    
      options: {limit: 25000, offset: $offset}
      date:{since:$days_ago}
      exchangeAddress :{is:"0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"}
    ) {

      transaction{
        hash
        txFrom{
          address
        }
       
      }
      buyCurrency{
        address
        name
      }
      sellCurrency{
        address
        name
      }
      
    }
  }
}
        """

        all_trades = []
        BULK_SIZE = 100
        N_BULKS = math.ceil(len(addresses) / BULK_SIZE)
        for i in tqdm(range(N_BULKS)):
            addresses_slice = addresses[i * BULK_SIZE:(i + 1) * BULK_SIZE]
            offset = 0
            while True:
                variables = {'addresses': addresses_slice, 'offset': offset, 'days_ago': n_days}
                trades = self.make_request_grapql(query, variables)['data']['ethereum']['dexTrades']
                all_trades.extend(trades)
                if len(trades) < 25000:
                    break
                offset += 25000

        weth_trades_dict = {i: 0 for i in addresses}
        for trade in all_trades:
            sender = trade['transaction']['txFrom']['address']
            if sender not in weth_trades_dict:
                continue
            weth_trades_dict[sender] += 1
        return weth_trades_dict

    def get_df(self, limit: int, days: int, last_tx_time: int, n_visible_rows: int):
        days_ago = self.get_n_days_ago(days)
        address_list = self.get_addresses(days, limit)
        txns_dict = {}
        last_txn_time_dict = {}

        last_txn_timestamp_limit = (
                datetime.utcnow() - timedelta(seconds=last_tx_time * 3600)).timestamp()
        for address in address_list:
            n_txns, last_txn_time = address_list[address]
            if last_txn_time > last_txn_timestamp_limit:
                txns_dict[address] = n_txns
                last_txn_time_dict[address] = last_txn_time

        non_weth_trades_dict = self.get_non_weth_trades_count(list(txns_dict.keys()), days_ago)
        out_transfers_dict = self.fetch_out_transfers(list(txns_dict.keys()), days_ago)
        balance_difference_dict = self.fetch_balance_difference(list(txns_dict.keys()), days_ago)
        in_transfers_dict = self.fetch_in_transfers(list(txns_dict.keys()), days_ago)

        data_for_df = []
        current_time = datetime.utcnow().timestamp()
        for address in txns_dict:
            balance_difference = balance_difference_dict[address]
            in_transfers = in_transfers_dict[address]
            out_transfers = out_transfers_dict[address]
            profit = balance_difference - in_transfers + out_transfers
            n_txns = txns_dict[address]
            last_txn_time = format_timespan(current_time - (last_txn_time_dict[address]))
            weth_trades = n_txns - non_weth_trades_dict[address]
            percentage = round(weth_trades / n_txns * 100)
            data_for_df.append([0, address, n_txns, profit, last_txn_time, weth_trades, percentage])
        data_for_df.sort(key=lambda x: x[3], reverse=True)
        data_for_df = data_for_df[:n_visible_rows]
        for ind, i in enumerate(data_for_df):
            i[0] = ind + 1
        df = pd.DataFrame(data_for_df, columns=['Rank', 'Address', 'N transactions', 'Profit', 'Last txn time',
                                                'ETH/WETH trades', '% ETH/WETH trades'])
        return df
