from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_abi.packed import encode_abi_packed

token_abi = open('tokenabi.json', 'r').read().replace('\n', '')

infura_url = 'https://mainnet.infura.io/v3/5ba458236d754f25a0e08414fdac066a'


class Web3Client:
    def __init__(self, infura_url):
        self.web3 = Web3(Web3.HTTPProvider(infura_url))
        self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)

    def get_price_from_uniswap_factory(self, token: str):
        try:
            pair_address = self.get_pair_address(token)

            price = self.get_price_from_uniswap(pair_address, token)
        except:
            try:
                pair_address = self.get_pair_address_usdc(token)
                price = self.get_price_from_uniswap(pair_address, token)
                usdc_price = self.get_price_from_uniswap(pair_address, token)
                price = price * usdc_price
            except:
                pair_address = self.get_pair_address_usdc(token)
                price = self.get_price_from_uniswap(pair_address, token)
                usdt_price = self.get_price_from_uniswap(pair_address, token)
                price = price * usdt_price
        return price

    def get_price_from_uniswap(self, pair_address: str, token_address: str):
        pair_address = Web3.toChecksumAddress(pair_address)
        token_address = Web3.toChecksumAddress(token_address)
        pair_contract = self.web3.eth.contract(address=pair_address, abi=token_abi)

        pair0_token = pair_contract.functions.token0().call()
        (reserve0, reserve1, blockTimestampLast) = pair_contract.functions.getReserves().call()

        if str(pair0_token) == str(token_address):
            token_amount = reserve0
            weth_amount = reserve1

        else:
            token_amount = reserve1
            weth_amount = reserve0

        weth_amount = weth_amount * 1e-18

        token_contract = self.web3.eth.contract(address=token_address, abi=token_abi)
        token_decimals = token_contract.functions.decimals().call()

        token_amount = token_amount * 10 ** (-token_decimals)

        return weth_amount / token_amount

    def get_pair_address(self, token: str):
        weth = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        uniswap_factory = '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f'

        token_b = weth
        pair_traded = [token, token_b]  # token_a, token_b are the address's
        pair_traded.sort()
        hexadem_ = '0x96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f'
        abiEncoded_1 = encode_abi_packed(['address', 'address'], (
            pair_traded[0], pair_traded[1]))
        salt_ = Web3.solidityKeccak(['bytes'], ['0x' + abiEncoded_1.hex()])
        abiEncoded_2 = encode_abi_packed(['address', 'bytes32'], (uniswap_factory, salt_))
        resPair = Web3.solidityKeccak(['bytes', 'bytes'], ['0xff' + abiEncoded_2.hex(), hexadem_])[12:]
        return resPair.hex()

    def get_pair_address_usdc(self, token: str):
        usdc = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        uniswap_factory = '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f'

        token_b = usdc
        pair_traded = [token, token_b]  # token_a, token_b are the address's
        pair_traded.sort()
        hexadem_ = '0x96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f'
        abiEncoded_1 = encode_abi_packed(['address', 'address'], (
            pair_traded[0], pair_traded[1]))
        salt_ = Web3.solidityKeccak(['bytes'], ['0x' + abiEncoded_1.hex()])
        abiEncoded_2 = encode_abi_packed(['address', 'bytes32'], (uniswap_factory, salt_))
        resPair = Web3.solidityKeccak(['bytes', 'bytes'], ['0xff' + abiEncoded_2.hex(), hexadem_])[12:]
        return resPair.hex()

    def get_pair_address_usdt(self, token: str):
        usdt = '0xdac17f958d2ee523a2206206994597c13d831ec7'
        uniswap_factory = '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f'

        token_b = usdt
        pair_traded = [token, token_b]  # token_a, token_b are the address's
        pair_traded.sort()
        hexadem_ = '0x96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f'
        abiEncoded_1 = encode_abi_packed(['address', 'address'], (
            pair_traded[0], pair_traded[1]))
        salt_ = Web3.solidityKeccak(['bytes'], ['0x' + abiEncoded_1.hex()])
        abiEncoded_2 = encode_abi_packed(['address', 'bytes32'], (uniswap_factory, salt_))
        resPair = Web3.solidityKeccak(['bytes', 'bytes'], ['0xff' + abiEncoded_2.hex(), hexadem_])[12:]
        return resPair.hex()
