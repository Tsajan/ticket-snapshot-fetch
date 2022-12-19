import json
import time
import csv
import numpy as np
import pandas as pd
from web3 import Web3
import requests
from web3.middleware import geth_poa_middleware
from dotenv import dotenv_values

config = dotenv_values('.env')
holder_wallet_addresses = []

GOLD_TICKET_ID = 1
SILVER_TICKET_ID = 2
BRONZE_TICKET_ID = 3

POLYGON_SCAN_API = "https://api.polygonscan.com/api"

def loadTowerInventoryContractABI():
    with open('ABI/TowerInventory.json') as ABIObject:
        contractABI = json.load(ABIObject)
    return contractABI

def txnListForContract():
    params = {
        
    }


def main():
    startTime = time.time()
    print("Start time: ", startTime)
    polygon_provider = config['POLYGON_PROVIDER_URL']
    tower_inventory_address = config['POLYGON_TOWER_INVENTORY_CONTRACT']

    w3 = Web3(Web3.HTTPProvider(polygon_provider))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    latestBlock = w3.eth.get_block('latest')
    print("Latest polygon block is: ", latestBlock)

    tower_inventory_address = Web3.toChecksumAddress(tower_inventory_address)
    tower_inventory_contract_abi = loadTowerInventoryContractABI()
    tower_inventory_instance = w3.eth.contract(address=tower_inventory_address, abi=tower_inventory_contract_abi)

    contract_name = tower_inventory_instance.functions.name().call()
    print("Contract name is: ", contract_name)

if __name__ == '__main__':
    main()



