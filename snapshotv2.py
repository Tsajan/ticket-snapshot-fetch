import json
import time
import csv
import numpy as np
import pandas as pd
from web3 import Web3
import requests
from web3.middleware import geth_poa_middleware
from dotenv import dotenv_values
from datetime import datetime

config = dotenv_values('.env')
wallet_addresses = []
BASE_METADATA_URL = 'https://public.nftstatic.com/static/nft/BSC/BRNFT/'

header_row = ['TokenId', 'WalletAddr', 'isGold', 'isSilver', 'isBronze']

data_rows = []

goldTicketCount = 0
silverTicketCount = 0
bronzeTicketCount = 0

def loadContractABI():
    with open('ABI/Tickets.json') as ABIObject:
        contractABI = json.load(ABIObject)
    return contractABI

def main():
    startTime = time.time()
    dateToday = datetime.today().strftime("%Y%m%d")
    outputFile = 'ticketMapping' + dateToday + '.csv'
    bsc_provider = config['BSC_PROVIDER_URL']
    brnft_address = config['BRNFT_CONTRACT_ADDRESS']

    print("Start time: ", startTime)
    print("Writing results to: ", outputFile)

    w3 = Web3(Web3.HTTPProvider(bsc_provider))
    # we need to use a middleware since BSC is a PoA consensus chain
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    brnft_contract_address = Web3.toChecksumAddress(brnft_address)
    brnft_contract_abi = loadContractABI()
    ticket_contract = w3.eth.contract(address=brnft_contract_address, abi=brnft_contract_abi)

    # write the header row
    with open(outputFile, "w") as file:
        writer = csv.writer(file)
        writer.writerow(header_row)

    # hard-coded values for bronze tickets that were checked with BRNFT values; limit specified by its contract
    bronzeTicketRange = range(100300227506, 100300228106)
    print("Length of bronzeTicketRange is: ", len(bronzeTicketRange))

    # hard-coded values for silver ticket that were checked with BRNFT values; limit specified by its contract
    silverTicketRange = range(100300228427, 100300228727)
    print("Length of silver ticket range is: ", len(silverTicketRange))

    # hard-coded values for gold tickets that were checked with BRNFT values; limit specified by its contract
    goldTicketRange = range(100300228731, 100300228834)
    print("Length of gold ticket range is: ", len(goldTicketRange))

    for bronzeTicket in bronzeTicketRange:
        fetchTokenDetails(ticket_contract, bronzeTicket)

    for silverTicket in silverTicketRange:
        fetchTokenDetails(ticket_contract, silverTicket)

    for goldTicket in goldTicketRange:
        fetchTokenDetails(ticket_contract, goldTicket)

    print("Gold: %d, Silver: %d, Bronze: %d" % (goldTicketCount, silverTicketCount, bronzeTicketCount))

    # print the data rows
    print(data_rows)

    with open(outputFile, "a") as file:
        writer = csv.writer(file)
        for row in data_rows:
            writer.writerow(row)

    endTime = time.time()
    print("End time is: ", endTime)



def fetchTokenDetails(ticket_contract, tokenId):
    TOKEN_METADATA_URL = BASE_METADATA_URL + str(tokenId)
    
    tokenMetadataResponse = requests.get(TOKEN_METADATA_URL)
    # time.sleep(2)
    
    global goldTicketCount
    global silverTicketCount
    global bronzeTicketCount

    ownerWalletAddr = ''

    try:
        ownerWalletAddr = ticket_contract.functions.ownerOf(tokenId).call()
    except:
        print("Owner of wallet address not found")
        ownerWalletAddr = '0xNOTFOUND'

    # time.sleep(2)
    print("Token %s is owned by: %s" % (tokenId, ownerWalletAddr))

    if(tokenMetadataResponse.status_code == 200):
        tokenMetadata = tokenMetadataResponse.json()

        if(tokenMetadata['name'] == 'Gold TOWER Ticket'):
            goldTicketCount = goldTicketCount + 1
            dataRow = [tokenId, ownerWalletAddr,1,0,0]
            data_rows.append(dataRow)
        elif(tokenMetadata['name'] == 'Silver TOWER Ticket'):
            silverTicketCount = silverTicketCount + 1
            dataRow = [tokenId, ownerWalletAddr,0,1,0]
            data_rows.append(dataRow)
        elif(tokenMetadata['name'] == 'Bronze TOWER Ticket'):
            bronzeTicketCount = bronzeTicketCount + 1
            dataRow = [tokenId, ownerWalletAddr,0,0,1]
            data_rows.append(dataRow)
    
    time.sleep(1)
    
if __name__ == "__main__":
    main()