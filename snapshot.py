import json
import time
import csv
import numpy as np
from web3 import Web3
import requests
from web3.middleware import geth_poa_middleware
from dotenv import dotenv_values

config = dotenv_values('.env')
wallet_addresses = []
BASE_METADATA_URL = 'https://public.nftstatic.com/static/nft/BSC/BRNFT/'

header_row = ['Address', 'GoldTicketCount', 'SilverTicketCount', 'BronzeTicketCount']


def loadContractABI():
    with open('ABI/Tickets.json') as ABIObject:
        contractABI = json.load(ABIObject)
    return contractABI

def BRNFTTransactionsFetch(address, startBlock=8432148, endBlock=99999999, pageNum=1, offset=50):
    PARAMS = {
        "module": "account",
        "action": "tokennfttx",
        "contractaddress": address,
        "startBlock": str(startBlock),
        "endBlock": str(endBlock),
        "page": str(pageNum),
        "offset": str(offset),
        "sort": "asc",
        "apikey": "V1JQ3R9CF1KVP7C6UIAPCQ2YHHP8MT8IFH",
    }

    global wallet_addresses

    response = requests.get('https://api.bscscan.com/api', params=PARAMS)

    print("Sleeping for 5 seconds")
    time.sleep(5)

    if(response.status_code == 200):
        data = response.json()
        if(data['status'] == '1'):
            result = data['result']
            print(result[-1])
            for item in result:
                if(item['tokenSymbol'] == "BRNFT"):
                    senderAddress = item['from']
                    receiverAddress = item['to']

                    if(senderAddress not in wallet_addresses):
                        wallet_addresses.append(senderAddress)

                    if(receiverAddress not in wallet_addresses):
                        wallet_addresses.append(receiverAddress)

            lastElement = result[-1]
            totalTxnCount = len(result)
            print("Offset is: %d \t Result count is: %d" % (offset, totalTxnCount))
            if(totalTxnCount < offset):
                print("All transactions have been fetched")
                resp = { "completed": True, "nextStartBlock": "", "nextPageNum": "" }
                print("Response is: ", resp)
                return resp
            else:
                nextStartBlock = lastElement['blockNumber']
                print("More results on the way!")
                resp = { "completed": False, "nextStartBlock": nextStartBlock }
                print("Response is: ", resp)
                return resp
        else:
            return { "completed": "error" }
    else:
        return { "completed": "error" } 

def main():
    bsc_provider = config['BSC_PROVIDER_URL']
    brnft_address = config['BRNFT_CONTRACT_ADDRESS']

    w3 = Web3(Web3.HTTPProvider(bsc_provider))
    # we need to use a middleware since BSC is a PoA consensus chain
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    brnft_contract_address = Web3.toChecksumAddress(brnft_address)
    brnft_contract_abi = loadContractABI()
    ticket_contract = w3.eth.contract(address=brnft_contract_address, abi=brnft_contract_abi)

    startBlockNum =  8432148
    blockLatest = w3.eth.get_block('latest')
    latestBlockNum=  str(blockLatest['number'])

    '''
    pageNum = 1
    resp = BRNFTTransactionsFetch(brnft_address, startBlockNum, latestBlockNum, pageNum, offset=10000)
    
    while(resp['completed'] == False):
        startBlockNum = resp['nextStartBlock']
        resp = BRNFTTransactionsFetch(brnft_address, startBlockNum, latestBlockNum, pageNum, offset=10000)
    '''

    # after fetching wallet addresses loop through each wallet address and find the contract balance
    # using Ticket contract abi
    print("Total revelavant addresses: ", len(wallet_addresses))
    print("Addresses: " , wallet_addresses)

    # Saving the wallet addresses to a CSV file using numpy
    npWalletAddress = np.array([wallet_addresses])
    transposedNPWalletAddres = npWalletAddress.T
    np.savetxt("addresses2.csv", transposedNPWalletAddres, delimiter=",", fmt="%s")
    
    print("Relevant wallet addresses saved to addresses.csv")
    print("Getting TOWER Ticket details for each addresses")
    fetchTicketDetails(ticket_contract)

def fetchTicketDetails(ticket_contract):
    print("In function fetchTicketDetails")
    wallet_addresses = []

    with open("addresses.csv", "r") as f:
        addresses = f.readlines()
        print(len(addresses))
        for addr in addresses:
            addr = addr.strip()
            wallet_addresses.append(addr)

    print("Wallet address count: ", len(wallet_addresses))

    # write the header row
    with open("balances.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(header_row)

        for address in wallet_addresses:
            print("Fetching token details for %s" %(address))
            goldTicketCount = 0
            silverTicketCount = 0
            bronzeTicketCount = 0

            if(address == "0x0000000000000000000000000000000000000000"):
                continue
            # Skipping the wallet address 0xe0a9e5b59701a776575fdd6257c3f89ae362629a because it has 5500+ tokens
            # will test later
            elif(address == "0xe0a9e5b59701a776575fdd6257c3f89ae362629a"):
                continue
            else:
                address = Web3.toChecksumAddress(address)
                walletNFTBalance = ticket_contract.functions.balanceOf(address).call()
                time.sleep(2)
                # if the wallet address has NFTs, then its balance should have been > 0
                if(walletNFTBalance > 0):
                    for tokenIndex in range(walletNFTBalance):
                        tokenId = ticket_contract.functions.tokenOfOwnerByIndex(address, tokenIndex).call()
                        time.sleep(2)
                        TOKEN_METADATA_URL = BASE_METADATA_URL + str(tokenId)
                        tokenMetadataResponse = requests.get(TOKEN_METADATA_URL)

                        if (tokenMetadataResponse.status_code == 200):
                            tokenMetadata = tokenMetadataResponse.json()
                            if(tokenMetadata['name'] == 'Gold TOWER Ticket'):
                                goldTicketCount = goldTicketCount + 1
                            elif(tokenMetadata['name'] == 'Silver TOWER Ticket'):
                                silverTicketCount = silverTicketCount + 1
                            elif(tokenMetadata['name'] == 'Bronze TOWER Ticket'):
                                bronzeTicketCount = bronzeTicketCount + 1
                            else:
                                continue
                        else:
                            continue

                    print("Wallet %s has gold: %d , silver: %d , bronze: %d" %(address, goldTicketCount, silverTicketCount, bronzeTicketCount))
                    walletRecord = [address, goldTicketCount, silverTicketCount, bronzeTicketCount]
                    writer.writerow(walletRecord)

                else:
                    continue
    
if __name__ == "__main__":
    main()