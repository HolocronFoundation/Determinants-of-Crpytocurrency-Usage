from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

CSVFiles = "D:\BTC CSV"

rpc_connection = AuthServiceProxy("http://%s:%s@127.0.0.1:8332"%("username", "password"))
blockCount = rpc_connection.getblockcount()
for i in range(100000, 100100):#blockCount):
    currentHash = rpc_connection.getblockhash(i)
    currentBlock = rpc_connection.getblock(currentHash)
    currentTime = currentBlock['time']
    for transaction in currentBlock['tx']:
        txInfo = rpc_connection.getrawtransaction(transaction)
        txDecoded = rpc_connection.decoderawtransaction(txInfo)
        processed = [txDecoded['txid'], currentTime, txDecoded['weight']]
        for output in txDecoded['vout']:
            processed.append(str(output['value']))
        print("Processed: " + str(processed))
    print("Done with block " + str(i))
