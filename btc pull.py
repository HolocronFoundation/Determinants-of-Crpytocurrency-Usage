import time, csv
from blockchain import blockexplorer

#decisions: chunk txs into 100 blocks

# TODO: add load api key function

def outputCSV(txList, lastBlockHeight, outputDirectory="E:/Databases/BTC Info"):
    with open(outputDirectory + "/BTC-Raw-" + str(lastBlockHeight) + ".csv", "w") as outputFile:
        writer = csv.writer(outputFile)
        writer.writerows(txList)

def processInputs(inputs):
    processedInputs = []
    for entry in inputs:
        try:
            processedInput = [entry.n,
                              entry.value,
                              entry.address,
                              entry.type,
                              entry.script,
                              entry.script_sig,
                              entry.sequence]
            processedInputs.append(processedInput)
        except AttributeError: #Catches coinbase transactions
            processedInput = ["CT",
                              'CT',
                              'CT',
                              'CT',
                              'CT',
                              entry.script_sig,
                              entry.sequence]
            processedInputs.append(processedInput)
    return processedInputs

def processOutputs(outputs):
    processedOutputs = []
    for entry in outputs:
        processedOutput = [entry.n,
                          entry.value,
                          entry.address,
                          entry.script,
                          entry.spent,
                          entry.addr_tag_link,
                          entry.addr_tag]
        processedOutputs.append(processedOutput)
    return processedOutputs

def pullBlock(block, errors=0):
    try:
        return blockexplorer.get_block_height(block, api_code=load_api_code())
    except Exception as e:
        print("Exception pulling... sleeping " + str(1+errors) + "seconds. Details below:")
        print(e)
        time.sleep(1+errors)
        return pullBlock(block, errors+1)

transactions = []

sleep = .35

lastPullTime = time.time()

for i in range(502900, 569000):
    blocks = pullBlock(i)
    lastPullTime = time.time()
    print(i)

    for block in blocks:
        for transaction in block.transactions:
            currentTransaction = [transaction.double_spend,
                                  transaction.block_height,
                                  transaction.time,
                                  transaction.relayed_by,
                                  transaction.hash,
                                  transaction.version,
                                  transaction.size,
                                  processInputs(transaction.inputs),
                                  processOutputs(transaction.outputs)]
            transactions.append(currentTransaction)

    if (i+1)%100 == 0:
        outputCSV(transactions, i)
        transactions = []

    if time.time() - lastPullTime < sleep:
        time.sleep(sleep - (time.time() - lastPullTime))
