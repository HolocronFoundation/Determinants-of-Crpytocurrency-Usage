import csv, glob, multiprocessing, sys

# Drops all inputs without relayed by

# Chunks txs into chunks of ~50k

# Preps for large field sizes
maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

def dropAndRechunk(inputDirectory="E:/Databases/BTC Info/", outputDirectory="D:/BTC - Dropped"):

    # Preps a file queue
    files = glob.glob(inputDirectory + '*.csv')
    print("Processing " + str(len(files)) + " file.")
    fileQueue = multiprocessing.Queue()
    for file in files:
        fileQueue.put(file)

    # Determines the number of threads
    threadsToUse = max(1, multiprocessing.cpu_count()-1)
    print("Using " + str(threadsToUse) + " worker threads.")

    # Preps an output counter and corresponding lock
    outputCount = multiprocessing.Value('i', 0)

    #workerThread(outputCount, fileQueue, outputDirectory)
    
    processes = []
    for i in range(threadsToUse):
        p = multiprocessing.Process(target=workerThread, args=(outputCount, fileQueue, outputDirectory))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    print("Done!")
    

def workerThread(fileCounter, fileQueue, outputDirectory):
    selectedFiles = []
    transactionsToKeep = []
    while not fileQueue.empty():
        print(str(fileQueue.qsize()) + " files remaining in queue.")
        while not fileQueue.empty() and len(selectedFiles) < 100:
            selectedFiles.append(fileQueue.get())
        print("Thread collected files, starting to process...")
        for file in selectedFiles:
            with open(file) as fileInProcess:
                reader = csv.reader(fileInProcess)
                for row in reader:
                    if len(row) > 3 and row[3] != '0.0.0.0' and row[3] != '127.0.0.1':
                        transactionsToKeep.append(row)
            if len(transactionsToKeep) > 5000:
                fileNumber = 0
                with fileCounter.get_lock():
                    fileNumber = fileCounter.value
                    fileCounter.value += 1
                print("Outputting file " + str(fileNumber))
                with open(outputDirectory + "/blanksDropped-" + str(fileNumber) + ".csv", "w") as outputFile:
                    writer = csv.writer(outputFile)
                    writer.writerows(transactionsToKeep)
                transactionsToKeep = []
        selectedFiles = []
    if transactionsToKeep != []:
        fileNumber = 0
        with fileCounter.get_lock():
            fileNumber = fileCounter.value
            fileCounter.value += 1
        with open(outputDirectory + "/blanksDropped-" + str(fileNumber) + ".csv", "w") as outputFile:
            writer = csv.writer(outputFile)
            writer.writerows(transactionsToKeep)
                
if __name__ == '__main__':
    dropAndRechunk()
        
