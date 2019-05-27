
# Appending Data Files
### This script appends files without otherwise processing them.
### This should work well for .csv and .txt files with an identical
### delimiter across all of the files.

### This script was written in Python 3.6.8
### Contributions by:
###     Samuel Troper (samuel.troper@bateswhite.com)

# IMPORTS
import glob, time, multiprocessing, os

#   glob is used for selecting files with unix-style matching.

#   time is used to keep track of time.

#   multiprocessing is used to create multiple python instances,
#   which allows for Python to take advantage of multiple cores.
#   It is distinct from threading due to some low level details
#   of Python which were created before multi-core systems were
#   first created. (Python was initially released in 1991)

#   os is used for interactions with the operating system
#   (e.g. deleting a temp file)

from tqdm import tqdm
#   tqdm is used to create a progress bar with almost no modifications
#   to any code, at a very, very low overhead (~60ns per iteration)
#   tqdm is an external library; it can be installed by typing
#   "pip install tqdm" into your preferred command line utility

def appendFiles(inputDirectory, outputDirectory, inputFileType="", outputFileName="appended_files.txt"):
    """A basic file appending script.

    Parameters
    ----------
    inputDirectory : str
        A string representing the directory containing input files
    outputDirectory : str
        A string representing the directory to output the appended files to
    inputFileType : str, optional
        A string representing the file type of the inputs (default is a blank string, which selects all file types)
    outputFileName : str, optional
        A string representing the name for the output file
    """
    # creates a list of all files of type inputFileType located in inputDirectory
    files = glob.glob(inputDirectory+'*'+inputFileType)
    # Using the with statement ensures the file is closed automatically
    # after exiting the indentation, making it a best practice for file
    # manipulation. The first parameter specifies the file location, and
    # the second represents the mode to open the file in, which is
    # 'w'rite mode here.
    with open(outputDirectory + outputFileName, 'w') as outputFile:
        # a standard for each loop
        # wrapping an iterable in tqdm creates a progress bar
        for file in tqdm(files):
            # Opens file in 'r'ead mode (the default for open)
            with open(file) as fileToAppend:
                # writes the content of fileToAppend at the end of
                # outputFile.
                outputFile.write(fileToAppend.read())

def appendFilesThreaded(inputDirectory, outputDirectory, inputFileType="", outputFileName="appended_files.txt"):
    """An advanced file appending script which implements threading.

    Threading results in an ~4x performance increase on an 8-core system.

    Parameters
    ----------
    inputDirectory : str
        A string representing the directory containing input files
    outputDirectory : str
        A string representing the directory to output the appended files to
    inputFileType : str, optional
        A string representing the file type of the inputs (default is a blank string, which selects all file types)
    outputFileName : str, optional
        A string representing the name for the output file
    """
    # creates a list of all files of type inputFileType located in inputDirectory
    files = glob.glob(inputDirectory+'*'+inputFileType)
    # sets the number of threads to use.
    #   The number of CPUs minus one is generally a good number to use because
    #   (i) each additional thread that can use an unused CPU core will essentially
    #   speed up your program by (oldNumberOfThreads+1)/oldNumberOfThreads
    #   e.g. doubling performance with the first additional thread
    #   (ii) there is no benefit to an additional thread if all CPU cores are actively in usE
    #   (iii) each thread has some overhead, so minimizing threads is beneficial.
    #   CPU count minus one is good because there is generally one thread always
    #   running on your computer - it is running your OS, mouse clicks, typing, etc.
    #   and therefore CPU count minus one will give you the number of cores that
    #   *may* not be in use.
    threadsToUse = max(1, multiprocessing.cpu_count()-1)
    print("Using " + str(threadsToUse) + " worker threads.")
    # creates a blank list to keep track of all processes
    processes = []
    # loops threadsToUse times to initialize that many threads
    for i in range(threadsToUse):
        # creates a process which will execute the function appendFilesWorker with the arguments specified in args
        p = multiprocessing.Process(target=appendFilesWorker, args=(files[int(i*len(files)/threadsToUse):int((i+1)*len(files)/threadsToUse)],outputDirectory+"temp-" + str(i) + outputFileName))
        # adds p to the processes list
        processes.append(p)
        # starts p
        p.start()
    # for each loop to loop through all processes
    for process in processes:
        # pauses master thread execution until process has completed
        process.join()
    # opens the output file in 'w'rite mode
    with open(outputDirectory + outputFileName, 'w') as outputFile:
        # loops through the number of threads
        for i in range(threadsToUse):
            # opens a threads individual output file
            with open(outputDirectory+"temp-" + str(i) + outputFileName) as fileToAppend:
                # writes the threads output to the final output
                outputFile.write(fileToAppend.read())
            # deletes the temp file
            os.remove(outputDirectory+"temp-" + str(i) + outputFileName)
    print('Done')
def appendFilesWorker(files, outputFileNamePath):
    with open(outputFileNamePath, 'w') as outputFile:
        for file in tqdm(files):
            with open(file) as fileToAppend:
                outputFile.write(fileToAppend.read())

####### The below methods are for an attempt at threading which was worse-performing than the single-threaded instantiation

def appendFilesThreaded_x(inputDirectory, outputDirectory, inputFileType="", outputFileName="appended_files.txt"):
    files = glob.glob(inputDirectory+'*'+inputFileType)
    fileQueue = multiprocessing.Queue()
    for file in files:
        fileQueue.put(file)
    threadsToUse = max(1, multiprocessing.cpu_count()-1)
    print("Using " + str(threadsToUse) + " worker threads.")
    for i in range(threadsToUse):
        process = multiprocessing.Process(target=appendFilesWorker_x, args=(fileQueue,outputDirectory+"temp-" + str(i) + outputFileName))
        process.start()
        process.join()
    with open(outputDirectory + outputFileName, 'w') as outputFile:
        for i in range(threadsToUse):
            with open(outputDirectory+"temp-" + str(i) + outputFileName) as fileToAppend:
                outputFile.write(fileToAppend.read())
            os.remove(outputDirectory+"temp-" + str(i) + outputFileName)
    print('Done')
def appendFilesWorker_x(fileQueue, outputFileNamePath):
    with open(outputFileNamePath, 'w') as outputFile:
        while not fileQueue.empty():
            if fileQueue.qsize()%10 is 0:
                print(str(fileQueue.qsize()) + " files remaining in queue.")
            with open(fileQueue.get()) as fileToAppend:
                outputFile.write(fileToAppend.read())

####### The below methods are for an attempt at threading which was worse-performing than the single-threaded instantiation
def initializeChildProcess(lock_, outputFilePathName_):
    global lock, outputFilePathName
    lock = lock_
    outputFilePathName = outputFilePathName_
def appendFilesThreaded_bad(inputDirectory, outputDirectory, inputFileType="", outputFileName="appended_files.txt"):
    lock = multiprocessing.Lock()
    threadsToUse = max(1, multiprocessing.cpu_count()-1)
    threadsToUse = 128
    print("Using " + str(threadsToUse) + " threads.")
    processPool = multiprocessing.Pool(threadsToUse, initializer=initializeChildProcess, initargs=(lock,outputDirectory+outputFileName))
    files = glob.glob(inputDirectory+'*'+inputFileType)
    for _ in tqdm(processPool.imap_unordered(appendIndividualFile, files), total=len(files)):
        pass
    processPool.close()
    processPool.join()
    print('Done')
    
def appendIndividualFile(file):
    appendData = None
    with open(file) as fileToAppend:
        appendData = fileToAppend.read()
    with lock, open(outputFilePathName, 'a') as outputFile:
        outputFile.write(appendData)
#######

def test(inputDirectory="//bateswhite.com/dc-fs/Admin/Data science/Knowledge base/Data for exercises/Fannie Mae/Loan-level files/",
         outputDirectory="//bateswhite.com/dc-fs/Admin/Data science/Knowledge base/Contents/Appending data/Output/",
         outputFileName="appended_files.txt",
         threaded=True):
    startTime = time.time()
    appendFilesThreaded(inputDirectory, outputDirectory, inputFileType=".txt", outputFileName=outputFileName)
    print("Completed. Total threaded execution time: " + str(time.time() - startTime))
    startTime = time.time()
    appendFiles(inputDirectory, outputDirectory, inputFileType=".txt", outputFileName=outputFileName)
    print("Completed. Total basic execution time: " + str(time.time() - startTime))

if __name__ == '__main__':
    test()
