from java.util.concurrent import Callable

class ThreadWrapper(Callable):
    def __init__(self, function, *args, **kwargs):
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._thread = None
        self._startTime = None
        self.exception = None
        self.result = None
        self.computeTime = None
        if "jobName" in kwargs.keys():
            jobName = kwargs["jobName"]
        else: 
            jobName = "No Name"
        self.jobName = jobName

    def call(self):
        self._startTime = time.time()
        self._thread = threading.currentThread().getName()
        try:
            self.result = self._function(*self._args, **self._kwargs)
        except Exception, e:
            self.exception = e
            ex_type, ex, tb = sys.exc_info()
            self.traceback = tb
            #print "thread for %s failed:\n%s" % (self.jobName, self.traceback.format_exc()) ##raise e

        self.computeTime = time.time() - self._startTime
        return self

    def __del__(self):
        del self.result

        

from java.util.concurrent import Executors, ExecutorCompletionService
import java.lang.Runtime as JavaRunTime
rt = JavaRunTime.getRuntime()
# over 2 because most intel machines report double the number of real cores, 
# returned by this function call, -1 because we want to leave something for WAT/ResSim/the OS.
MAX_THREADS = max(1, rt.availableProcessors()/2 - 1) 

...

pool = Executors.newFixedThreadPool(numThreads)
ecs = ExecutorCompletionService(pool)
jobs = list()

def runKnnAndStorageAreas(tableLabel, modelFPart, ...):
    # function to do compute
	# call kNN compute functions and post-processors for interpolations
    ...
    return listOfTimeSeriesContainers

for task in listOfTasksCsvFile:
    ...
    # get the parameters from task
    ...
    jobs.append(ThreadWrapper(runKnnAndStorageAreas, tableLabel,
        modelFPart, ..., jobName=tableLabel))

for job in jobs[tableType]:
    ecs.submit(job)
# queue output to single file
submitted = len(jobs[tableType])
threadErrors = []
while submitted > 0:
    jobResults = ecs.take().get()
    if not jobResults.exception is None:
        msg = "Compute for %s failed: %s\n" % (jobResults.jobName, jobResults.exception.__repr__())
        msg += "%s\n" % "".join(traceback.format_tb(jobResults.traceback))
        threadErrors.append(msg)
        printError(msg)
        # raise jobResults.exception 
    else:
        threadTime = jobResults.computeTime
        minutes = math.floor(threadTime / 60.0)
        seconds = threadTime % 60.0
        printMessage("Thread for %s finished. \n\t Total time to compute: %i minutes, %2.1f seconds." % (jobResults.jobName, minutes, seconds))
        # set next file to be a DSS 6 output - used to write predictor TSCs to out file 
        Heclib.zset('DssVersion', '', 6)
        outFileName = outFileNameTemplate.replace("%REACH%", jobResults.jobName)
        outTSCs = jobResults.result
        errors = writeTSCs(outTSCs, outFileName)
        for e in errors: 
            printError("%s - %s" % (jobResults.jobName, e))
    submitted -= 1
    del jobResults


    