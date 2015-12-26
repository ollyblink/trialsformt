package mapreduce.execution.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.computation.standardprocedures.EndReached;
import mapreduce.execution.task.Task;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.IDCreator;
import mapreduce.utils.SyncedCollectionProvider;

public class Job implements Serializable, Comparable<Job> {

	// private static Logger logger = LoggerFactory.getLogger(Job.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1152022246679324578L;

	private static final int DEFAULT_NUMBER_OF_ADD_TRIALS = 3; // 3 times
	private static final long DEFAULT_TIME_TO_LIVE_IN_MS = 10000; // 10secs
	private static final FileSize DEFAULT_FILE_SIZE = FileSize.THIRTY_TWO_KILO_BYTES;
	private static final PriorityLevel DEFAULT_PRIORITY_LEVEL = PriorityLevel.MODERATE;

	/** specifies a unique id for this job */
	private String id;

	/** identifier for the submitting entity (@see{MRJobSubmissionManager}) */
	private final String jobSubmitterID;

	/** Used for order of jobs @see{<code>PriorityLevel</code> */
	private final PriorityLevel priorityLevel;

	/** Used for order of jobs. System.currentTimeInMillis(): long */
	private final Long creationTime;
	/**
	 * Contains all procedures for this job. Processing is done from 0 to procedures.size()-1, meaning that the first procedure added using
	 * Job.nextProcedure(procedure) is also the first one to be processed.
	 */
	private List<ProcedureInformation> procedures;

	/**
	 * Internal counter that specifies the currently processed procedure
	 */
	private int currentProcedureIndex;

	/** maximal file size to be put on the DHT at once */
	private FileSize maxFileSize = DEFAULT_FILE_SIZE;

	/**
	 * Where the data files for the first procedure is stored
	 */
	private String fileInputFolderPath;

	/**
	 * Specifies the number of workers that maximally should execute a task of a job (a job's data is divided into a number of tasks). If 3 or more
	 * workers should finish a task, a majority vote on the task result determines if the result is likely to be correct (if 2 out of 3 workers
	 * achieved the same hash, the task is finished). The principle is the same as in tennis: best of 3 or best of 5. Always,
	 * Math.Round(maxNrOfFinishedWorkersPerTask/2) workers need to achieve the same hash.
	 */
	private int maxNrOfFinishedWorkersPerTask;

	/**
	 * if true, the peer tries to pull tasks from own storage before accessing the dht. If false, locality of data is ignored and instead the dht is
	 * directly accessed in all cases (possibly slower)
	 */
	private boolean useLocalStorageFirst;

	/** How many times should the dht operation be tried before it is declared as failed? */
	private int maxNrOfDHTActions;

	/** For how long should be waited until it declares the dht operation to be failed? In milliseconds */
	private long timeToLiveInMs;

	/** Number of times this job was already submitted. used together with maxNrOfDHTActions can determine if job submission should be cancelled */
	private int submissionCounter;

	private Job(String jobSubmitterID, PriorityLevel... priorityLevel) {
		this.jobSubmitterID = jobSubmitterID;
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.priorityLevel = (priorityLevel == null || priorityLevel.length == 0 || priorityLevel.length > 1 ? DEFAULT_PRIORITY_LEVEL
				: priorityLevel[0]);
		this.creationTime = System.currentTimeMillis();
		this.currentProcedureIndex = 0;
		this.procedures = Collections.synchronizedList(new ArrayList<>());
	}

	public static Job create(String jobSubmitterID, PriorityLevel... priorityLevel) {
		return new Job(jobSubmitterID, priorityLevel).maxFileSize(DEFAULT_FILE_SIZE).timeToLiveInMs(DEFAULT_TIME_TO_LIVE_IN_MS)
				.maxNrOfDHTActions(DEFAULT_NUMBER_OF_ADD_TRIALS).useLocalStorageFirst(true).maxNrOfFinishedWorkersPerTask(3);
	}

	public String id() {
		return this.id;
	}

	public String jobSubmitterID() {
		return this.jobSubmitterID;
	}

	/**
	 * Returns the procedure at the specified index. As procedures are added using Job.nextProcedure(procedure), the index in the list they are stored
	 * in also specifies the order in which procedures are processed. As such, the index starts from 0 (first procedure) and ends with list.size()-1
	 * (last procedure to be processed). Use this method together with Job.currentProcedureIndex() to access the currently processed procedure. E.g.
	 * Job.procedure(Job.currentProcedureIndex())
	 * 
	 * @param index
	 * @return the procedure at the specified index
	 */
	public ProcedureInformation procedure(int index) {
		try {
			return procedures.get(index);
		} catch (Exception e) {
			return ProcedureInformation.create(EndReached.create());
		}
	}

	/**
	 * Convenience method. Same as Job.procedure(Job.currentProcedureIndex())
	 * 
	 * @return
	 */
	public ProcedureInformation currentProcedure() {
		return this.procedure(this.currentProcedureIndex);
	}

	/**
	 * Convenience method. Same as Job.procedure(Job.currentProcedureIndex()+1). Retrieves the procedure information for the next procedure to be
	 * executed after the current procedure
	 * 
	 * @return
	 */
	public ProcedureInformation subsequentProcedure() {
		return this.procedure(this.currentProcedureIndex + 1);
	}

	/**
	 * Adds a procedure to the job. The idea is that chaining of nextProcedure() calls (e.g. Job.nextProcedure(...).nextProcedure(...) also indicates
	 * how procedures are processed (from 0 to N). This means that the first procedure added is also the first procedure that is going to be
	 * processed, until the last added procedure that was added.
	 * 
	 * @param procedure
	 * @return
	 */
	public Job addSubsequentProcedure(IMapReduceProcedure procedure) {
		this.procedures.add(ProcedureInformation.create(procedure));
		return this;
	}

	/**
	 * convenience method, same as currentProcedureIndex()+1
	 * 
	 * @return the index of the procedure following the currently executed procedure
	 */
	public int subsequentProcedureIndex() {
		return this.currentProcedureIndex + 1;
	}

	public int currentProcedureIndex() {
		return currentProcedureIndex;
	}

	public void incrementCurrentProcedureIndex() {
		++this.currentProcedureIndex;
	}

	public int submissionCounter() {
		return this.submissionCounter;
	}

	public int incrementSubmissionCounter() {
		return ++this.submissionCounter;
	}

	public void resetSubmissionCounter() {
		this.submissionCounter = 0;
	}

	public int maxNrOfFinishedWorkersPerTask() {
		return maxNrOfFinishedWorkersPerTask;
	}

	public Job maxNrOfFinishedWorkersPerTask(int maxNrOfFinishedWorkersPerTask) {
		this.maxNrOfFinishedWorkersPerTask = maxNrOfFinishedWorkersPerTask;
		return this;
	}

	public Job fileInputFolderPath(String fileInputFolderPath) {
		this.fileInputFolderPath = fileInputFolderPath;
		return this;
	}

	public String fileInputFolderPath() {
		return fileInputFolderPath;
	}

	public Job maxFileSize(FileSize maxFileSize) {
		this.maxFileSize = maxFileSize;
		return this;
	}

	public FileSize maxFileSize() {
		return this.maxFileSize;
	}

	public boolean useLocalStorageFirst() {
		return this.useLocalStorageFirst;
	}

	public Job useLocalStorageFirst(boolean useLocalStorageFirst) {
		this.useLocalStorageFirst = useLocalStorageFirst;
		return this;
	}

	public Job timeToLiveInMs(long timeToLiveInMs) {
		this.timeToLiveInMs = timeToLiveInMs;
		return this;
	}

	public long timeToLiveInMs() {
		return this.timeToLiveInMs;
	}

	public Job maxNrOfDHTActions(int maxNrOfDHTActions) {
		this.maxNrOfDHTActions = maxNrOfDHTActions;
		return this;
	}

	public int maxNrOfDHTActions() {
		return this.maxNrOfDHTActions;
	}

	@Override
	public String toString() {
		return id + "_SUBMISSIONNR_" + submissionCounter + "_SUBMITTER_" + jobSubmitterID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((jobSubmitterID == null) ? 0 : jobSubmitterID.hashCode());
		result = prime * result + submissionCounter;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Job other = (Job) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (jobSubmitterID == null) {
			if (other.jobSubmitterID != null)
				return false;
		} else if (!jobSubmitterID.equals(other.jobSubmitterID))
			return false;
		if (submissionCounter != other.submissionCounter)
			return false;
		return true;
	}

	public Job copy() {
		Job job = new Job(jobSubmitterID, priorityLevel);
		job.id = id;
		job.currentProcedureIndex = currentProcedureIndex;
		job.fileInputFolderPath = fileInputFolderPath;
		job.maxFileSize = maxFileSize;
		job.maxNrOfDHTActions = maxNrOfDHTActions;
		job.maxNrOfFinishedWorkersPerTask = maxNrOfFinishedWorkersPerTask;
		job.procedures = SyncedCollectionProvider.syncedArrayList();
		for (ProcedureInformation pI : procedures) {
			ProcedureInformation copyPI = ProcedureInformation.create(pI.procedure()).isFinished(pI.isFinished());
			List<Task> tasks = copyPI.tasks();
			for (Task task : pI.tasks()) {
				Task taskCopy = Task.newInstance(task.id(), task.jobId()); // NO DEEP TASK COPY
				tasks.add(taskCopy);
			}
			job.procedures.add(pI);
		}
		job.submissionCounter = submissionCounter;
		job.timeToLiveInMs = timeToLiveInMs;
		job.useLocalStorageFirst = useLocalStorageFirst;
		return job;
	}

	@Override
	public int compareTo(Job job) {
		if (priorityLevel == job.priorityLevel) {
			return creationTime.compareTo(job.creationTime);
		} else {
			return priorityLevel.compareTo(job.priorityLevel);
		}
	}

	public static void main(String[] args) {
		List<Job> jobs = new ArrayList<>();
		jobs.add(Job.create("1", PriorityLevel.LOW));
		jobs.add(Job.create("2", PriorityLevel.MODERATE));
		jobs.add(Job.create("3", PriorityLevel.HIGH));
		jobs.add(Job.create("4", PriorityLevel.MODERATE));
		jobs.add(Job.create("5", PriorityLevel.LOW));
		jobs.add(Job.create("6", PriorityLevel.HIGH));

		Collections.sort(jobs);
		for (Job job : jobs) {
			System.err.println(job.jobSubmitterID() + ", " + job.priorityLevel);
		}
	}

	public String subsequentJobProcedureDomain() {
		String subsequentProcedureName = subsequentProcedure().procedure().getClass().getSimpleName();
		return DomainProvider.INSTANCE.jobProcedureDomain(id, subsequentProcedureName, subsequentProcedureIndex(), submissionCounter);
	}
}
