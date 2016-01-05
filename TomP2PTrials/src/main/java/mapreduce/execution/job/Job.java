package mapreduce.execution.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.utils.FileSize;
import mapreduce.utils.IDCreator;

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
	private static final int DEFAULT_MAX_NR_OF_FINISHED_WORKERS = 3;

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
	private List<Procedure> procedures;

	/**
	 * Internal counter that specifies the currently processed procedure
	 */
	private int previousProcedureIndex;

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
	private int jobSubmissionCounter;

	// private boolean isActive;

	private Job(String jobSubmitterID, PriorityLevel... priorityLevel) {
		this.jobSubmitterID = jobSubmitterID;
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.priorityLevel = (priorityLevel == null || priorityLevel.length == 0 || priorityLevel.length > 1 ? DEFAULT_PRIORITY_LEVEL
				: priorityLevel[0]);
		this.creationTime = System.currentTimeMillis();
		this.previousProcedureIndex = 0;
		this.procedures = Collections.synchronizedList(new ArrayList<>());
		// Add initial
		Procedure procedureInformation = Procedure.create(StartProcedure.create(), 0);
		this.procedures.add(procedureInformation);
	}

	public static Job create(String jobSubmitterID, PriorityLevel... priorityLevel) {
		return new Job(jobSubmitterID, priorityLevel).maxFileSize(DEFAULT_FILE_SIZE).timeToLiveInMs(DEFAULT_TIME_TO_LIVE_IN_MS)
				.maxNrOfDHTActions(DEFAULT_NUMBER_OF_ADD_TRIALS).useLocalStorageFirst(true)
				.maxNrOfFinishedWorkersPerTask(DEFAULT_MAX_NR_OF_FINISHED_WORKERS);
	}

	public String id() {
		// S == Submitter
		// SNR == Submission counter
		return this.id + "_S(" + jobSubmitterID + ")_SNR(" + jobSubmissionCounter + ")";
	}

	public Job id(String id) {
		this.id = id;
		return this;
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
	public Procedure procedure(int index) {
		if (index < 0) {
			return procedures.get(0);
		} else if (index >= procedures.size()) {
			return Procedure.create(EndProcedure.create(), procedures.size());
		} else {
			return procedures.get(index);
		}
	}

	/**
	 * Convenience method. Same as Job.procedure(Job.currentProcedureIndex())
	 * 
	 * @return
	 */
	public Procedure previousProcedure() {
		return this.procedure(this.previousProcedureIndex);
	}

	/**
	 * Convenience method. Same as Job.procedure(Job.currentProcedureIndex()+1). Retrieves the procedure information for the next procedure to be
	 * executed after the current procedure
	 * 
	 * @return
	 */
	public Procedure currentProcedure() {
		return this.procedure(this.previousProcedureIndex + 1);
	}

	/**
	 * Adds a procedure to the job. The idea is that chaining of nextProcedure() calls (e.g. Job.nextProcedure(...).nextProcedure(...) also indicates
	 * how procedures are processed (from 0 to N). This means that the first procedure added is also the first procedure that is going to be
	 * processed, until the last added procedure that was added.
	 * 
	 * @param procedure
	 * @return
	 */
	public Job addSubsequentProcedure(IExecutable procedure) {
		Procedure procedureInformation = Procedure.create(procedure, this.procedures.size());
		this.procedures.add(procedureInformation);
		return this;
	}

	public void incrementProcedureIndex() {
		++this.previousProcedureIndex;
	}

	public int submissionCounter() {
		return this.jobSubmissionCounter;
	}

	public int incrementSubmissionCounter() {
		return ++this.jobSubmissionCounter;
	}

	public void resetSubmissionCounter() {
		this.jobSubmissionCounter = 0;
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
		return id();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id() == null) ? 0 : id().hashCode());
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
		if (id() == null) {
			if (other.id() != null)
				return false;
		} else if (!id().equals(other.id()))
			return false;
		return true;
	}
 

	@Override
	public int compareTo(Job job) {
		if (priorityLevel == job.priorityLevel) {
			return creationTime.compareTo(job.creationTime);
		} else {
			return priorityLevel.compareTo(job.priorityLevel);
		}
	}

//	public static void main(String[] args) {
//		List<Job> jobs = new ArrayList<>();
//		jobs.add(Job.create("1", PriorityLevel.LOW));
//		jobs.add(Job.create("2", PriorityLevel.MODERATE));
//		jobs.add(Job.create("3", PriorityLevel.HIGH));
//		jobs.add(Job.create("4", PriorityLevel.MODERATE));
//		jobs.add(Job.create("5", PriorityLevel.LOW));
//		jobs.add(Job.create("6", PriorityLevel.HIGH));
//
//		Collections.sort(jobs);
//		for (Job job : jobs) {
//			System.err.println(job.jobSubmitterID() + ", " + job.priorityLevel);
//		}
//	}

	public PriorityLevel priorityLevel() { 
		return priorityLevel;
	}

	public Long creationTime() {
		return creationTime;
	}
	//
	// public boolean isActive() {
	// return isActive;
	// }
	//
	// public Job isActive(boolean isActive) {
	// this.isActive = isActive;
	// return this;
	// }

}
