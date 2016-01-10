package mapreduce.execution.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ch.qos.logback.classic.Logger;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.utils.FileSize;
import mapreduce.utils.IDCreator;
import mapreduce.utils.SyncedCollectionProvider;

public class Job implements Serializable, Comparable<Job>, Cloneable {

	// private static Logger logger = LoggerFactory.getLogger(Job.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1152022246679324578L;

	// private static final int DEFAULT_NUMBER_OF_ADD_TRIALS = 3; // 3 times
	// private static final long DEFAULT_TIME_TO_LIVE_IN_MS = 10000; // 10secs
	private static final FileSize DEFAULT_FILE_SIZE = FileSize.THIRTY_TWO_KILO_BYTES;
	private static final PriorityLevel DEFAULT_PRIORITY_LEVEL = PriorityLevel.MODERATE;
	// private static final int DEFAULT_MAX_NR_OF_FINISHED_WORKERS = 1;

	/** specifies a unique id for this job */
	private String id;

	/** identifier for the submitting entity (@see{MRJobSubmissionManager}) */
	private String jobSubmitterID;

	/** Used for order of jobs @see{<code>PriorityLevel</code> */
	private PriorityLevel priorityLevel;

	/** Used for order of jobs. System.currentTimeInMillis(): long */
	private Long creationTime;
	/**
	 * Contains all procedures for this job. Processing is done from 0 to procedures.size()-1, meaning that the first procedure added using
	 * Job.nextProcedure(procedure) is also the first one to be processed.
	 */
	private List<Procedure> procedures;

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

	// /**
	// * Specifies the number of workers that maximally should execute a task of a job (a job's data is divided into a number of tasks). If 3 or more
	// * workers should finish a task, a majority vote on the task result determines if the result is likely to be correct (if 2 out of 3 workers
	// * achieved the same hash, the task is finished). The principle is the same as in tennis: best of 3 or best of 5. Always,
	// * Math.Round(maxNrOfFinishedWorkersPerTask/2) workers need to achieve the same hash.
	// */
	// private int nrOfSameResultHash = DEFAULT_MAX_NR_OF_FINISHED_WORKERS;

	/**
	 * if true, the peer tries to pull tasks from own storage before accessing the dht. If false, locality of data is ignored and instead the dht is
	 * directly accessed in all cases (possibly slower)
	 */
	private boolean useLocalStorageFirst;

	private boolean isFinished;

	/** Number of times this job was already submitted. used together with maxNrOfDHTActions can determine if job submission should be cancelled */
	// private int jobSubmissionCounter;

	// private boolean isActive;

	private Job(String jobSubmitterID, PriorityLevel... priorityLevel) {
		this.jobSubmitterID = jobSubmitterID;
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.priorityLevel = (priorityLevel == null || priorityLevel.length == 0 || priorityLevel.length > 1 ? DEFAULT_PRIORITY_LEVEL
				: priorityLevel[0]);
		this.creationTime = System.currentTimeMillis();
		this.currentProcedureIndex = 0;
		this.procedures = SyncedCollectionProvider.syncedArrayList();
		// Add initial
		Procedure startProcedure = Procedure.create(StartProcedure.create(), 0).nrOfSameResultHash(1);
		this.procedures.add(startProcedure);
	}

	public static Job create(String jobSubmitterID, PriorityLevel priorityLevel, boolean useLocalStorageFirst) {
		return new Job(jobSubmitterID, priorityLevel).maxFileSize(DEFAULT_FILE_SIZE).useLocalStorageFirst(useLocalStorageFirst);
	}

	public static Job create(String jobSubmitterID, PriorityLevel priorityLevel) {
		return new Job(jobSubmitterID, priorityLevel).maxFileSize(DEFAULT_FILE_SIZE).useLocalStorageFirst(true);
	}

	public static Job create(String jobSubmitterID) {
		return new Job(jobSubmitterID, PriorityLevel.MODERATE).maxFileSize(DEFAULT_FILE_SIZE).useLocalStorageFirst(true);
	}

	public String id() {
		// S == Submitter
		// SNR == Submission counter
		return this.id + "_S(" + jobSubmitterID + ")";// _SNR(" + jobSubmissionCounter + ")";
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
			return Procedure.create(EndProcedure.create(), procedures.size()).nrOfSameResultHash(0);
		} else {
			return procedures.get(index);
		}
	}

	/**
	 * Convenience method. Same as Job.procedure(Job.currentProcedureIndex()+1). Retrieves the procedure information for the next procedure to be
	 * executed after the current procedure
	 * 
	 * @return
	 */
	public Procedure currentProcedure() {
		return this.procedure(this.currentProcedureIndex);
	}

	/**
	 * Adds a procedure to the job. The idea is that chaining of nextProcedure() calls (e.g. Job.nextProcedure(...).nextProcedure(...) also indicates
	 * how procedures are processed (from 0 to N). This means that the first procedure added is also the first procedure that is going to be
	 * processed, until the last added procedure that was added.
	 * 
	 * @param procedure
	 *            the actual procedure to execute next
	 * @param combiner
	 *            combines data for this procedure before sending it to the dht. If combiner is null, no combination is done before sending the data
	 *            to the dht. Often, this is the same as the subsequent procedure following this procedure, only applied locally
	 * @param nrOfSameResultHashForProcedure
	 *            specifies how many times this procedure should achieve the same result hash before one is confident enough to consider the procedure
	 *            to be finished
	 * @param numberOfSameResultHashForTasks
	 *            specifies how many times the tasks of this procedure should achieve the same result hash before one is confident enough to consider
	 *            a task to be finished
	 * @return
	 */
	public Job addSucceedingProcedure(IExecutable procedure, IExecutable combiner, int nrOfSameResultHashForProcedure,
			int nrOfSameResultHashForTasks) {
		if (procedure == null) {
			return this;
		}
		nrOfSameResultHashForProcedure = (nrOfSameResultHashForProcedure == 0 ? 1 : nrOfSameResultHashForProcedure);
		nrOfSameResultHashForTasks = (nrOfSameResultHashForTasks == 0 ? 1 : nrOfSameResultHashForTasks);

		Procedure procedureInformation = Procedure.create(procedure, this.procedures.size()).nrOfSameResultHash(nrOfSameResultHashForProcedure)
				.nrOfSameResultHashForTasks(nrOfSameResultHashForTasks).combiner(combiner);
		this.procedures.add(procedureInformation);
		return this;
	}

	public void incrementProcedureIndex() {
		if (this.currentProcedureIndex <= procedures.size()) {
			++this.currentProcedureIndex;
		}
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
		if (!isFinished && job.isFinished) {
			return -1;
		} else if (isFinished && !job.isFinished) {
			return 1;
		} else {// if (!isFinished && !job.isFinished) {
			if (priorityLevel == job.priorityLevel) {
				return creationTime.compareTo(job.creationTime);
			} else {
				return priorityLevel.compareTo(job.priorityLevel);
			}
		}
		// else {
		// return 0;
		// }
	}

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

	@Override
	public Job clone() {

		try {
			Job job = (Job) super.clone();

			// job.creationTime = creationTime;
			// job.currentProcedureIndex = currentProcedureIndex;
			// job.fileInputFolderPath = fileInputFolderPath;
			// job.id = id;
			// job.jobSubmitterID = jobSubmitterID;
			// job.maxFileSize = maxFileSize;
			// job.nrOfSameResultHash = nrOfSameResultHash;
			// job.priorityLevel = priorityLevel;
			// job.useLocalStorageFirst = useLocalStorageFirst;
			job.procedures = SyncedCollectionProvider.syncedArrayList();
			for (Procedure p : procedures) {
				job.procedures.add(p.clone());
			}
			return job;
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		List<Job> jobs = new ArrayList<>();
		jobs.add(Job.create("1", PriorityLevel.LOW).isFinished(true));
		jobs.add(Job.create("2", PriorityLevel.MODERATE).isFinished(true));
		jobs.add(Job.create("3", PriorityLevel.HIGH).isFinished(true));
		jobs.add(Job.create("4", PriorityLevel.MODERATE));
		jobs.add(Job.create("5", PriorityLevel.LOW));
		jobs.add(Job.create("6", PriorityLevel.HIGH));

		Collections.sort(jobs);
		for (Job job : jobs) {
			System.err.println(job.jobSubmitterID() + ", " + job.priorityLevel);
		}
	}
	// public static void main(String[] args) {
	// Job job = Job.create("ME", PriorityLevel.HIGH).addSucceedingProcedure(WordCountReducer.create()).fileInputFolderPath("File input path")
	// .maxFileSize(FileSize.EIGHT_BYTES).nrOfSameResultHash(20);
	// Job job2 = job.clone();
	// System.out.println(job.currentProcedureIndex + ", " + job.fileInputFolderPath + ", " + job.id + ", " + job.jobSubmitterID + ", "
	// + job.maxFileSize + ", " + job.nrOfSameResultHash + ", " + job.creationTime + ", " + job.serialVersionUID + ", "
	// + job.useLocalStorageFirst + ", " + job.procedures);
	// System.out.println(job2.currentProcedureIndex + ", " + job2.fileInputFolderPath + ", " + job2.id + ", " + job2.jobSubmitterID + ", "
	// + job2.maxFileSize + ", " + job2.nrOfSameResultHash + ", " + job2.creationTime + ", " + job2.serialVersionUID + ", "
	// + job2.useLocalStorageFirst + ", " + job2.procedures);
	// }

	public Job isFinished(boolean isFinished) {
		this.isFinished = isFinished;
		return this;
	}

	public boolean isFinished() {
		return isFinished;
	}

}
