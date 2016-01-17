package mapreduce.execution.procedures;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.finishables.AbstractFinishable;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

/**
 * 
 * @author Oliver
 *
 */
public   class Procedure extends AbstractFinishable implements Serializable, Cloneable {
	private static Logger logger = LoggerFactory.getLogger(Procedure.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1717123684693430690L;
	/** The actual procedure to execute */
	private Object executable;
	/** Which procedure in the job's procedure list (@link{Job} it is (counted from 0 == StartProcedure to N-1 == EndProcedure) */
	private final int procedureIndex;
	/** Tasks this procedure needs to execute */
	private List<Task> tasks;
	/** Location of keys to create the tasks for this procedure */
	private JobProcedureDomain dataInputDomain;
	/**
	 * Used to combine data before it is sent to the dht. "Local" aggregation. Is often the same as the subsequent procedure (e.g. WordCount: Combiner
	 * of WordCountMapper would be WordCountReducer as it locally reduces the words). It is not guaranteed that this always works!
	 */
	private Object combiner;
	/** How many times should each task be executed and reach the same result hash until it is assumed to be a correct answer? */
	private int nrOfSameResultHashForTasks = 1;
	/** Just to make sure this indeed is the same procedure of the same job (may be another job with the same procedure) */
	private String jobId;
	private boolean needsMultipleDifferentDomainsForTasks;

	private Procedure(Object executable, int procedureIndex) {
		this.executable = executable;
		this.procedureIndex = procedureIndex;
		this.tasks = SyncedCollectionProvider.syncedArrayList();
		this.dataInputDomain = null;
	}

	public static Procedure create(Object executable, int procedureIndex) {
		return new Procedure(executable, procedureIndex);
	}

	public Procedure executable(IExecutable executable) {
		this.executable = executable;
		return this;
	}

	@Override
	// Calculates the result hash as an XOR of all the task's result hash's with Number160.ZERO as a base hash. Returns null if not all tasks have
	// finished yet
	public Number160 calculateResultHash() {
		Number160 resultHash = Number160.ZERO;
		if (tasks.size() < dataInputDomain.expectedNrOfFiles()) {
			return null; // not all possible tasks have been assigned yet...
		} else {
			synchronized (tasks) {
				for (Task task : tasks) {
					if (task.isFinished()) {
						Number160 taskResultHash = task.calculateResultHash();
						resultHash = resultHash.xor(taskResultHash);
					} else {
						return null; // All tasks have to be finished before this can be called
					}
				}
			}
			return resultHash;
		}
	}

	/**
	 * How many tasks of this procedure have finished (be aware: simply having all tasks finished does not mean that all tasks were already received)
	 * --> Does not imply the procedure is completed yet! This method is exclusively used to inform other executors about the finishing state of this
	 * executor. If it should be the case that two executors execute the same procedure on different input data sets, nrOfFinishedTasks will determine
	 * which executor to cancel and which to keep (the idea is to execute only on the same data set to keep results consistent, even if the data may
	 * have been corrupted as there is no way to determine that beforehand.
	 * 
	 * 
	 * @return
	 */
	public int nrOfFinishedTasks() {
		int finishedTasksCounter = 0;
		synchronized (tasks) {
			for (Task task : tasks) {
				if (task.isFinished()) {
					++finishedTasksCounter;
				}
			}
		}
		return finishedTasksCounter;
	}

	/** Reset the result domains of the tasks, such that this procedure may be executed once more */
	public void reset() {
		synchronized (tasks) {
			for (Task task : tasks) {
				task.reset();
			}
		}
	}

	// SETTER/GETTER
	@Override
	// Convenience for Fluent
	public Procedure nrOfSameResultHash(int nrOfSameResultHash) {
		return (Procedure) super.nrOfSameResultHash(nrOfSameResultHash);
	}

	/**
	 * Used to assign to tasks while creating them
	 * 
	 * @param nrOfSameResultHashForTasks
	 *            see description above
	 * @return
	 */
	public Procedure nrOfSameResultHashForTasks(int nrOfSameResultHashForTasks) {
		this.nrOfSameResultHashForTasks = nrOfSameResultHashForTasks;
		synchronized (tasks) { // If it's set on the go, should update all tasks (hopefully never happens...)
			for (Task task : tasks) {
				task.nrOfSameResultHash(nrOfSameResultHashForTasks);
			}
		}
		return this;
	}

	public int procedureIndex() {
		return this.procedureIndex;
	}

	public Procedure addTask(Task task) {
		logger.info("add task "+task);
		synchronized (tasks) {
			if (!this.tasks.contains(task)) {
				task.nrOfSameResultHash(nrOfSameResultHashForTasks);
				task.needsMultipleDifferentDomains(needsMultipleDifferentDomainsForTasks);
				this.tasks.add(task);
			}
		}
		return this;
	}

	public List<Task> tasks() {
		return this.tasks;
	}

	public Object combiner() {
		return this.combiner;
	}

	public Procedure combiner(Object combiner) {
		this.combiner = combiner;
		return this;
	}

	public Procedure dataInputDomain(JobProcedureDomain dataInputDomain) {
		this.dataInputDomain = dataInputDomain;
		this.jobId = dataInputDomain.jobId();
		return this;
	}

	public JobProcedureDomain dataInputDomain() {
		return this.dataInputDomain;
	}

	/**
	 * Set via dataInputDomain
	 * 
	 * @return
	 */
	public String jobId() {
		return jobId;
	}

	public Object executable() {
		return executable;
	}

	@Override
	public Procedure needsMultipleDifferentDomains(boolean needsMultipleDifferentDomains) {
		return (Procedure) super.needsMultipleDifferentDomains(needsMultipleDifferentDomains);
	}

	public Procedure needsMultipleDifferentDomainsForTasks(boolean needsMultipleDifferentDomainsForTasks) {
		this.needsMultipleDifferentDomainsForTasks = needsMultipleDifferentDomainsForTasks;
		synchronized (tasks) { // If it's set on the go, should update all tasks (hopefully never happens...)
			for (Task task : tasks) {
				task.needsMultipleDifferentDomains(needsMultipleDifferentDomainsForTasks);
			}
		}
		return this;
	}

	// END Setter/Getter
	@Override
	public Procedure clone() {
		Procedure procedure = null;
		try {
			procedure = (Procedure) super.clone();
			return procedure;
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String toString() {
		return "Procedure [executable=" + executable + ", procedureIndex=" + procedureIndex + ", tasks=" + tasks + ", dataInputDomain="
				+ dataInputDomain + ", combiner=" + combiner + ", nrOfSameResultHashForTasks=" + nrOfSameResultHashForTasks + ", jobId=" + jobId
				+ ", needsMultipleDifferentDomainsForTasks=" + needsMultipleDifferentDomainsForTasks + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((executable == null) ? 0 : executable.hashCode());
		result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
		result = prime * result + procedureIndex;
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
		Procedure other = (Procedure) obj;
		if (executable == null) {
			if (other.executable != null)
				return false;
		} else if (!executable.equals(other.executable))
			return false;
		if (jobId == null) {
			if (other.jobId != null)
				return false;
		} else if (!jobId.equals(other.jobId))
			return false;
		if (procedureIndex != other.procedureIndex)
			return false;
		return true;
	}

}
