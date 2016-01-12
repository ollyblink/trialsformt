package mapreduce.execution;

import mapreduce.utils.DomainProvider;
import net.tomp2p.peers.Number160;

public class JobProcedureDomain implements IDomain {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1860215470614190272L;
	private String jobId;
	private String procedureExecutor;
	private String procedureSimpleName;
	private int procedureIndex;
	private Number160 resultHash;
	/**
	 * Number of tasks for this procedure (may be different from tasks.size() because tasks are pulled after another and not all at the same time).
	 * When the preceding procedure finishes, it will add the number of task's (==tasksSize) such that the next procedure knows how many tasks there
	 * are to be processed.
	 */
	private int tasksSize;
	/**
	 * This data item is simply here for the MessageConsumer to decide which result to take if two executors execute a procedure on different input
	 * domains
	 */
	private int nrOfFinishedTasks;

	private JobProcedureDomain() {

	}

	public static JobProcedureDomain create(String jobId, String procedureExecutor, String procedureSimpleName, int procedureIndex) {
		return new JobProcedureDomain(jobId, procedureExecutor, procedureSimpleName, procedureIndex);
	}

	private JobProcedureDomain(String jobId, String procedureExecutor, String procedureSimpleName, int procedureIndex) {

		this.jobId = jobId;
		this.procedureExecutor = procedureExecutor;
		this.procedureSimpleName = procedureSimpleName;
		this.procedureIndex = procedureIndex;
		this.resultHash = Number160.ZERO;
	}

	@Override
	public Number160 resultHash() {
		return resultHash;
	}

	@Override
	public JobProcedureDomain resultHash(Number160 resultHash) {
		this.resultHash = resultHash;
		return this;
	}

	@Override
	public String executor() {
		return procedureExecutor;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
		result = prime * result + nrOfFinishedTasks;
		result = prime * result + ((procedureExecutor == null) ? 0 : procedureExecutor.hashCode());
		result = prime * result + procedureIndex;
		result = prime * result + ((procedureSimpleName == null) ? 0 : procedureSimpleName.hashCode());
		result = prime * result + ((resultHash == null) ? 0 : resultHash.hashCode());
		result = prime * result + tasksSize;
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
		JobProcedureDomain other = (JobProcedureDomain) obj;
		if (jobId == null) {
			if (other.jobId != null)
				return false;
		} else if (!jobId.equals(other.jobId))
			return false;
		if (nrOfFinishedTasks != other.nrOfFinishedTasks)
			return false;
		if (procedureExecutor == null) {
			if (other.procedureExecutor != null)
				return false;
		} else if (!procedureExecutor.equals(other.procedureExecutor))
			return false;
		if (procedureIndex != other.procedureIndex)
			return false;
		if (procedureSimpleName == null) {
			if (other.procedureSimpleName != null)
				return false;
		} else if (!procedureSimpleName.equals(other.procedureSimpleName))
			return false;
		if (resultHash == null) {
			if (other.resultHash != null)
				return false;
		} else if (!resultHash.equals(other.resultHash))
			return false;
		if (tasksSize != other.tasksSize)
			return false;
		return true;
	}

	public String jobId() {
		return this.jobId;
	}

	public String procedureSimpleName() {
		return this.procedureSimpleName;
	}

	public Integer procedureIndex() {
		return this.procedureIndex;
	}

	@Override
	public String toString() {
		return DomainProvider.INSTANCE.jobProcedureDomain(this);
	}

	@Override
	public JobProcedureDomain clone() {
		JobProcedureDomain jpd = null;
		try {
			jpd = (JobProcedureDomain) super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return jpd;
	}

	public int tasksSize() {
		return tasksSize;
	}

	public JobProcedureDomain tasksSize(int tasksSize) {
		this.tasksSize = tasksSize;
		return this;
	}

	public int nrOfFinishedTasks() {
		return this.nrOfFinishedTasks;
	}

	public JobProcedureDomain nrOfFinishedTasks(int nrOfFinishedTasks) {
		this.nrOfFinishedTasks = nrOfFinishedTasks;
		return this;
	}

}