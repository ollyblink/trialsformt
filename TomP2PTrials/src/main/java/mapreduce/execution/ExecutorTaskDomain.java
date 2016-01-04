package mapreduce.execution;

import mapreduce.utils.DomainProvider;
import net.tomp2p.peers.Number160;

public class ExecutorTaskDomain implements IDomain {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4306577142597619221L;
	private String taskId;
	private String taskExecutor;
	private int taskStatusIndex;
	private int taskSubmissionCount;
	private long taskCreationTime;
	private Number160 resultHash;
	private JobProcedureDomain jobProcedureDomain;

	public ExecutorTaskDomain(String taskId, String taskExecutor, int taskStatusIndex, JobProcedureDomain jobProcedureDomain) {

		this.taskId = taskId;
		this.taskExecutor = taskExecutor;
		this.taskStatusIndex = taskStatusIndex;
		this.taskSubmissionCount = 0;
		this.taskCreationTime = System.currentTimeMillis();
		this.resultHash = Number160.ZERO;
		this.jobProcedureDomain = jobProcedureDomain;
	}

	@Override
	public void incrementSubmissionCount() {
		++this.taskSubmissionCount;
	}

	@Override
	public int submissionCount() {
		return this.taskSubmissionCount;
	}

	@Override
	public long creationTime() {
		return this.taskCreationTime;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((jobProcedureDomain == null) ? 0 : jobProcedureDomain.hashCode());
		result = prime * result + ((resultHash == null) ? 0 : resultHash.hashCode());
		result = prime * result + (int) (taskCreationTime ^ (taskCreationTime >>> 32));
		result = prime * result + ((taskExecutor == null) ? 0 : taskExecutor.hashCode());
		result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
		result = prime * result + taskStatusIndex;
		result = prime * result + taskSubmissionCount;
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
		ExecutorTaskDomain other = (ExecutorTaskDomain) obj;
		if (jobProcedureDomain == null) {
			if (other.jobProcedureDomain != null)
				return false;
		} else if (!jobProcedureDomain.equals(other.jobProcedureDomain))
			return false;
		if (resultHash == null) {
			if (other.resultHash != null)
				return false;
		} else if (!resultHash.equals(other.resultHash))
			return false;
		if (taskCreationTime != other.taskCreationTime)
			return false;
		if (taskExecutor == null) {
			if (other.taskExecutor != null)
				return false;
		} else if (!taskExecutor.equals(other.taskExecutor))
			return false;
		if (taskId == null) {
			if (other.taskId != null)
				return false;
		} else if (!taskId.equals(other.taskId))
			return false;
		if (taskStatusIndex != other.taskStatusIndex)
			return false;
		if (taskSubmissionCount != other.taskSubmissionCount)
			return false;
		return true;
	}

	@Override
	public Number160 resultHash() {
		return resultHash;
	}

	@Override
	public String executor() {
		return taskExecutor;
	}

	public String taskId() {
		return taskId;
	}

	public int taskStatusIndex() {
		return taskStatusIndex;
	}

	public JobProcedureDomain jobProcedureDomain() {
		return jobProcedureDomain;
	}

	@Override
	public String toString() {
		return DomainProvider.INSTANCE.concatenation(jobProcedureDomain, this);
	}

	@Override
	public void resultHash(Number160 resultHash) {
		this.resultHash = resultHash;
	}
 
}