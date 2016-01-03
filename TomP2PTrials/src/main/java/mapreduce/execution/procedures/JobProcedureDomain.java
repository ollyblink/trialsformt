package mapreduce.execution.procedures;

import mapreduce.execution.IDomain;
import mapreduce.utils.DomainProvider;
import net.tomp2p.peers.Number160;

public class JobProcedureDomain implements IDomain {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1860215470614190272L;
	public String jobId;
	public String procedureExecutor;
	public String procedureSimpleName;
	public int procedureIndex;
	public int procedureSubmissionCount;
	public long procedureCreationTime;
	public Number160 resultHash;

	public JobProcedureDomain(String jobId, String procedureExecutor, String procedureSimpleName, int procedureIndex) {

		this.jobId = jobId;
		this.procedureExecutor = procedureExecutor;
		this.procedureSimpleName = procedureSimpleName;
		this.procedureIndex = procedureIndex;
		this.procedureSubmissionCount = 0;
		this.procedureCreationTime = System.currentTimeMillis();
		this.resultHash = Number160.ZERO;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
		result = prime * result + (int) (procedureCreationTime ^ (procedureCreationTime >>> 32));
		result = prime * result + ((procedureExecutor == null) ? 0 : procedureExecutor.hashCode());
		result = prime * result + procedureIndex;
		result = prime * result + ((procedureSimpleName == null) ? 0 : procedureSimpleName.hashCode());
		result = prime * result + procedureSubmissionCount;
		result = prime * result + ((resultHash == null) ? 0 : resultHash.hashCode());
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
		if (procedureCreationTime != other.procedureCreationTime)
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
		if (procedureSubmissionCount != other.procedureSubmissionCount)
			return false;
		if (resultHash == null) {
			if (other.resultHash != null)
				return false;
		} else if (!resultHash.equals(other.resultHash))
			return false;
		return true;
	}

	@Override
	public Number160 resultHash() {
		return resultHash;
	}

	@Override
	public void resultHash(Number160 resultHash) {
		this.resultHash = resultHash;
	}

	@Override
	public String executor() {
		return procedureExecutor;
	}

	@Override
	public long creationTime() {
		return this.procedureCreationTime;
	}

	@Override
	public int submissionCount() {
		return this.procedureSubmissionCount;
	}

	@Override
	public void incrementSubmissionCount() {
		++this.procedureSubmissionCount;
	}

	public String jobId() {
		return this.jobId;
	}

	public String procedureSimpleName() {
		return this.procedureSimpleName;
	}

	public int procedureIndex() {
		return this.procedureIndex;
	}

	@Override
	public String toString() {
		return DomainProvider.INSTANCE.jobProcedureDomain(this);
	}

}