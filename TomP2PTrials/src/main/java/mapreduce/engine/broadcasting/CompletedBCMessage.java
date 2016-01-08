package mapreduce.engine.broadcasting;

import mapreduce.engine.messageConsumer.IMessageConsumer;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.IDomain;
import mapreduce.execution.JobProcedureDomain;

public class CompletedBCMessage implements IBCMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8321149591470136899L;
	private final IDomain outputDomain;
	private final JobProcedureDomain inputDomain;
	private final BCMessageStatus status;

	private CompletedBCMessage(IDomain outputDomain, JobProcedureDomain inputDomain, BCMessageStatus status) {
		this.outputDomain = outputDomain;
		this.inputDomain = inputDomain;
		this.status = status;
	}

	public static CompletedBCMessage createCompletedTaskBCMessage(IDomain outputDomain, JobProcedureDomain inputDomain) {
		return new CompletedBCMessage(outputDomain, inputDomain, BCMessageStatus.COMPLETED_TASK);
	}

	public static CompletedBCMessage createCompletedProcedureBCMessage(IDomain outputDomain, JobProcedureDomain inputDomain) {
		return new CompletedBCMessage(outputDomain, inputDomain, BCMessageStatus.COMPLETED_PROCEDURE);
	}

	@Override
	public IDomain outputDomain() {
		return outputDomain;
	}

	@Override
	public JobProcedureDomain inputDomain() {
		return inputDomain;
	}

	@Override
	public BCMessageStatus status() {
		return status;
	}

	@Override
	public int compareTo(IBCMessage o) {
		return status.compareTo(o.status());
	}

	@Override
	public void execute(IMessageConsumer messageConsumer) {
		if (status == BCMessageStatus.COMPLETED_TASK) {
			messageConsumer.handleCompletedTask((ExecutorTaskDomain) outputDomain, inputDomain);
		} else { // status == BCMessageStatus.COMPLETED_PROCEDURE
			messageConsumer.handleCompletedProcedure((JobProcedureDomain) outputDomain, inputDomain);
		}
	}

	@Override
	public String toString() {
		return "CompletedBCMessage(" + status + ") [outputDomain=" + outputDomain.toString() + " from inputDomain=" + inputDomain.toString() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((inputDomain == null) ? 0 : inputDomain.hashCode());
		result = prime * result + ((outputDomain == null) ? 0 : outputDomain.hashCode());
		result = prime * result + ((status == null) ? 0 : status.hashCode());
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
		CompletedBCMessage other = (CompletedBCMessage) obj;
		if (inputDomain == null) {
			if (other.inputDomain != null)
				return false;
		} else if (!inputDomain.equals(other.inputDomain))
			return false;
		if (outputDomain == null) {
			if (other.outputDomain != null)
				return false;
		} else if (!outputDomain.equals(other.outputDomain))
			return false;
		if (status != other.status)
			return false;
		return true;
	}

}