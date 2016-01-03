package mapreduce.manager.broadcasting.broadcastmessages;

import mapreduce.execution.IDomain;
import mapreduce.execution.procedures.ExecutorTaskDomain;
import mapreduce.execution.procedures.JobProcedureDomain;
import mapreduce.manager.broadcasting.broadcastmessageconsumer.IMessageConsumer;

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

	public static CompletedBCMessage createCompletedTaskBCMessage(ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {
		return new CompletedBCMessage(outputDomain, inputDomain, BCMessageStatus.COMPLETED_TASK);
	}

	public static CompletedBCMessage createCompletedProcedureBCMessage(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
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

}
