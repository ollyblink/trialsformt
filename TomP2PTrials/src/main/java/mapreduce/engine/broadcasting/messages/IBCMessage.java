package mapreduce.engine.broadcasting.messages;

import java.io.Serializable;

import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.execution.IDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;

public interface IBCMessage extends Serializable {
	public BCMessageStatus status();

	/**
	 * @return Unix timestamp of when the message was created
	 */
	public Long creationTime();

	public Integer procedureIndex();

	public void execute(Job job, final IMessageConsumer messageConsumer);

	public IDomain outputDomain();

	public JobProcedureDomain inputDomain();

}
