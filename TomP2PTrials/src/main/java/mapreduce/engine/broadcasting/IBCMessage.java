package mapreduce.engine.broadcasting;

import java.io.Serializable;

import mapreduce.engine.messageConsumer.IMessageConsumer;
import mapreduce.execution.IDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;

public interface IBCMessage extends Serializable, Comparable<IBCMessage> {
	public BCMessageStatus status();
//
//	/**
//	 * @return Unix timestamp of when the message was created
//	 */
//	public Long creationTime();
//
//	/**
//	 * determines if this message was already processed
//	 * 
//	 * @return
//	 */
//	public boolean isAlreadyProcessed();
//
//	/**
//	 * Setter to determine if this message was already processed
//	 * 
//	 * @param isAlreadyProcessed
//	 * @return instance of this message
//	 */
//	public IBCMessage isAlreadyProcessed(final boolean isAlreadyProcessed);
//
//	public IBCMessage sender(final String sender);
//
//	public String sender();

	public void execute(Job job, final IMessageConsumer messageConsumer);

//	public String id();
//
//	public String jobId();

	public IDomain outputDomain();

	public JobProcedureDomain inputDomain();

}
