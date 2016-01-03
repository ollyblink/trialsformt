package mapreduce.manager.broadcasting.broadcastmessages.jobmessages;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasting.broadcastmessageconsumer.IMessageConsumer;
import mapreduce.manager.broadcasting.broadcastmessages.BCMessageStatus;

public class ProcedureCompletedBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6148158172287886702L;
	private BCMessageStatus status;

	@Override
	public BCMessageStatus status() {
		return status;
	}

	private ProcedureCompletedBCMessage(BCMessageStatus status) {
		this.status = status;
	}

	public static ProcedureCompletedBCMessage create() {
		return new ProcedureCompletedBCMessage(BCMessageStatus.FINISHED_PROCEDURE);
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		messageConsumer.handleFinishedProcedure(job);
	}

	@Override
	public ProcedureCompletedBCMessage sender(final String sender) {
		return (ProcedureCompletedBCMessage) super.sender(sender);
	}

	@Override
	public ProcedureCompletedBCMessage job(Job job) {
		super.job(job);
		return this;
	}
}