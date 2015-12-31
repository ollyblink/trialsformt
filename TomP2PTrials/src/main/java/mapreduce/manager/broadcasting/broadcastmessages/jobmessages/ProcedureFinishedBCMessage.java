package mapreduce.manager.broadcasting.broadcastmessages.jobmessages;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasting.broadcastmessageconsumer.IMessageConsumer;
import mapreduce.manager.broadcasting.broadcastmessages.BCMessageStatus;

public class ProcedureFinishedBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6148158172287886702L;
	private BCMessageStatus status;

	@Override
	public BCMessageStatus status() {
		return status;
	}

	private ProcedureFinishedBCMessage(BCMessageStatus status) {
		this.status = status;
	}

	public static ProcedureFinishedBCMessage create() {
		return new ProcedureFinishedBCMessage(BCMessageStatus.FINISHED_PROCEDURE);
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		messageConsumer.handleFinishedProcedure(job);
	}

	@Override
	public ProcedureFinishedBCMessage sender(final String sender) {
		return (ProcedureFinishedBCMessage) super.sender(sender);
	}

	@Override
	public ProcedureFinishedBCMessage job(Job job) {
		super.job(job);
		return this;
	}
}