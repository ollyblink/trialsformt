package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.execution.job.Job;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;

public class FinishedProcedureBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6148158172287886702L;
	private BCMessageStatus status;

	@Override
	public BCMessageStatus status() {
		return status;
	}

	private FinishedProcedureBCMessage(BCMessageStatus status) {
		this.status = status;
	}

	public static FinishedProcedureBCMessage create() {
		return new FinishedProcedureBCMessage(BCMessageStatus.FINISHED_PROCEDURE);
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		messageConsumer.handleFinishedProcedure(job);
	}

	@Override
	public FinishedProcedureBCMessage sender(final String sender) {
		return (FinishedProcedureBCMessage) super.sender(sender);
	}

	@Override
	public FinishedProcedureBCMessage job(Job job) {
		super.job(job);
		return this;
	}
}