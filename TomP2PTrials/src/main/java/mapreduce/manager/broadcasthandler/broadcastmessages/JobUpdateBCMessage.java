package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;

public class JobUpdateBCMessage extends AbstractJobBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6148158172287886702L;
	private BCMessageStatus status;

	@Override
	public BCMessageStatus status() {
		return status;
	}

	private JobUpdateBCMessage(BCMessageStatus status) {
		this.status = status;
	}

	public static JobUpdateBCMessage newFinishedAllTasksBCMessage() {
		return new JobUpdateBCMessage(BCMessageStatus.FINISHED_ALL_TASKS);
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		job.isFinishedFor(job.procedure(job.currentProcedureIndex()), true);
		messageConsumer.updateJob(job, sender);
	}

}