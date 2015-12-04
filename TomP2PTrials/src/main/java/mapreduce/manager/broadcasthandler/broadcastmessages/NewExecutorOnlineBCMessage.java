package mapreduce.manager.broadcasthandler.broadcastmessages;

import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.IMessageConsumer;

public class NewExecutorOnlineBCMessage extends AbstractBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6468006063095217009L;

	@Override
	public BCMessageStatus status() {
		return BCMessageStatus.NEW_EXECUTOR_ONLINE;
	}

	@Override
	public void execute(final IMessageConsumer messageConsumer) {
		messageConsumer.handleNewExecutorOnline();
	}

}
