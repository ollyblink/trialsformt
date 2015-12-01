package mapreduce.execution.broadcasthandler.broadcastmessages;

import mapreduce.execution.broadcasthandler.messageconsumer.IMessageConsumer;

public class NewExecutorOnline extends AbstractBCMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6468006063095217009L;

	@Override
	public BCStatusType status() {
		return BCStatusType.NEW_EXECUTOR_ONLINE;
	}

	@Override
	public void execute(IMessageConsumer messageConsumer) {
		messageConsumer.handleNewExecutorOnline();
	}

}
