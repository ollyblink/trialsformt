package firstdesignidea.execution.broadcasthandler.broadcastmessages;

import java.util.concurrent.BlockingQueue;

import firstdesignidea.server.MRJobExecutor;

public class MessageConsumer implements Runnable {

	private BlockingQueue<IBCMessage> bcMessages;
	private MRJobExecutor mrJobExecutor;

	public MessageConsumer(BlockingQueue<IBCMessage> bcMessages, MRJobExecutor mrJobExecutor) {
		this.bcMessages = bcMessages;
		this.mrJobExecutor = mrJobExecutor;
	}

	@Override
	public void run() {
		try {
			IBCMessage nextMessage = bcMessages.take();
			nextMessage.execute(mrJobExecutor);

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
