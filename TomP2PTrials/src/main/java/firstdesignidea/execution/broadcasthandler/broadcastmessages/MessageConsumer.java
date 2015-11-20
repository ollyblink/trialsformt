package firstdesignidea.execution.broadcasthandler.broadcastmessages;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.server.MRJobExecutor;

/**
 * <code>MessageConsumer</code> stores incoming <code>IBCMessage</code> on a queue for future processing
 * 
 * @author ozihler
 *
 */
public class MessageConsumer implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

	private static final long DEFAULT_SLEEPING_TIME = 500;
	private BlockingQueue<IBCMessage> bcMessages;
	private MRJobExecutor mrJobExecutor;
	private long sleepingTime;
	private boolean isWaitingForExecutionFinishing;

	private MessageConsumer(BlockingQueue<IBCMessage> bcMessages, MRJobExecutor mrJobExecutor) {
		this.bcMessages = bcMessages;
		this.mrJobExecutor = mrJobExecutor;
	}

	public static MessageConsumer newMessageConsumer(BlockingQueue<IBCMessage> bcMessages, MRJobExecutor mrJobExecutor) {
		return new MessageConsumer(bcMessages, mrJobExecutor);
	}

	@Override
	public void run() {
		try {
			logger.info("In MessageConsumer");
			logger.info("number of BC messages: " + bcMessages.size());
			while (bcMessages.isEmpty()) {
			}
			IBCMessage nextMessage = bcMessages.take();
			nextMessage.execute(mrJobExecutor);
			this.isWaitingForExecutionFinishing(true);
			while (isWaitingForExecutionFinishing()) {
				Thread.sleep(sleepingTime);
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public long sleepingTime() {
		return (sleepingTime == 0 ? DEFAULT_SLEEPING_TIME : sleepingTime);
	}

	public MessageConsumer sleepingTime(long sleepingTime) {
		this.sleepingTime = sleepingTime;
		return this;
	}

	public boolean isWaitingForExecutionFinishing() {
		return isWaitingForExecutionFinishing;
	}

	public MessageConsumer isWaitingForExecutionFinishing(boolean isWaitingForExecutionFinishing) {
		this.isWaitingForExecutionFinishing = isWaitingForExecutionFinishing;
		return this;
	}

}
