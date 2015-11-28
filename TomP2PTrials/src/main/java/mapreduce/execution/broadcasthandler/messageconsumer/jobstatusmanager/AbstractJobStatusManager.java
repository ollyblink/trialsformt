package mapreduce.execution.broadcasthandler.messageconsumer.jobstatusmanager;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;

public abstract class AbstractJobStatusManager extends Thread { 
	protected List<IBCMessage> bcMessages;

	protected AbstractJobStatusManager() {
		this.bcMessages = Collections.synchronizedList(new LinkedList<IBCMessage>());
	}

	public void handleMessage(IBCMessage message) {
		synchronized (bcMessages) {
			bcMessages.add(message);
		}
		
		manageMessages();
	}

	 protected abstract void manageMessages();
}
