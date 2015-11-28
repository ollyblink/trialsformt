package mapreduce.execution.broadcasthandler.messageconsumer;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.TreeMultimap;

import mapreduce.execution.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.execution.broadcasthandler.broadcastmessages.JobStatus;

public class MessageConsumerBCMessageHelper extends Thread {
	private Multimap<JobStatus, IBCMessage> tracking;

	public MessageConsumerBCMessageHelper() {
		Multimap<JobStatus, IBCMessage> tmp = TreeMultimap.create();
		this.tracking = Multimaps.synchronizedMultimap(tmp);
	}

	public static MessageConsumerBCMessageHelper newInstance() {
		return new MessageConsumerBCMessageHelper();
	}

	public void manageMessage(IBCMessage bcMessage) {
		tracking.put(bcMessage.status(), bcMessage);
		ExecutorService service = Executors.newFixedThreadPool(tracking.keySet().size());
		for (JobStatus j : tracking.keySet()) {
			final JobStatus jobStatus = j;
			final Collection<IBCMessage> msgs = tracking.get(j);
			service.execute(new Runnable() {

				@Override
				public void run() {
					System.err.println(jobStatus + ":{" + msgs + "}");

				}
			});
		}
		service.shutdown();
		while (service.isTerminated()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
