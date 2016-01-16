package mapreduce.engine.broadcasting.broadcasthandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.Procedures;
import mapreduce.utils.DomainProvider;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;

public class JobCalculationBroadcastHandler extends AbstractMapReduceBroadcastHandler {

	private static Logger logger = LoggerFactory.getLogger(JobCalculationBroadcastHandler.class);

	@Override
	public void evaluateReceivedMessage(IBCMessage bcMessage) {
		String jobId = bcMessage.inputDomain().jobId();
		Job job = getJob(jobId);
		if (job == null) {
			dhtConnectionProvider.get(DomainProvider.JOB, jobId).addListener(new BaseFutureAdapter<FutureGet>() {

				@Override
				public void operationComplete(FutureGet future) throws Exception {
					if (future.isSuccess()) {
						if (future.data() != null) {
							Job job = (Job) future.data().object();
							for (Procedure procedure : job.procedures()) {
								if (procedure.executable() instanceof String) {// Means a java script function --> convert
									procedure.executable(Procedures.convertJavascriptToJava((String) procedure.executable()));
								}
								if (procedure.combiner() != null && procedure.combiner() instanceof String) {
									// Means a java script function --> convert
									procedure.combiner(Procedures.convertJavascriptToJava((String) procedure.combiner()));
								}
							}
							processMessage(bcMessage, job);
						}
					} else {
						logger.info("No success retrieving Job (" + jobId + ") from DHT. Try again");
					}
				}
			});
		} else {// Don't receive it if I sent it to myself
			if (messageConsumer.executor().id() != null && !bcMessage.outputDomain().executor().equals(messageConsumer.executor().id())) {
				processMessage(bcMessage, job);
			}
		}

	}

	@Override
	public void processMessage(IBCMessage bcMessage, Job job) { 
		if (!job.isFinished()) { 
			jobFuturesFor.put(job, taskExecutionServer.submit(new Runnable() {

				@Override
				public void run() {
					bcMessage.execute(job, messageConsumer);
				}
			}, job.priorityLevel(), job.creationTime(), bcMessage.procedureIndex(), bcMessage.status(), bcMessage.creationTime()));
			updateTimestamp(job, bcMessage);
		} else {
			synchronized (this) {
				abortJobExecution(job);
			}
		}
	}

	/**
	 *
	 * 
	 * @param nrOfConcurrentlyExecutedBCMessages
	 *            number of threads for this thread pool: how many bc messages may be executed at the same time?
	 * @return
	 */
	public static JobCalculationBroadcastHandler create(int nrOfConcurrentlyExecutedBCMessages) {
		return new JobCalculationBroadcastHandler(nrOfConcurrentlyExecutedBCMessages);
	}

	// Setter, Getter, Creator, Constructor follow below..
	private JobCalculationBroadcastHandler(int nrOfConcurrentlyExecutedBCMessages) {
		super(nrOfConcurrentlyExecutedBCMessages);
	}

}
