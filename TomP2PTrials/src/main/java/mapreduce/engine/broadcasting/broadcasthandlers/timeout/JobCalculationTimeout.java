package mapreduce.engine.broadcasting.broadcasthandlers.timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.AbstractMapReduceBroadcastHandler;
import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.execution.jobs.Job;

public class JobCalculationTimeout extends AbstractTimeout {
	private static Logger logger = LoggerFactory.getLogger(JobCalculationTimeout.class);

	public JobCalculationTimeout(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp) {
		super(broadcastHandler, job, currentTimestamp);
	}

	@Override
	public void run() {
		sleep();
		synchronized (this.broadcastHandler) {
			logger.info("for " + broadcastHandler.executorId() + " Timeout for job " + job + ", last bc message: " + bcMessage);
			if (bcMessage.inputDomain() != null && bcMessage.inputDomain().procedureIndex() == -1) {
				// handle start differently first, because it may be due to expected file size that is not the same...
				int actualTasksSize = job.currentProcedure().tasks().size();
				int expectedTasksSize = bcMessage.inputDomain().expectedNrOfFiles();
				if (actualTasksSize < expectedTasksSize) {
					job.currentProcedure().dataInputDomain().expectedNrOfFiles(actualTasksSize);
					JobCalculationExecutor executor = (JobCalculationExecutor) broadcastHandler.messageConsumer().executor();
					CompletedBCMessage msg = executor.tryFinishProcedure(job.currentProcedure());
					if (msg != null) {
						broadcastHandler.processMessage(msg, broadcastHandler.getJob(job.currentProcedure().jobId()));
						broadcastHandler.dhtConnectionProvider().broadcastCompletion(msg);
						logger.info("tryFinishProcedure: Broadcasted Completed Procedure MSG: " + msg);
					}
				}
			} else {
				this.broadcastHandler.abortJobExecution(job);
			}
		}
	}

}
