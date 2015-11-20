package firstdesignidea.server;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.broadcasthandler.broadcastmessages.IBCMessage;
import firstdesignidea.execution.broadcasthandler.broadcastmessages.MessageConsumer;
import firstdesignidea.execution.broadcasthandler.broadcastobserver.IBroadcastListener;
import firstdesignidea.execution.computation.IMapReduceProcedure;
import firstdesignidea.execution.computation.context.IContext;
import firstdesignidea.execution.computation.context.PrintContext;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.JobStatus;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.execution.scheduling.ITaskScheduler;
import firstdesignidea.execution.scheduling.MinAssignedWorkersTaskScheduler;
import firstdesignidea.storage.IDHTConnectionProvider;

public class MRJobExecutor implements IBroadcastListener {
	private static final ITaskScheduler DEFAULT_TASK_SCHEDULER = new MinAssignedWorkersTaskScheduler();

	private static Logger logger = LoggerFactory.getLogger(MRJobExecutor.class);

	private IDHTConnectionProvider dhtConnectionProvider;
	private BlockingQueue<IBCMessage> bcMessages;
	private ITaskScheduler taskScheduler;
	private int maxNrOfFinishedPeers;

	private MessageConsumer messageConsumer;

	private IContext context;

	private MRJobExecutor(IDHTConnectionProvider dhtConnectionProvider) {
		this.bcMessages = new LinkedBlockingQueue<IBCMessage>();
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.messageConsumer = MessageConsumer.newMessageConsumer(bcMessages, this);
		new Thread(messageConsumer).start();

	}

	public static MRJobExecutor newJobExecutor(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobExecutor(dhtConnectionProvider);
	}

	private MRJobExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		this.dhtConnectionProvider.broadcastDistributor().broadcastListener(this);
		return this;
	}
	
	public MRJobExecutor taskScheduler(ITaskScheduler taskScheduler){
		this.taskScheduler = taskScheduler;
		return this;
	}
	public ITaskScheduler taskScheduler(){
		if(this.taskScheduler == null){
			this.taskScheduler = DEFAULT_TASK_SCHEDULER;
		}
		return this.taskScheduler;
	}
	

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public MRJobExecutor maxNrOfFinishedPeers(int maxNrOfFinishedPeers) {
		this.maxNrOfFinishedPeers = maxNrOfFinishedPeers;
		return this;
	}

	public int maxNrOfFinishedPeers() {
		if(this.maxNrOfFinishedPeers == 0){
			this.maxNrOfFinishedPeers = 1;
		}
		return this.maxNrOfFinishedPeers;
	}

	public MRJobExecutor context(IContext context) {
		this.context = context;
		return this;
	}

	public IContext context() {
		if (context == null) {
			this.context = new PrintContext();
		}
		return this.context;
	}

	public void executeJob(Job job) {
		List<Task> tasks = this.taskScheduler().schedule(job);
		for (Task task : tasks) {
			if (task.numberOfPeersWithStatus(JobStatus.FINISHED_TASK) == this.maxNrOfFinishedPeers) {
				continue; // there are already enough peers that finished this task
			} else {
				List<Serializable> dataForTask = this.dhtConnectionProvider.getDataForTask(task);
				for (Object key : task.keys()) {
					for (Serializable value : dataForTask) {
						callProcedure(key, value, task.procedure());
					}
				}
			}
		}
	}

	private void callProcedure(Object key, Serializable value, IMapReduceProcedure<?, ?, ?, ?> procedure) {
		Method process = procedure.getClass().getMethods()[0];
		try {
			process.invoke(procedure, new Object[] { key, value, context() });
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	public void start() {
		logger.info("Try to connect.");
		dhtConnectionProvider.connect();
	}

	@Override
	public void inform(IBCMessage bcMessage) {
		bcMessages.add(bcMessage);
	}

}
