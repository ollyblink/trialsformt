package firstdesignidea.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import firstdesignidea.execution.exceptions.IncorrectFormatException;
import firstdesignidea.execution.exceptions.NotSetException;
import firstdesignidea.execution.jobtask.IJobManager;
import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.JobStatus;
import firstdesignidea.execution.jobtask.Task;
import firstdesignidea.execution.jobtask.TaskStatus;
import firstdesignidea.execution.scheduling.IJobScheduler;
import firstdesignidea.storage.IDHTConnection;
import firstdesignidea.utils.FormatUtils;
import firstdesignidea.utils.PortGenerator;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MRJobExecutor implements IJobManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutor.class);
	private static final int MIN_PORT = 10000;
	private int port;
	private String ip;

	private IJobScheduler jobScheduler;
//	private IMapperEngine mapperEngine;
//	private IReducerEngine reducerEngine;
	private IDHTConnection dhtConnection;

	private MRJobExecutor() {

	}

	public static MRJobExecutor newJobExecutor() {
		return new MRJobExecutor();
	}

	public MRJobExecutor ip(String ip) {
		this.ip = ip;
		return this;
	}

	public MRJobExecutor port(int port) {
		this.port = port;
		return this;
	}

	public String ip() throws IncorrectFormatException, NotSetException {
		if (ip != null) {
			if (FormatUtils.isCorrectIP4(ip)) {
				return this.ip;
			} else {
				throw new IncorrectFormatException("This IP has not the correct format.");
			}
		} else {
			throw new NotSetException("Could not find a valid IP.");
		}
	}

	public int port() throws IncorrectFormatException, NotSetException {
		if (port <= MIN_PORT) {
			this.port = PortGenerator.generatePort();
		}
		return this.port;
	}

//	public MRJobExecutor mapperEngine(IMapperEngine mapperEngine) {
//		this.mapperEngine = mapperEngine;
//		return this;
//	}
//
//	public MRJobExecutor reducerEngine(IReducerEngine reducerEngine) {
//		this.reducerEngine = reducerEngine;
//		return this;
//	}

	public MRJobExecutor dhtConnection(IDHTConnection dhtConnection) {
		this.dhtConnection = dhtConnection;
		if (this.dhtConnection.broadcastHandler() != null) {
			this.dhtConnection.broadcastHandler().jobManager(this);
		}
		return this;
	}

	public void startListeningAndExecuting() {
		// Bootstrap to known peer or make himself known peer if none is yet
		// register different listeners
		// register broadcast listener on peer creation
		// bootstrap(peer);

	}

	@Override
	public void scheduleJobs(Number160 jobQueueLocation) {
		dhtConnection.get(jobQueueLocation, getJobsFromjobQueueListener());

	}

	private BaseFutureListener<FutureGet> getJobsFromjobQueueListener() {
		return new BaseFutureListener<FutureGet>() {
 

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					future.dataMap().values();
					List<Job> jobs = new ArrayList<Job>();
					Job chosen = jobScheduler.schedule(jobs);
					Number160 jobIdHash = Number160.createHash(chosen.id());
//					Map<Task, Map<PeerAddress, TaskStatus>> mapTasks = mapperEngine.split(chosen);
//					dhtConnection.add(jobIdHash, mapTasks, putTaskMapListener(dhtConnection, chosen.id(), jobIdHash));
				} else {
					logger.error("Could not get jobs from job queue.");

				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("Exception caught", t);
			}

		};
	}

	private BaseFutureListener<FuturePut> putTaskMapListener(final IDHTConnection dhtConnection, final String jobId, final Number160 jobIdHash) {
		return new BaseFutureListener<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
//					dhtConnection.broadcast(jobIdHash, JobStatus.DISTRIBUTED_MAP_TASKS);
				} else {
					logger.error("Could not put task map for job " + jobId);
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("Exception in putTaskMapListener generation", t);
			}
		};
	}

	@Override
	public void startMapping(Number160 jobLocation) {
		dhtConnection.get(jobLocation, new BaseFutureListener<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					Map<Task, Map<PeerAddress, TaskStatus>> mapTasks = (Map<Task, Map<PeerAddress, TaskStatus>>) future.data().object();
				 
				} else {
					logger.error("Could not get job from job location.");
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				logger.debug("Exception caught.", t);
			}

		});
	}

	@Override
	public void scheduleReduceTasks() {
		// TODO Auto-generated method stub

	}

	@Override
	public void startReducing() {
		// TODO Auto-generated method stub

	}

}
