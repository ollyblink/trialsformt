package mapreduce.execution.task.taskexecutorscleaner;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Multimap;

import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.storage.LocationBean;
import mapreduce.testutils.TestUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class TaskExecutorsCleanerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testWith3DifferentPeers() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		Job job = TestUtils.testJob();
		TaskExecutorsCleaner cleaner = TaskExecutorsCleaner.newInstance();
		Multimap<Task, LocationBean> toRemove = cleaner.cleanUp(job);
		/*
		 * max nr of finished executors: 3 task in job have the following executing peers: 1FE, 2FF, 3F, 4E, 5FF So the job after cleanUp should have:
		 * 1: F, 2: F, 3: F and the tuplesToRemove should have data for 1E, 2F, 4E, 5FF
		 * 
		 */
		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		for (Task task : tasks) {
			ArrayList<PeerAddress> remainingPeers = task.allAssignedPeers();
			assertEquals(3, remainingPeers.size());
			assertEquals(true, remainingPeers.contains(new PeerAddress(new Number160(1))));
			assertEquals(true, remainingPeers.contains(new PeerAddress(new Number160(2))));
			assertEquals(true, remainingPeers.contains(new PeerAddress(new Number160(3))));
			assertEquals(false, remainingPeers.contains(new PeerAddress(new Number160(4))));
			assertEquals(false, remainingPeers.contains(new PeerAddress(new Number160(5))));

			assertEquals(1, task.statiForPeer(new PeerAddress(new Number160(1))).size());
			assertEquals(1, task.statiForPeer(new PeerAddress(new Number160(2))).size());
			assertEquals(1, task.statiForPeer(new PeerAddress(new Number160(3))).size());
			assertEquals(0, task.statiForPeer(new PeerAddress(new Number160(4))).size());
			assertEquals(0, task.statiForPeer(new PeerAddress(new Number160(5))).size());

			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(1))).get(0));
			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(2))).get(0));
			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(3))).get(0));

			Collection<LocationBean> collection = toRemove.get(task);
			List<Number160> rP = new ArrayList<Number160>();
			for (LocationBean b : collection) {
				Field peerIdField = b.getClass().getDeclaredField("peerId");
				peerIdField.setAccessible(true);
				Number160 peerId = (Number160) peerIdField.get(b);
				rP.add(peerId);
			}
			assertEquals(5, rP.size());
			assertEquals(true, rP.contains(new Number160(1)));
			assertEquals(true, rP.contains(new Number160(2)));
			assertEquals(false, rP.contains(new Number160(3)));
			assertEquals(true, rP.contains(new Number160(4)));
			assertEquals(true, rP.contains(new Number160(5)));
  
		}
	}
	@Test
	public void testWithSamePeerTwice() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		Job job = TestUtils.testJob2();
		TaskExecutorsCleaner cleaner = TaskExecutorsCleaner.newInstance();
		Multimap<Task, LocationBean> toRemove = cleaner.cleanUp(job);
		/*
		 * max nr of finished executors: 3 task in job have the following executing peers: 1FE, 2FF, 3F, 4E, 5FF So the job after cleanUp should have:
		 * 1: F, 2: F, 3: F and the tuplesToRemove should have data for 1E, 2F, 4E, 5FF
		 * 
		 */
		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		for (Task task : tasks) {
			ArrayList<PeerAddress> remainingPeers = task.allAssignedPeers();
			assertEquals(2, remainingPeers.size());
			assertEquals(true, remainingPeers.contains(new PeerAddress(new Number160(1))));
			assertEquals(true, remainingPeers.contains(new PeerAddress(new Number160(2)))); 

			assertEquals(2, task.statiForPeer(new PeerAddress(new Number160(1))).size());
			assertEquals(1, task.statiForPeer(new PeerAddress(new Number160(2))).size()); 

			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(1))).get(0));
			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(1))).get(1));
			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(2))).get(0));

			Collection<LocationBean> collection = toRemove.get(task);
			List<Number160> rP = new ArrayList<Number160>();
			for (LocationBean b : collection) {
				Field peerIdField = b.getClass().getDeclaredField("peerId");
				peerIdField.setAccessible(true);
				Number160 peerId = (Number160) peerIdField.get(b);
				rP.add(peerId);
			}
			assertEquals(2, rP.size());
			assertEquals(true, rP.contains(new Number160(1)));
			assertEquals(true, rP.contains(new Number160(2))); 
  
		}
	}
	
	@Test
	public void testWithOnlyOnePeer() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		Job job = TestUtils.testJob3();
		TaskExecutorsCleaner cleaner = TaskExecutorsCleaner.newInstance();
		Multimap<Task, LocationBean> toRemove = cleaner.cleanUp(job);
		/*
		 * max nr of finished executors: 3 task in job have the following executing peers: 1FE, 2FF, 3F, 4E, 5FF So the job after cleanUp should have:
		 * 1: F, 2: F, 3: F and the tuplesToRemove should have data for 1E, 2F, 4E, 5FF
		 * 
		 */
		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		for (Task task : tasks) {
			ArrayList<PeerAddress> remainingPeers = task.allAssignedPeers();
			assertEquals(1, remainingPeers.size());
			assertEquals(true, remainingPeers.contains(new PeerAddress(new Number160(1)))); 

			assertEquals(3, task.statiForPeer(new PeerAddress(new Number160(1))).size()); 

			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(1))).get(0));
			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(1))).get(1));
			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(1))).get(2));

			Collection<LocationBean> collection = toRemove.get(task);
			List<Number160> rP = new ArrayList<Number160>();
			for (LocationBean b : collection) {
				Field peerIdField = b.getClass().getDeclaredField("peerId");
				peerIdField.setAccessible(true);
				Number160 peerId = (Number160) peerIdField.get(b);
				rP.add(peerId);
			}
			assertEquals(1, rP.size());
			assertEquals(true, rP.contains(new Number160(1))); 
  
		}
	}
	
	@Test
	public void testWithTooFewPeersButFinishesAnyways() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		Job job = TestUtils.testJob4();
		TaskExecutorsCleaner cleaner = TaskExecutorsCleaner.newInstance();
		Multimap<Task, LocationBean> toRemove = cleaner.cleanUp(job);
		/*
		 * max nr of finished executors: 3 task in job have the following executing peers: 1FE, 2FF, 3F, 4E, 5FF So the job after cleanUp should have:
		 * 1: F, 2: F, 3: F and the tuplesToRemove should have data for 1E, 2F, 4E, 5FF
		 * 
		 */
		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		for (Task task : tasks) {
			ArrayList<PeerAddress> remainingPeers = task.allAssignedPeers();
			assertEquals(1, remainingPeers.size());
			assertEquals(true, remainingPeers.contains(new PeerAddress(new Number160(1)))); 

			assertEquals(2, task.statiForPeer(new PeerAddress(new Number160(1))).size()); 

			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(1))).get(0));
			assertEquals(BCMessageStatus.FINISHED_TASK, task.statiForPeer(new PeerAddress(new Number160(1))).get(1)); 

			Collection<LocationBean> collection = toRemove.get(task);
			List<Number160> rP = new ArrayList<Number160>();
			for (LocationBean b : collection) {
				Field peerIdField = b.getClass().getDeclaredField("peerId");
				peerIdField.setAccessible(true);
				Number160 peerId = (Number160) peerIdField.get(b);
				rP.add(peerId);
			}
			assertEquals(0, rP.size()); 
  
		}
	}
}
