package firstdesignidea.execution.scheduling;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import firstdesignidea.execution.jobtask.Job;
import firstdesignidea.execution.jobtask.JobStatus;
import firstdesignidea.execution.jobtask.Task;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MinAssignedWorkersTaskSchedulerTest {

	private static final Random RND = new Random();
	private MinAssignedWorkersTaskScheduler taskScheduler;
	private Job job;

	@Before
	public void setUp() throws Exception {
		this.taskScheduler = new MinAssignedWorkersTaskScheduler();
		Task[] tasksToTest = new Task[5];
		tasksToTest[0] = (Task.newTask().id("1")
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.FINISHED_TASK)
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.FINISHED_TASK));

		tasksToTest[1] = (Task.newTask().id("2")
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.FINISHED_TASK)
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.FINISHED_TASK)
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.EXECUTING_TASK));

		tasksToTest[2] = (Task.newTask().id("3")
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.EXECUTING_TASK)
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.EXECUTING_TASK));

		tasksToTest[3] = (Task.newTask().id("4")
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.FINISHED_TASK)
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.EXECUTING_TASK));

		tasksToTest[4] = (Task.newTask().id("5")
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.FINISHED_TASK)
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.EXECUTING_TASK)
				.updateExecutingPeerStatus(new PeerAddress(Number160.createHash(RND.nextInt(Integer.MAX_VALUE))), JobStatus.EXECUTING_TASK));

		this.job = Job.newJob().tasks(tasksToTest);
	}

	@Test
	public void testScheduledTasks() {
		// Test result should be the following order (task ids): 3,4,5,1,2
		List<Task> scheduledTasks = this.taskScheduler.schedule(job);
		// 0_ in front of the task id is needed because this is the procedure index
		assertTrue("Number of scheduled tasks is 5", scheduledTasks.size() == 5);
		assertTrue("task id 3", scheduledTasks.get(0).id().equals("0_3"));
		assertTrue("task id 4", scheduledTasks.get(1).id().equals("0_4"));
		assertTrue("task id 5", scheduledTasks.get(2).id().equals("0_5"));
		assertTrue("task id 2", scheduledTasks.get(3).id().equals("0_1"));
		assertTrue("task id 1", scheduledTasks.get(4).id().equals("0_2"));
	}

}
