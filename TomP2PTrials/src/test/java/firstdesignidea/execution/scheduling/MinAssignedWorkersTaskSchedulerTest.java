package firstdesignidea.execution.scheduling;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import mapreduce.execution.jobtask.Job;
import mapreduce.execution.jobtask.JobStatus;
import mapreduce.execution.jobtask.Task;
import mapreduce.execution.scheduling.MinAssignedWorkersTaskScheduler;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class MinAssignedWorkersTaskSchedulerTest {

	private static final Random RND = new Random();
	private MinAssignedWorkersTaskScheduler taskScheduler;
	private Job job;
	private ArrayList<Task> tasks;

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

		tasks = new ArrayList<Task>();
		Collections.addAll(tasks, tasksToTest);
	}

	@Test
	public void testScheduledTasks() {
		// Test result should be the following order (task ids): 3,4,5,1,2
		Task task1 = this.taskScheduler.schedule(tasks);

		// 0_ in front of the task id is needed because this is the procedure index
		assertTrue("task id 3", task1.id().equals("0_3"));
		assertTrue("task id 3", tasks.get(0).id().equals("0_3"));
		assertTrue("task id 4", tasks.get(1).id().equals("0_4"));
		assertTrue("task id 5", tasks.get(2).id().equals("0_5"));
		assertTrue("task id 2", tasks.get(3).id().equals("0_1"));
		assertTrue("task id 1", tasks.get(4).id().equals("0_2"));
	}

}
