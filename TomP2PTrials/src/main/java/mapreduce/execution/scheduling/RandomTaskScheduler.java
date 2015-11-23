package mapreduce.execution.scheduling;

import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.jobtask.Task;

public class RandomTaskScheduler implements ITaskScheduler {
	private static final Random RND = new Random();
	private static Logger logger = LoggerFactory.getLogger(RandomTaskScheduler.class);

	private RandomTaskScheduler() {
		// TODO Auto-generated constructor stub
	}

	public static RandomTaskScheduler newRandomTaskScheduler() {
		return new RandomTaskScheduler();
	}

	@Override
	public Task schedule(List<Task> tasksToSchedule) {

		if (tasksToSchedule == null || tasksToSchedule.size() == 0) {
			return null;
		} else {
			return tasksToSchedule.get(RND.nextInt(tasksToSchedule.size()));
		}
	}
}
