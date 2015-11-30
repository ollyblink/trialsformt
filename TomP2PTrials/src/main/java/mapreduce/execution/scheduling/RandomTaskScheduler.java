package mapreduce.execution.scheduling;

import java.util.List;
import java.util.Random;

import mapreduce.execution.jobtask.Task;

public class RandomTaskScheduler extends AbstractTaskScheduler {
	private static final Random RND = new Random();
	// private static Logger logger = LoggerFactory.getLogger(RandomTaskScheduler.class);

	private RandomTaskScheduler() {
	}

	public static RandomTaskScheduler newRandomTaskScheduler() {
		return new RandomTaskScheduler();
	}

	@Override
	public Task schedule(List<Task> tasksToSchedule) {
		if (tasksToSchedule == null || tasksToSchedule.size() == 0) {
			return null;
		} else {
			Task assignedTask = null;
			if (!allTasksAreFinished(tasksToSchedule)) {
				do {
					assignedTask = tasksToSchedule.get(RND.nextInt(tasksToSchedule.size()));
				} while (assignedTask.isFinished());
			}
			return assignedTask;
		}
	}
}
