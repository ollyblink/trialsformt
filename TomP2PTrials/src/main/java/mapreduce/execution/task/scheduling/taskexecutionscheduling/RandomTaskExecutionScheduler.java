package mapreduce.execution.task.scheduling.taskexecutionscheduling;

import java.util.List;
import java.util.Random;

import mapreduce.execution.task.Task2;

public class RandomTaskExecutionScheduler extends AbstractTaskExecutionScheduler {
	private static final Random RND = new Random();
	// private static Logger logger = LoggerFactory.getLogger(RandomTaskScheduler.class);

	private RandomTaskExecutionScheduler() {
	}

	public static RandomTaskExecutionScheduler newInstance() {
		return new RandomTaskExecutionScheduler();
	}

	@Override
	protected Task2 scheduleNonNull(List<Task2> tasksToSchedule) {
		Task2 assignedTask = null;
		if (!allTasksAreFinished(tasksToSchedule)) {
			do {
				assignedTask = tasksToSchedule.get(RND.nextInt(tasksToSchedule.size()));
			} while (assignedTask.isFinished());
		}
		return assignedTask;
	}
}
