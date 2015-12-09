package mapreduce.execution.task.scheduling.taskresultcomparisonscheduling;

import java.util.List;
import java.util.Random;

import mapreduce.execution.task.Task;

public class RandomTaskResultComparisonScheduler extends AbstractTaskResultComparisonScheduler {

	private static final Random RND = new Random();

	public static RandomTaskResultComparisonScheduler newInstance() {
		return new RandomTaskResultComparisonScheduler();
	}

	@Override
	protected Task scheduleNonNull(List<Task> tasksToSchedule) {
		Task assignedTask = null;
		if (!allTasksResultsAreAssigned(tasksToSchedule)) {
			do {
				assignedTask = tasksToSchedule.get(RND.nextInt(tasksToSchedule.size()));
			} while (assignedTask.finalDataLocation() != null);
		}
		return assignedTask;
	}

}
