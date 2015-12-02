package mapreduce.execution.scheduling.taskresultcomparisonscheduling;

import java.util.List;

import mapreduce.execution.jobtask.Task;
import mapreduce.execution.scheduling.ITaskScheduler;

public abstract class AbstractTaskResultComparisonScheduler implements ITaskScheduler {
	@Override
	public Task schedule(List<Task> tasksToSchedule) {
		if (tasksToSchedule != null && tasksToSchedule.size() > 0) {
			return scheduleNonNull(tasksToSchedule);
		} else {
			return null;
		}
	}

	protected abstract Task scheduleNonNull(List<Task> tasksToSchedule);

	protected boolean allTasksResultsAreAssigned(List<Task> tasksToSchedule) {
		boolean allAssigned = true;
		for (Task task : tasksToSchedule) {
			if (task.finalDataLocation() == null) {
				allAssigned = false;
			}
		}
		return allAssigned;
	}

	protected boolean noTaskResultAssignedYet(List<Task> tasksToSchedule) {
		boolean nonAssigned = true;
		for (Task task : tasksToSchedule) {
			if (task.finalDataLocation() != null) {
				nonAssigned = false;
			}
		}
		return nonAssigned;
	}
}
