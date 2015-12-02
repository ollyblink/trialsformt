package mapreduce.execution.scheduling.taskexecutionscheduling;

import java.util.List;

import mapreduce.execution.jobtask.Task;
import mapreduce.execution.scheduling.ITaskScheduler;

public abstract class AbstractTaskExecutionScheduler implements ITaskScheduler {

	@Override
	public Task schedule(List<Task> tasksToSchedule) {
		if (tasksToSchedule != null && tasksToSchedule.size() > 0) { 
			return scheduleNonNull(tasksToSchedule);
		} else {
			return null;
		}
	}

	protected abstract Task scheduleNonNull(List<Task> tasksToSchedule);

	protected boolean allTasksAreFinished(List<Task> tasksToSchedule) {
		boolean allFinished = true;
		for (Task task : tasksToSchedule) {
			if (task.totalNumberOfFinishedExecutions() >= task.maxNrOfFinishedWorkers()) {
				task.isFinished(true);
			}
			if (!task.isFinished()) {
				allFinished = false;
				break;
			}
		}
		return allFinished;
	}

	protected boolean noTaskAssignedYet(List<Task> tasksToSchedule) {
		boolean nonStartedYet = true;
		for (Task task : tasksToSchedule) {
			if (task != null && task.allAssignedPeers().size() > 0) {
				nonStartedYet = false;
				break;
			}
		}
		return nonStartedYet;
	}
}