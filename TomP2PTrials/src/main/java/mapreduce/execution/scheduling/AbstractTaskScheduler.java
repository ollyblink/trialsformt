package mapreduce.execution.scheduling;

import java.util.List;

import mapreduce.execution.jobtask.Task;

public abstract class AbstractTaskScheduler implements ITaskScheduler {

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

	protected boolean NoTaskAssignedYet(List<Task> tasksToSchedule) {
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