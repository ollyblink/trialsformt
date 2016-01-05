package mapreduce.execution.task.scheduling.taskexecutionscheduling;

import java.util.List;

import mapreduce.execution.task.Task2;
import mapreduce.execution.task.scheduling.ITaskScheduler;

public abstract class AbstractTaskExecutionScheduler implements ITaskScheduler {

	@Override
	public Task2 schedule(List<Task2> tasksToSchedule) {
		if (tasksToSchedule != null) {
			return scheduleNonNull(tasksToSchedule);
		}
		return null;
	}

	protected abstract Task2 scheduleNonNull(List<Task2> tasksToSchedule);

	protected boolean allTasksAreFinished(List<Task2> tasksToSchedule) {
		synchronized (tasksToSchedule) {
			for (Task2 task : tasksToSchedule) {
				if (!task.isFinished()) {
					return false;
				}
			}
		}
		return true;
	}

	protected boolean noTaskAssignedYet(List<Task2> tasksToSchedule) {
		synchronized (tasksToSchedule) {
			for (Task2 task : tasksToSchedule) {
				if (task != null && task.outputDomains().size() > 0) {
					return false;
				}
			}
		}
		return true;
	}
}