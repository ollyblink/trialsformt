package mapreduce.execution.task.scheduling.taskexecutionscheduling;

import java.util.List;

import mapreduce.execution.task.Task;
import mapreduce.execution.task.Tasks;
import mapreduce.execution.task.scheduling.ITaskScheduler;
import mapreduce.utils.TimeToLive;

public abstract class AbstractTaskExecutionScheduler implements ITaskScheduler {

	protected long timeToSleep = 10;

	@Override
	public Task schedule(List<Task> tasksToSchedule, long timeToLive) {
		if (tasksToSchedule != null) {
			if (TimeToLive.INSTANCE.cancelOnTimeout(tasksToSchedule, timeToSleep, timeToLive)) {
				return scheduleNonNull(tasksToSchedule);
			} else {
				return null;
			}
		}
		return null;

	}

	protected abstract Task scheduleNonNull(List<Task> tasksToSchedule);

	protected boolean allTasksAreFinished(List<Task> tasksToSchedule) {
		for (Task task : tasksToSchedule) {
			if (!task.isFinished()) {
				return false;
			}
		}
		return true;
	}

	protected boolean noTaskAssignedYet(List<Task> tasksToSchedule) {
		for (Task task : tasksToSchedule) {
			if (task != null && Tasks.allAssignedPeers(task).size() > 0) {
				return false;
			}
		}
		return true;
	}
}