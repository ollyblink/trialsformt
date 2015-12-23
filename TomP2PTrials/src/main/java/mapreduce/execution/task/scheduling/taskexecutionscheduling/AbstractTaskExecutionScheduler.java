package mapreduce.execution.task.scheduling.taskexecutionscheduling;

import java.util.List;

import mapreduce.execution.task.Task;
import mapreduce.execution.task.Tasks;
import mapreduce.execution.task.scheduling.ITaskScheduler;

public abstract class AbstractTaskExecutionScheduler implements ITaskScheduler {

	private boolean failedWhileWaiting = false;

	@Override
	public Task schedule(List<Task> tasksToSchedule) {
		if (tasksToSchedule != null) {
			return scheduleNonNull(tasksToSchedule);
		}
		return null;
	}

	public boolean failedWhileWaiting() {
		return failedWhileWaiting;
	}

	protected abstract Task scheduleNonNull(List<Task> tasksToSchedule);

	protected boolean allTasksAreFinished(List<Task> tasksToSchedule) {
		synchronized (tasksToSchedule) {
			for (Task task : tasksToSchedule) {
				if (!task.isFinished()) {
					return false;
				}
			}
		}
		return true;
	}

	protected boolean noTaskAssignedYet(List<Task> tasksToSchedule) {
		synchronized (tasksToSchedule) {
			for (Task task : tasksToSchedule) {
				if (task != null && Tasks.allAssignedPeers(task).size() > 0) {
					return false;
				}
			}
		}
		return true;
	}
}