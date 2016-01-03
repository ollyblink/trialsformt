package mapreduce.execution.task.scheduling.taskexecutionscheduling;

import java.util.List;

import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.Task2;
import mapreduce.execution.task.Tasks;
import mapreduce.execution.task.scheduling.ITaskScheduler;

public abstract class AbstractTaskExecutionScheduler implements ITaskScheduler {

	// private boolean failedWhileWaiting = false;
	protected Procedure procedureInformation;

	public AbstractTaskExecutionScheduler procedureInformation(Procedure procedureInformation) {
		this.procedureInformation = procedureInformation;
		return this;
	}

	@Override
	public Task2 schedule(List<Task2> tasksToSchedule) {
		if (tasksToSchedule != null) {
			// if (TimeToLive.INSTANCE.cancelOnTimeout(tasksToSchedule, EmptyListCondition.create())) {
			return scheduleNonNull(tasksToSchedule);
			// }
		}
		return null;
	}

	// public boolean failedWhileWaiting() {
	// return failedWhileWaiting;
	// }

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