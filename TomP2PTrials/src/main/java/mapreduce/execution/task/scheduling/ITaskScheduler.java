package mapreduce.execution.task.scheduling;

import java.util.List;

import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.task.Task;

public interface ITaskScheduler {

	/**
	 * Schedules the next task to execute according to a defined scheduling algorithm
	 * 
	 * @param tasksToSchedule
	 * @return
	 */
	public Task schedule(List<Task> tasksToSchedule);

	/**
	 * Although this actually contains the task list, I thought it was easier to imagine schedule(tasks) than schedule(procedureInformation) and then
	 * pull the tasks from there using procedureInformation.tasks()... but it sure is the same... Needs procedureInformation to set it to finished in
	 * case it finishes while scheduling
	 * 
	 * @param procedureInformation
	 * @return
	 */
	public ITaskScheduler procedureInformation(ProcedureInformation procedureInformation);
}
