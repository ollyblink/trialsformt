import mapreduce.execution.job.Job;
import mapreduce.execution.task.Tasks;

public class Taskupdate {
	//Make sure the transmitted job only contains procedure information for the tasks that need to be updated...
	//Don't forget the isFinished for all Job/Procedure/Task
	public void TaskUpdate(Job job, Task task, TaskResult result) {
		// Job here needs to only have tasks for the currently executed procedure used
		if (jobs.contains(job)) { // If it contains it, we may update it
			if (job.equals(currentlyExecutedJob)) { // We only want to update a currently executed job
				// Check the current procedure's actuality...
				if (currentlyExecutedJob.procedureIndex() == job.procedureIndex()) { // This is what we hope for... the received index is the same as
																						// the currently executed job
					List<Task> tasks = currentlyExecutedJob.tasks();
					synchronized (tasks) {
						if (tasks.contains(task)) { // The task was already once created or received... How cool
							Task taskToUpdate = tasks.get(tasks.indexOf(task));
							Tasks.updateStati(taskToUpdate, task, job.maxNrOfFinishedWorkersPerTask());
						} else { // This executor has not yet had that task which means it has not created it yet... simply add id to the tasks and it
									// can
									// be scheduled for execution
							tasks.add(task);
							// Task scheduler takes care of scheduling the task in case the execution stopped and the time to live did not yet run
							// out...
							// This is a treaky one... If we use a TimeToLive to decide if the Procedure is finished
							// Hmn...
							// Why not use a little average time for that? Always add received message time and use the max waiting time (Plus some
							// delta)
							// to wait in the scheduler...
						}
					}
				} else { // Abnormal case, something wrong because the executed procedure is not the same...
					if (currentlyExecutedJob.procedureIndex() < job.procedureIndex()) { // The current job is outdated
						handleReceivedJob(job); // Will abort it
					} else if (currentlyExecutedJob.procedureIndex() > job.procedureIndex()) { // The received job is outdated
						// Hmn, may be because either the message received is out of date, or there was a task assigned that was not yet assigned
						// before
						// the procedure ended...
						List<Task> tasks = currentlyExecutedJob.procedure(job.procedureIndex()).tasks(); // Or get it over DHT...
						synchronized (tasks) {
							if (!tasks.contains(task)) {
								// This is crap. That means the received task is from an older procedure and was not yet executed. So the currently
								// executed
								// procedure needs to be stopped, data removed, and the jobs current procedure index needs to be set back to the
								// received
								// one
								jobs.remove(job);
								handleReceivedJob(job);
							} else {
								// Nothing to worry, this is just an old message and thus can be discarded...
							}
						}
					} else {
						// Not possible as this case is covered by the previous if else statement (if(currentlyExecutedJob.procedureIndex() ==
						// job.procedureIndex()))
					}
				}
			} else { // Well another job is being executed... so simply treat it as new job received and let that method decide what to do with it
				handleReceivedJob(job);
			}
		} else { // New job received in case of recently joined executor
			handleReceivedJob(job);
		}
	}
}
