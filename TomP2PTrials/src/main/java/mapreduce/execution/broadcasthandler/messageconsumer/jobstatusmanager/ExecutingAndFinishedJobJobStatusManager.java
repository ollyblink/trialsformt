package mapreduce.execution.broadcasthandler.messageconsumer.jobstatusmanager;

public class ExecutingAndFinishedJobJobStatusManager extends AbstractJobStatusManager {

	public static ExecutingAndFinishedJobJobStatusManager newInstance() {
		return new ExecutingAndFinishedJobJobStatusManager();
	}

	private ExecutingAndFinishedJobJobStatusManager() {

	}

	@Override
	protected void manageMessages() {
		// TODO Auto-generated method stub

	}

}
