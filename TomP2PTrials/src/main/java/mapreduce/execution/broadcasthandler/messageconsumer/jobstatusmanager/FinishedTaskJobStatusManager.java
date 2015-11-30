package mapreduce.execution.broadcasthandler.messageconsumer.jobstatusmanager;

public class FinishedTaskJobStatusManager extends AbstractJobStatusManager {

	public static FinishedTaskJobStatusManager newInstance() {
		return new FinishedTaskJobStatusManager();
	}

	private FinishedTaskJobStatusManager() {

	}

	@Override
	protected void manageMessages() {
		// TODO Auto-generated method stub

	}

}