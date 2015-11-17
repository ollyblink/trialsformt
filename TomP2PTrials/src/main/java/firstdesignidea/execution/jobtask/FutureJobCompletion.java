package firstdesignidea.execution.jobtask;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.Cancel;
import net.tomp2p.peers.PeerAddress;

public class FutureJobCompletion implements BaseFuture{

	@Override
	public void cancel() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BaseFuture await() throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean await(long timeoutMillis) throws InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public BaseFuture awaitUninterruptibly() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean awaitUninterruptibly(long timeoutMillis) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCompleted() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isSuccess() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFailed() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public BaseFuture failed(String reason) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BaseFuture failed(BaseFuture origin) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BaseFuture failed(String reason, BaseFuture origin) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BaseFuture failed(Throwable t) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BaseFuture failed(String reason, Throwable t) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String failedReason() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FutureType type() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BaseFuture awaitListeners() throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BaseFuture awaitListenersUninterruptibly() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BaseFuture addListener(BaseFutureListener<? extends BaseFuture> listener) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BaseFuture removeListener(BaseFutureListener<? extends BaseFuture> listener) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BaseFuture addCancel(Cancel cancel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BaseFuture removeCancel(Cancel cancel) {
		// TODO Auto-generated method stub
		return null;
	}

	public Map<Task, List<PeerAddress>> locations() {
		return new HashMap<Task, List<PeerAddress>>();
	}

}
