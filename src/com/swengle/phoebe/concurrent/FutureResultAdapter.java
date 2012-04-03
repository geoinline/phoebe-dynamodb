package com.swengle.phoebe.concurrent;

import java.util.concurrent.Future;

import com.swengle.phoebe.concurrent.util.FutureHelper;

/**
 * Adapts a Future object to a (much more convenient) Result object.
 * 
 * @author Brian O'Connor <btoc008@gmail.com>
 */
public class FutureResultAdapter<T> implements FutureResult<T> {
	/** */
	Future<T> future;

	/** */
	public FutureResultAdapter(Future<T> fut) {
		this.future = fut;
	}

	@Override
	public T now() {
		try {
			return this.future.get();
		} catch (Exception e) {
			FutureHelper.unwrapAndThrow(e);
			return null; // make compiler happy
		}
	}
}
