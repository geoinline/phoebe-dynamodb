/**
 * 
 */
package com.swengle.phoebe.result;

import com.swengle.phoebe.key.EntityKey;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class DeleteResult<T> {
	private EntityKey<? extends T> entityKey;
	private Exception exception;
	
	/**
	 * Create a new DeleteResult object
	 */
	public DeleteResult(EntityKey<? extends T> entityKey) {
		this.entityKey = entityKey;
	}

	/**
	 * @return the exception
	 */
	public Exception getException() {
		return exception;
	}

	/**
	 * @param exception the exception to set
	 */
	public void setException(Exception exception) {
		this.exception = exception;
	}

	/**
	 * @return the entityKey
	 */
	public EntityKey<? extends T> getEntityKey() {
		return entityKey;
	}

	/**
	 * @param entityKey the entityKey to set
	 */
	public void setEntityKey(EntityKey<? extends T> entityKey) {
		this.entityKey = entityKey;
	}
	
	/**
	 * Whether an exception occurred
	 */
	public boolean hasException() {
		return exception != null;
	}

}
