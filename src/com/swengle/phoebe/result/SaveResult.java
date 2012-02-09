/**
 * 
 */
package com.swengle.phoebe.result;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class SaveResult<T> {
	private T object;
	private Exception exception;

	/**
	 * 
	 */
	public SaveResult(T object) {
		this.object = object;
	}

	/**
	 * @return the object
	 */
	public T getObject() {
		return object;
	}

	/**
	 * @param object the object to set
	 */
	public void setObject(T object) {
		this.object = object;
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
	 * Whether an exception occurred
	 */
	public boolean hasException() {
		return exception != null;
	}

}
