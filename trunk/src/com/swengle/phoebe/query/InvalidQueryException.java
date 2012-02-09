/**
 * 
 */
package com.swengle.phoebe.query;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class InvalidQueryException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3891248474904965952L;

	/**
	 * 
	 */
	public InvalidQueryException() {
		// nothing to do
	}

	/**
	 * @param arg0
	 */
	public InvalidQueryException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public InvalidQueryException(Throwable arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public InvalidQueryException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
