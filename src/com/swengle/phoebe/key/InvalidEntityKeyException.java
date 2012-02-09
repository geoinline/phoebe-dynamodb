/**
 * 
 */
package com.swengle.phoebe.key;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class InvalidEntityKeyException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3727707549185868296L;

	/**
	 * 
	 */
	public InvalidEntityKeyException() {
		// nothing to do
	}

	/**
	 * @param message
	 */
	public InvalidEntityKeyException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public InvalidEntityKeyException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public InvalidEntityKeyException(String message, Throwable cause) {
		super(message, cause);
	}

}
