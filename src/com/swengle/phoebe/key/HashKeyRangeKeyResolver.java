/**
 * 
 */
package com.swengle.phoebe.key;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public interface HashKeyRangeKeyResolver {
	/**
	 * Should return a string array of length 1 for just a hashKey, a length of 2 for a hashKey and rangeKey
	 * string[0] should be the string representation of the hashKey
	 * string[1] (if necessary) should be the string representation of the rangeKey
	 */
	String[] split(String hashKeyRangeKey);
	/**
	 * Combines the hashKey and rangeKey into a single string
	 * This string should be usable in the split() method
	 */
	String join(Object hashKey, Object rangeKey);
}
