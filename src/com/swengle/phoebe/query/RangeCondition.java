/**
 * 
 */
package com.swengle.phoebe.query;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public interface RangeCondition<T> {
	/** range key between val1 and val2 (inclusive it seems) **/
	T between(Object val1, Object val2);
	/** range key equal to val **/
	T equal(Object val);
	/** range key greater than val **/
	T greaterThan(Object val);
	/** range key greater than or equal to val **/
	T greaterThanOrEq(Object val);
	/** range key less than val **/
	T lessThan(Object val);
	/** range key less than or equal to val **/
	T lessThanOrEq(Object val);
	/** range key starting with prefix **/
	T startingWith(String prefix);

}
