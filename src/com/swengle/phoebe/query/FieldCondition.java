/**
 * 
 */
package com.swengle.phoebe.query;


/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public interface FieldCondition<T> {
	/** field between val1 and val2 (inclusive it seems) **/
	T between(Object val1, Object val2);
	/** if field is a string, then we do a substring... if it's a set we check if its in the set **/
	T contains(Object val);
	/** field equal to val **/
	T equal(Object val);
	/** field greater than val **/
	T greaterThan(Object val);
	/** field greater than or equal to val **/
	T greaterThanOrEq(Object val);
	/** field is in one of supplied vals **/
	T in(Iterable<? extends Object> vals);
	/** field is not null **/
	T isNotNull();
	/** field is null **/
	T isNull();
	/** field is less than val **/
	T lessThan(Object val);
	/** field is less than or equal val **/
	T lessThanOrEq(Object val);
	/** if field is a string, then we check substring does not exist... if it's a set we check if its not in the set **/
	T notContains(Object val);
	/** field is not equal to val **/
	T notEqual(Object val);
	/** field starts with prefix **/
	T startingWith(String prefix);




}
