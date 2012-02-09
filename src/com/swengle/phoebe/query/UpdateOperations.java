/**
 * 
 */
package com.swengle.phoebe.query;


/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public interface UpdateOperations<T> {
	/** adds the val to a set field **/
	UpdateOperations<T> add(String fieldName, Object val);
	/** adds the vals to a set field **/
	UpdateOperations<T> addAll(String fieldName, Iterable<?> vals);
	/** decrements the number field by 1 **/
	UpdateOperations<T> dec(String fieldName);
	/** increments the number field by 1 **/
	UpdateOperations<T> inc(String fieldName);
	/** increments the number field by value (negative values allowed) **/
	UpdateOperations<T> inc(String fieldName, Number value);
	/** removes val from the set field **/
	UpdateOperations<T> remove(String fieldName, Object val);
	/** removes vals from the set field **/
	UpdateOperations<T> removeAll(String fieldName, Iterable<?> vals);
	/** sets the field to val **/
	UpdateOperations<T> set(String fieldName, Object val);
	/** removes the field **/
	UpdateOperations<T> unset(String fieldName);

}
