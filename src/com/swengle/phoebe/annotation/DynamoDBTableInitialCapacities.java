/**
 * 
 */
package com.swengle.phoebe.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface DynamoDBTableInitialCapacities {
	long readCapacityUnits();
	long writeCapacityUnits();
}
