/**
 * 
 */
package com.swengle.phoebe.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Any method annotated with OnCreate will be called just after
 * to entity is created in the datastore
 * 
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface OnCreate {
}