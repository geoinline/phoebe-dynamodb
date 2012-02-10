/**
 * 
 */
package com.swengle.phoebe.test.model;

import java.util.Set;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBAttribute;

/**
 * @author Administrator
 *
 */
public class EntityWithHashKey extends EntityWithJustHashKey {
	private String string;
	private int number;
	private Set<String> set;

	/**
	 * 
	 */
	public EntityWithHashKey() {
		// nothing to do
	}

	/**
	 * @return the string
	 */
	@DynamoDBAttribute(attributeName="0")
	public String getString() {
		return string;
	}

	/**
	 * @param string the string to set
	 */
	public void setString(String string) {
		this.string = string;
	}

	/**
	 * @return the number
	 */
	@DynamoDBAttribute(attributeName="1")
	public int getNumber() {
		return number;
	}

	/**
	 * @param number the number to set
	 */
	public void setNumber(int number) {
		this.number = number;
	}

	/**
	 * @return the set
	 */
	@DynamoDBAttribute(attributeName="3")
	public Set<String> getSet() {
		return set;
	}

	/**
	 * @param set the set to set
	 */
	public void setSet(Set<String> set) {
		this.set = set;
	}

	

}
