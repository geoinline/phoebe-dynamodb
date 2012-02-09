/**
 * 
 */
package com.swengle.phoebe.test.model;

import java.util.Set;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;

/**
 * @author Administrator
 *
 */
@DynamoDBTable(tableName="TestObjectWithHashKey")
public class ObjectWithHashKey {
	private String id;
	private String foo;
	private String bar;
	private Set<String> set;
	private Integer counter;

	/**
	 * 
	 */
	public ObjectWithHashKey() {
		// nothing to do
	}
	
	public ObjectWithHashKey(String id, String foo) {
		this.id = id;
		this.foo = foo;
	}

	/**
	 * @return the id
	 */
	@DynamoDBHashKey(attributeName="id1")
	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the foo
	 */
	@DynamoDBAttribute(attributeName="0")
	public String getFoo() {
		return foo;
	}

	/**
	 * @param foo the foo to set
	 */
	public void setFoo(String foo) {
		this.foo = foo;
	}

	/**
	 * @return the bar
	 */
	@DynamoDBAttribute(attributeName="1")
	public String getBar() {
		return bar;
	}

	/**
	 * @param bar the bar to set
	 */
	public void setBar(String bar) {
		this.bar = bar;
	}

	/**
	 * @return the set
	 */
	@DynamoDBAttribute(attributeName="2")
	public Set<String> getSet() {
		return set;
	}

	/**
	 * @param foobar the set to set
	 */
	public void setSet(Set<String> foobar) {
		this.set = foobar;
	}

	/**
	 * @return the counter
	 */
	@DynamoDBAttribute(attributeName="3")
	public Integer getCounter() {
		return counter;
	}

	/**
	 * @param counter the counter to set
	 */
	public void setCounter(Integer counter) {
		this.counter = counter;
	}

}
