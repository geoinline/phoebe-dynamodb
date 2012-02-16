/**
 * 
 */
package com.swengle.phoebe.test.model;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;
import com.swengle.phoebe.annotation.DynamoDBTableInitialCapacities;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
@DynamoDBTable(tableName="EntityWithHashKey")
@DynamoDBTableInitialCapacities(readCapacityUnits = 128, writeCapacityUnits = 128)
public class EntityWithJustHashKey {
	private String hashKey;

	/**
	 * 
	 */
	public EntityWithJustHashKey() {
		// nothing to do
	}


	/**
	 * @return the hashKey
	 */
	@DynamoDBHashKey(attributeName="hashKey")
	public String getHashKey() {
		return hashKey;
	}

	/**
	 * @param hashKey the hashKey to set
	 */
	public void setHashKey(String hashKey) {
		this.hashKey = hashKey;
	}

}
