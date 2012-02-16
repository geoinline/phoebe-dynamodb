/**
 * 
 */
package com.swengle.phoebe.test.model;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;
import com.swengle.phoebe.annotation.DynamoDBTableInitialCapacities;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
@DynamoDBTable(tableName="EntityWithHashKeyAndRangeKey")
@DynamoDBTableInitialCapacities(readCapacityUnits = 128, writeCapacityUnits = 128)
public class EntityWithJustHashKeyAndJustRangeKey extends EntityWithJustHashKey {
	private String rangeKey;

	/**
	 * 
	 */
	public EntityWithJustHashKeyAndJustRangeKey() {
		// nothing to do
	}

	/**
	 * @return the rangeKey
	 */
	@DynamoDBRangeKey(attributeName="rangeKey")
	public String getRangeKey() {
		return rangeKey;
	}

	/**
	 * @param rangeKey the rangeKey to set
	 */
	public void setRangeKey(String rangeKey) {
		this.rangeKey = rangeKey;
	}

}
