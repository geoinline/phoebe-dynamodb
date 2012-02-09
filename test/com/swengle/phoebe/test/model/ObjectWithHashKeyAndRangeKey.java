/**
 * 
 */
package com.swengle.phoebe.test.model;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;

/**
 * @author Administrator
 *
 */
@DynamoDBTable(tableName="TestObjectWithHashKeyAndRangeKey")
public class ObjectWithHashKeyAndRangeKey extends ObjectWithHashKey {
	private String rangeId;

	/**
	 * 
	 */
	public ObjectWithHashKeyAndRangeKey() {
		// nothing to do
	}

	/**
	 * @return the rangeId
	 */
	@DynamoDBRangeKey(attributeName="id2")
	public String getRangeId() {
		return rangeId;
	}

	/**
	 * @param rangeId the rangeId to set
	 */
	public void setRangeId(String rangeId) {
		this.rangeId = rangeId;
	}

}
