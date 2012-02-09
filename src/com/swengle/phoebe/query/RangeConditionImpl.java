/**
 * 
 */
package com.swengle.phoebe.query;

import java.lang.reflect.Method;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.swengle.phoebe.reflect.DynamoDBReflector;
import com.swengle.phoebe.reflect.DynamoDBReflector.MarshallerType;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class RangeConditionImpl<T extends Query<?>> implements RangeCondition<T> {
	private T query;
	private Condition condition;
	private MarshallerType marshallerType;
	
	/**
	 * Create a new RangeConditionImpl object
	 */
	public RangeConditionImpl(T query) {
		this.query = query;
		Method rangeKeyGetter = DynamoDBReflector.INSTANCE.getRangeKeyGetter(query.getKindClass());
		marshallerType = DynamoDBReflector.INSTANCE.getMarshallerType(rangeKeyGetter);
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.RangeCondition#between(java.lang.Object, java.lang.Object)
	 */
	@Override
	public T between(Object val1, Object val2) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.BETWEEN.toString());
		condition.withAttributeValueList(convertToAttributeValue(val1), convertToAttributeValue(val2));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.RangeCondition#equal(java.lang.Object)
	 */
	@Override
	public T equal(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.EQ.toString());
		condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.RangeCondition#greaterThan(java.lang.Object)
	 */
	@Override
	public T greaterThan(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.GT.toString());
    	condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.RangeCondition#greaterThanOrEq(java.lang.Object)
	 */
	@Override
	public T greaterThanOrEq(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.GE.toString());
    	condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.RangeCondition#lessThan(java.lang.Object)
	 */
	@Override
	public T lessThan(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.LT.toString());
    	condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.RangeCondition#lessThanOrEq(java.lang.Object)
	 */
	@Override
	public T lessThanOrEq(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.LE.toString());
    	condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.RangeCondition#startingWith(java.lang.String)
	 */
	@Override
	public T startingWith(String prefix) {
		if (marshallerType != MarshallerType.S) {
			throw new InvalidQueryException("Range key must be a string for the startingWith operation.");
		}
		condition = new Condition().withComparisonOperator(ComparisonOperator.BEGINS_WITH.toString());
    	condition.withAttributeValueList(new AttributeValue().withS(prefix));
		return query;
	}

	/**
	 * @return the condition
	 */
	public Condition getCondition() {
		return condition;
	}

	private AttributeValue convertToAttributeValue(Object val) {
		AttributeValue attributeValue = new AttributeValue();
		if (marshallerType == MarshallerType.S || marshallerType == MarshallerType.SS) {
			return attributeValue.withS(String.valueOf(val));
		} else if (marshallerType == MarshallerType.N || marshallerType == MarshallerType.NS) {
			return attributeValue.withN(String.valueOf(val));
		}
		return attributeValue;
	}

}
