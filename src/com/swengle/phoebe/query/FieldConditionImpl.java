/**
 * 
 */
package com.swengle.phoebe.query;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.swengle.phoebe.reflect.DynamoDBReflector;
import com.swengle.phoebe.reflect.DynamoDBReflector.MarshallerType;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class FieldConditionImpl<T extends Query<?>> implements FieldCondition<T> {
	private T query;
	private Condition condition;
	private Method fieldMethod;
	private String attributeName;
	private MarshallerType marshallerType;
	
	/**
	 * 
	 */
	public FieldConditionImpl(T query, String fieldName) {
		this.query = query;
		fieldMethod = DynamoDBReflector.INSTANCE.findGetterMethodForFieldName(query.getKindClass(), fieldName);
		marshallerType = DynamoDBReflector.INSTANCE.getMarshallerType(fieldMethod);
		if (marshallerType == null) {
			throw new InvalidQueryException(fieldName + " needs to be a String, Number, String Set or Number Set.");
		}
		attributeName = DynamoDBReflector.INSTANCE.getAttributeName(fieldMethod);
	}
	
	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#between(java.lang.Object, java.lang.Object)
	 */
	@Override
	public T between(Object val1, Object val2) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.BETWEEN.toString());
		condition.withAttributeValueList(convertToAttributeValue(val1), convertToAttributeValue(val2));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#contains(java.lang.Object)
	 */
	@Override
	public T contains(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.CONTAINS.toString());
		condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
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

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#equal(java.lang.Object)
	 */
	@Override
	public T equal(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.EQ.toString());
		condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/**
	 * @return the attributeName
	 */
	public String getAttributeName() {
		return attributeName;
	}

	/**
	 * @return the condition
	 */
	public Condition getCondition() {
		return condition;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#greaterThan(java.lang.Object)
	 */
	@Override
	public T greaterThan(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.GT.toString());
		condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#greaterThanOrEq(java.lang.Object)
	 */
	@Override
	public T greaterThanOrEq(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.GE.toString());
		condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#in(java.lang.Iterable)
	 */
	@Override
	public T in(Iterable<? extends Object> vals) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.IN.toString());
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
    	for (Object val : vals) {
			attributeValueList.add(convertToAttributeValue(val));
		}
    	condition.setAttributeValueList(attributeValueList);
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#isNotNull()
	 */
	@Override
	public T isNotNull() {
		condition = new Condition().withComparisonOperator(ComparisonOperator.NOT_NULL.toString());
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#isNull()
	 */
	@Override
	public T isNull() {
		condition = new Condition().withComparisonOperator(ComparisonOperator.NULL.toString());
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#lessThan(java.lang.Object)
	 */
	@Override
	public T lessThan(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.LT.toString());
		condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#lessThanOrEq(java.lang.Object)
	 */
	@Override
	public T lessThanOrEq(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.LE.toString());
		condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#notContains(java.lang.Object)
	 */
	@Override
	public T notContains(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.NOT_CONTAINS.toString());
		condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#notEqual(java.lang.Object)
	 */
	@Override
	public T notEqual(Object val) {
		condition = new Condition().withComparisonOperator(ComparisonOperator.NE.toString());
		condition.withAttributeValueList(convertToAttributeValue(val));
		return query;
	}

	/**
	 * @param attributeName the attributeName to set
	 */
	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}

	/**
	 * @param condition the condition to set
	 */
	public void setCondition(Condition condition) {
		this.condition = condition;
	}
	
	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.FieldCondition#startingWith(java.lang.String)
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

}
