/**
 * 
 */
package com.swengle.phoebe.query;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodb.model.AttributeAction;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.swengle.phoebe.reflect.ArgumentMarshaller;
import com.swengle.phoebe.reflect.DynamoDBReflector;
import com.swengle.phoebe.reflect.DynamoDBReflector.MarshallerType;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 * 
 */
public class UpdateOperationsImpl<T> implements UpdateOperations<T> {
	private Class<T> kindClass;
	private Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();

	/**
	 * Create a new UpdateOperationsImpl object
	 */
	public UpdateOperationsImpl(Class<T> kindClass) {
		this.kindClass = kindClass;
	}
	
	
	
	
	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.UpdateOperations#add(java.lang.String, java.lang.Object)
	 */
	@Override
	public UpdateOperations<T> add(String fieldName, Object val) {
		return addAll(fieldName,  new LinkedHashSet<Object>(Arrays.asList(val)));
	}




	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.UpdateOperations#addAll(java.lang.String, java.util.Set)
	 */
	@Override
	public UpdateOperations<T> addAll(String fieldName, Iterable<?> vals) {
		MarshallerType marshallerType = getArgumentMarshallerTypeForFieldName(fieldName);
		if (marshallerType != MarshallerType.SS && marshallerType != MarshallerType.NS) {
			throw new InvalidQueryException(fieldName + " needs to be a string set or number set.");
		}
		AttributeValueUpdate attributeValueUpdate = new AttributeValueUpdate();
		attributeValueUpdate.withAction(AttributeAction.ADD);
		AttributeValue attributeValue = new AttributeValue();
		Set<String> valsAsStrings = new HashSet<String>();
		for (Object val : vals) {
			valsAsStrings.add(String.valueOf(val));
		}
		if (marshallerType == MarshallerType.SS) {
			attributeValue.setSS(valsAsStrings);
		} else {
			attributeValue.setNS(valsAsStrings);
		}
		attributeValueUpdate.withValue(attributeValue);
		updateItems.put(getAttributeName(fieldName), attributeValueUpdate);
		return this;
	}




	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.UpdateOperations#dec(java.lang.String)
	 */
	@Override
	public UpdateOperations<T> dec(String fieldName) {
		return inc(fieldName, -1);
	}




	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.UpdateOperations#inc(java.lang.String)
	 */
	@Override
	public UpdateOperations<T> inc(String fieldName) {
		return inc(fieldName, 1);
	}




	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.UpdateOperations#inc(java.lang.String, java.lang.Number)
	 */
	@Override
	public UpdateOperations<T> inc(String fieldName, Number value) {
		MarshallerType marshallerType = getArgumentMarshallerTypeForFieldName(fieldName);
		if (marshallerType != MarshallerType.N) {
			throw new InvalidQueryException(fieldName + " needs to be a number.");
		}
		AttributeValueUpdate attributeValueUpdate = new AttributeValueUpdate();
		attributeValueUpdate.withAction(AttributeAction.ADD);
		attributeValueUpdate.setValue(new AttributeValue().withN(String.valueOf(value)));
		updateItems.put(getAttributeName(fieldName), attributeValueUpdate);
		return this;
	}




	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.UpdateOperations#remove(java.lang.String, java.lang.Object)
	 */
	@Override
	public UpdateOperations<T> remove(String fieldName, Object val) {
		return removeAll(fieldName, new LinkedHashSet<Object>(Arrays.asList(val)));
	}




	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.UpdateOperations#removeAll(java.lang.String, java.util.Set)
	 */
	@Override
	public UpdateOperations<T> removeAll(String fieldName, Iterable<?> vals) {
		MarshallerType marshallerType = getArgumentMarshallerTypeForFieldName(fieldName);
		if (marshallerType != MarshallerType.SS && marshallerType != MarshallerType.NS) {
			throw new InvalidQueryException(fieldName + " needs to be a string set or number set.");
		}
		AttributeValueUpdate attributeValueUpdate = new AttributeValueUpdate();
		attributeValueUpdate.withAction(AttributeAction.DELETE);
		AttributeValue attributeValue = new AttributeValue();
		Set<String> valsAsStrings = new HashSet<String>();
		for (Object val : vals) {
			valsAsStrings.add(String.valueOf(val));
		}
		if (marshallerType == MarshallerType.SS) {
			attributeValue.setSS(valsAsStrings);
		} else {
			attributeValue.setNS(valsAsStrings);
		}
		attributeValueUpdate.withValue(attributeValue);
		updateItems.put(getAttributeName(fieldName), attributeValueUpdate);
		return this;
	}




	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.UpdateOperations#set(java.lang.String, java.lang.Object)
	 */
	@Override
	public UpdateOperations<T> set(String fieldName, Object val) {
		ArgumentMarshaller argumentMarshaller = getArgumentMarshallerForFieldName(fieldName);
		AttributeValueUpdate attributeValueUpdate = new AttributeValueUpdate();
		attributeValueUpdate.withAction(AttributeAction.PUT);
		attributeValueUpdate.setValue(argumentMarshaller.marshall(val));
		updateItems.put(getAttributeName(fieldName), attributeValueUpdate);
		return this;
	}




	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.UpdateOperations#unset(java.lang.String)
	 */
	@Override
	public UpdateOperations<T> unset(String fieldName) {
		AttributeValueUpdate attributeValueUpdate = new AttributeValueUpdate();
		attributeValueUpdate.withAction(AttributeAction.DELETE);
		updateItems.put(getAttributeName(fieldName), attributeValueUpdate);
		return this;
	}




	/**
	 * 
	 * Return the attribute name for a field name
	 */
	private String getAttributeName(String fieldName) {
		String attributeName = DynamoDBReflector.INSTANCE.fieldToAttributeName(kindClass, fieldName);
		if (attributeName == null) {
			throw new IllegalArgumentException("Unknown field: " + fieldName);
		}
		return attributeName;
	}
	
	/**
	 * Find the getter method for a field name
	 */
	private Method findGetterMethodForFieldName(String fieldName) {
		return DynamoDBReflector.INSTANCE.findGetterMethodForFieldName(kindClass, fieldName);
	}
	
	/**
	 * Determine the MarshallerType(S,N,SS,NS) for a field name
	 */
	public MarshallerType getArgumentMarshallerTypeForFieldName(String fieldName) {
		return DynamoDBReflector.INSTANCE.getMarshallerType(findGetterMethodForFieldName(fieldName));
	}
	
	/**
     * Returns a marshaller that knows how to provide an AttributeValue for the
     * result of the getter field name.
     */
	public ArgumentMarshaller getArgumentMarshallerForFieldName(String fieldName) {
		return DynamoDBReflector.INSTANCE.getArgumentMarshaller(findGetterMethodForFieldName(fieldName));
	}




	/**
	 * @return the kindClass
	 */
	public Class<T> getKindClass() {
		return kindClass;
	}



	/**
	 * @return the updateItems
	 */
	public Map<String, AttributeValueUpdate> getUpdateItems() {
		return updateItems;
	}



}
