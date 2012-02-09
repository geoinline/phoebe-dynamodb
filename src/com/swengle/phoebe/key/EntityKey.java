/**
 * 
 */
package com.swengle.phoebe.key;

import java.lang.reflect.Method;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.swengle.phoebe.reflect.DynamoDBReflector;
import com.swengle.phoebe.reflect.DynamoDBReflector.MarshallerType;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 * 
 */
public class EntityKey<T> implements Comparable<EntityKey<T>> {
	private DynamoDBTable dynamoDBTable;
	private Object hashKey;
	private Object rangeKey;
	private Class<T> kindClass;
	private Key dynamoDBKey;
	private boolean shouldHaveRangeKey;


	@SuppressWarnings("unchecked")
	public static <T> EntityKey<T> create(T keyOrEntity) {
		if (keyOrEntity instanceof EntityKey<?>) {
			return (EntityKey<T>)keyOrEntity;
		} else {
			Class<?> kindClass = (Class<?>) keyOrEntity.getClass();
			Method hashKeyGetter = DynamoDBReflector.INSTANCE
					.getHashKeyGetter(kindClass);
			Method rangeKeyGetter = DynamoDBReflector.INSTANCE
					.getRangeKeyGetter(kindClass);
			Object rangeKeyValue = null;
			if (rangeKeyGetter != null) {
				rangeKeyValue = DynamoDBReflector.INSTANCE.safeInvoke(
						rangeKeyGetter, keyOrEntity);
			}
			return (EntityKey<T>) EntityKey.create(kindClass, DynamoDBReflector.INSTANCE.safeInvoke(hashKeyGetter, keyOrEntity), rangeKeyValue);
		}
	}
	
	
	public static <T> EntityKey<T> create(Class<T> kindClass,
			Object hashKey) {
		return new EntityKey<T>(kindClass, hashKey);
	}

	public static <T> EntityKey<T> create(Class<T> kindClass,
			Object hashKey, Object rangeKey) {
		return new EntityKey<T>(kindClass, hashKey, rangeKey);
	}
	
	public static <T> EntityKey<T> create(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver, String hashKeyRangeKey) {
		String[] keys = hashKeyRangeKeyResolver.split(hashKeyRangeKey);
		if (keys.length != 1 || keys.length != 2) {
			throw new InvalidEntityKeyException("Given HashKeyRangeKeyResolver should return a string array of length 1 or 2");
		}
		if (keys.length == 1) {
			return new EntityKey<T>(kindClass, keys[0], null);
		} else {
			return new EntityKey<T>(kindClass, keys[0], keys[1]);
		}
	}
	

	private EntityKey(Class<T> kindClass, Object hashKey) {
		this(kindClass, hashKey, null);
	}

	private EntityKey(Class<T> kindClass, Object hashKey,
			Object rangeKey) {
		dynamoDBKey = new Key();
		setKindClass(kindClass);
		setHashKey(hashKey);
		setRangeKey(rangeKey);
	}

	/**
	 * @return the dynamoDBTable
	 */
	public DynamoDBTable getDynamoDBTable() {
		return dynamoDBTable;
	}

	/**
	 * @return the hashKey
	 */
	public Object getHashKey() {
		return hashKey;
	}

	/**
	 * @param hashKey
	 *            the hashKey to set
	 */
	private void setHashKey(Object hashKey) {
		if (hashKey == null) {
			throw new InvalidEntityKeyException("A hashKey is required.");
		}
		this.hashKey = hashKey;
		Method hashKeyGetter = DynamoDBReflector.INSTANCE.getHashKeyGetter(kindClass);
		MarshallerType marshallerType = DynamoDBReflector.INSTANCE.getMarshallerType(hashKeyGetter);
		if (marshallerType == MarshallerType.N) {
			dynamoDBKey.withHashKeyElement(new AttributeValue().withN(String.valueOf(hashKey)));
		} else {
			dynamoDBKey.withHashKeyElement(new AttributeValue().withS(String.valueOf(hashKey)));
		}
	}

	/**
	 * @return the rangeKey
	 */
	public Object getRangeKey() {
		return rangeKey;
	}

	/**
	 * @param rangeKey
	 *            the rangeKey to set
	 */
	private void setRangeKey(Object rangeKey) {
		if (shouldHaveRangeKey) {
			if (rangeKey == null) {
				throw new InvalidEntityKeyException("rangeKey is required for "
						+ kindClass);
			}
			this.rangeKey = rangeKey;
			
			Method rangeKeyGetter = DynamoDBReflector.INSTANCE.getRangeKeyGetter(kindClass);
			MarshallerType marshallerType = DynamoDBReflector.INSTANCE.getMarshallerType(rangeKeyGetter);
			if (marshallerType == MarshallerType.N) {
				dynamoDBKey.withRangeKeyElement(new AttributeValue().withN(String.valueOf(rangeKey)));
			} else {
				dynamoDBKey.withRangeKeyElement(new AttributeValue().withS(String.valueOf(rangeKey)));
			}
		} else if (rangeKey != null) {
			throw new InvalidEntityKeyException("No rangeKey is required for "
					+ kindClass);
		}
	}

	/**
	 * @return the kindClass
	 */
	public Class<? extends T> getKindClass() {
		return kindClass;
	}

	/**
	 * @param kindClass
	 *            the kindClass to set
	 */
	private void setKindClass(Class<T> kindClass) {
		this.kindClass = kindClass;
		this.dynamoDBTable = DynamoDBReflector.INSTANCE.getTable(kindClass);
		if (DynamoDBReflector.INSTANCE.getRangeKeyGetter(kindClass) != null) {
			this.shouldHaveRangeKey = true;
		} else {
			this.shouldHaveRangeKey = false;
		}
	}

	/**
	 * @return the dynamoDBKey
	 */
	public Key getDynamoDBKey() {
		return dynamoDBKey;
	}

	@Override
	public int compareTo(EntityKey<T> obj) {
		return new CompareToBuilder().append(this.kindClass, obj.kindClass)
				.append(this.hashKey, obj.hashKey)
				.append(this.rangeKey, obj.rangeKey).toComparison();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof EntityKey<?> == false) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		EntityKey<?> rhs = (EntityKey<?>) obj;
		return new EqualsBuilder().append(this.kindClass, rhs.kindClass)
				.append(this.hashKey, rhs.hashKey)
				.append(this.rangeKey, rhs.rangeKey).isEquals();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37).append(kindClass).append(hashKey)
				.append(rangeKey).toHashCode();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{table:" + this.dynamoDBTable.tableName() + ", key:");
		sb.append(this.dynamoDBKey.toString());
		sb.append("}");
		return sb.toString();
	}

}
