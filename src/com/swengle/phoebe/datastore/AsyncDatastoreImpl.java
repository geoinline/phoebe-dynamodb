/**
 * 
 */
package com.swengle.phoebe.datastore;

import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.ObjectId;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMappingException;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodb.model.BatchGetItemResult;
import com.amazonaws.services.dynamodb.model.BatchResponse;
import com.amazonaws.services.dynamodb.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.DeleteTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.KeysAndAttributes;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.ReturnValue;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;
import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.annotation.DynamoDBTableInitialCapacities;
import com.swengle.phoebe.concurrent.FutureResult;
import com.swengle.phoebe.concurrent.FutureResultAdapter;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.key.HashKeyRangeKeyResolver;
import com.swengle.phoebe.query.Query;
import com.swengle.phoebe.query.UpdateOperations;
import com.swengle.phoebe.query.UpdateOperationsImpl;
import com.swengle.phoebe.reflect.ArgumentUnmarshaller;
import com.swengle.phoebe.reflect.DynamoDBReflector;
import com.swengle.phoebe.result.DeleteResult;
import com.swengle.phoebe.result.SaveResult;
import com.swengle.phoebe.result.UpdateResult;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 * 
 */
public class AsyncDatastoreImpl implements AsyncDatastore {
	private static final Log LOG = LogFactory.getLog(AsyncDatastoreImpl.class);

	private static final class ValueUpdate {
		private Method method;
		private AttributeValue newValue;

		public ValueUpdate(Method method, AttributeValue newValue) {
			this.method = method;
			this.newValue = newValue;
		}
	}

	private Phoebe phoebe;

	private boolean consistentRead;
	private boolean lifecycleEnabled;

	
	/**
	 * Create a new AsyncDatastoreImpl object
	 */
	public AsyncDatastoreImpl(Phoebe phoebe, boolean consistentRead, boolean lifecycleEnabled) {
		this.phoebe = phoebe;
		this.consistentRead = consistentRead;
		this.lifecycleEnabled = lifecycleEnabled;
	}

	/**
	 * Converts the {@link AttributeValueUpdate} map given to an equivalent
	 * {@link AttributeValue} map.
	 */
	private Map<String, AttributeValue> convertToItem(
			Map<String, AttributeValueUpdate> putValues) {
		Map<String, AttributeValue> map = new HashMap<String, AttributeValue>();
		for (Entry<String, AttributeValueUpdate> entry : putValues.entrySet()) {
			/*
			 * AttributeValueUpdate allows nulls for its values, since they are
			 * semantically meaningful. AttributeValues never have null values.
			 */
			if (entry.getValue().getValue() != null)
				map.put(entry.getKey(), entry.getValue().getValue());
		}
		return map;
	}

	private <T extends Object> void create(T entity, boolean insert) {
		Map<String, AttributeValueUpdate> updateValues = new HashMap<String, AttributeValueUpdate>();
		Map<String, AttributeValueUpdate> keyUpdateValues = new HashMap<String, AttributeValueUpdate>();
		Map<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue>();
		List<ValueUpdate> inMemoryUpdates = new LinkedList<ValueUpdate>();

		Method hashKeyGetter = DynamoDBReflector.INSTANCE
				.getHashKeyGetter(entity.getClass());
		AttributeValue hashKeyElement = null;

		Method rangeKeyGetter = DynamoDBReflector.INSTANCE
				.getRangeKeyGetter(entity.getClass());
		AttributeValue rangeKeyElement = null;

		String tableName = DynamoDBReflector.INSTANCE.getTable(
				entity.getClass()).tableName();

		for (Method method : DynamoDBReflector.INSTANCE
				.getRelevantGetters(entity.getClass())) {

			Object getterResult = DynamoDBReflector.INSTANCE.safeInvoke(method,
					entity);
			String attributeName = DynamoDBReflector.INSTANCE
					.getAttributeName(method);

			if (method.equals(hashKeyGetter)) {
				if (getterResult == null
						&& !DynamoDBReflector.INSTANCE
								.isAssignableKey(hashKeyGetter)) {
					throw new IllegalArgumentException(
							"hashKey needs to be set or use @DynamoDBAutoGeneratedKey");
				} else if (getterResult == null) {
					insert = true;
					AttributeValue newVersionValue = getAutoGeneratedKeyAttributeValue(hashKeyGetter);
					updateValues.put(attributeName, new AttributeValueUpdate()
							.withAction("PUT").withValue(newVersionValue));
					inMemoryUpdates.add(new ValueUpdate(hashKeyGetter,
							newVersionValue));
				} else if (insert) {
					updateValues.put(
							attributeName,
							new AttributeValueUpdate().withValue(
									DynamoDBReflector.INSTANCE
											.getSimpleAttributeValue(method,
													getterResult)).withAction(
									"PUT"));
				} else {
					keyUpdateValues.put(
							attributeName,
							new AttributeValueUpdate().withValue(
									DynamoDBReflector.INSTANCE
											.getSimpleAttributeValue(method,
													getterResult)).withAction(
									"PUT"));
				}
				hashKeyElement = DynamoDBReflector.INSTANCE.getHashKeyElement(
						getterResult, hashKeyGetter);
			} else if (method.equals(rangeKeyGetter)) {
				if (getterResult == null
						&& !DynamoDBReflector.INSTANCE
								.isAssignableKey(rangeKeyGetter)) {
					throw new IllegalArgumentException(
							"rangeKey needs to be set or use @DynamoDBAutoGeneratedKey");
				} else if (getterResult == null) {
					insert = true;
					AttributeValue newVersionValue = getAutoGeneratedKeyAttributeValue(rangeKeyGetter);
					updateValues.put(attributeName, new AttributeValueUpdate()
							.withAction("PUT").withValue(newVersionValue));
					inMemoryUpdates.add(new ValueUpdate(rangeKeyGetter,
							newVersionValue));
				} else if (insert) {
					updateValues.put(
							attributeName,
							new AttributeValueUpdate().withValue(
									DynamoDBReflector.INSTANCE
											.getSimpleAttributeValue(method,
													getterResult)).withAction(
									"PUT"));
				} else {
					keyUpdateValues.put(
							attributeName,
							new AttributeValueUpdate().withValue(
									DynamoDBReflector.INSTANCE
											.getSimpleAttributeValue(method,
													getterResult)).withAction(
									"PUT"));
				}
				rangeKeyElement = DynamoDBReflector.INSTANCE
						.getRangeKeyElement(getterResult, rangeKeyGetter);
			} else if (DynamoDBReflector.INSTANCE
					.isVersionAttributeGetter(method)) {
				if (!insert) {
					// not an insert we need to comply with version
					ExpectedAttributeValue expected = new ExpectedAttributeValue();
					AttributeValue currentValue = DynamoDBReflector.INSTANCE
							.getSimpleAttributeValue(method, getterResult);
					expected.setExists(currentValue != null);
					if (currentValue != null) {
						expected.setValue(currentValue);
					}
					expectedValues.put(attributeName, expected);
				}

				AttributeValue newVersionValue = DynamoDBReflector.INSTANCE
						.getVersionAttributeValue(method, getterResult);
				updateValues.put(attributeName, new AttributeValueUpdate()
						.withAction("PUT").withValue(newVersionValue));
				inMemoryUpdates.add(new ValueUpdate(method, newVersionValue));
			} else {
				AttributeValue currentValue = DynamoDBReflector.INSTANCE
						.getSimpleAttributeValue(method, getterResult);
				if (currentValue != null) {
					updateValues.put(attributeName, new AttributeValueUpdate()
							.withValue(currentValue).withAction("PUT"));
				} else if (!insert) {
					updateValues.put(attributeName,
							new AttributeValueUpdate().withAction("DELETE"));
				}
			}
		}

		Key objectKey = new Key().withHashKeyElement(hashKeyElement)
				.withRangeKeyElement(rangeKeyElement);

		boolean runOnCreateMethods = false;
		boolean runOnUpdateMethods = false;


		// check if updateValues is empty to work around the
		// odd case where DynamoDB does not create an entity
		// on update when there are just key attributes
		if (insert || updateValues.isEmpty()) {
			expectedValues.put(
					DynamoDBReflector.INSTANCE.getAttributeName(hashKeyGetter),
					new ExpectedAttributeValue().withExists(false));
			if (!insert) {
				updateValues = keyUpdateValues;
			}
			try {
				PutItemRequest putItemRequest = new PutItemRequest()
						.withTableName(tableName)
						.withItem(convertToItem(updateValues))
						.withExpected(expectedValues);

				if (LOG.isDebugEnabled()) {
					LOG.debug("PutItemRequest: " + putItemRequest);
				}
				
				LOG.info("PutItemRequest: " + putItemRequest);

				phoebe.getClient().putItem(putItemRequest);
				if (lifecycleEnabled) {
					runOnCreateMethods = true;
				}
			} catch (ConditionalCheckFailedException e) {
				if (insert) {
					throw new DuplicateEntityException(
							"Entity already exists with key: "
									+ objectKey.toString());
				}
			}
		} else {
			UpdateItemRequest updateItemRequest = new UpdateItemRequest()
					.withTableName(tableName).withKey(objectKey)
					.withAttributeUpdates(updateValues)
					.withExpected(expectedValues);

			if (LOG.isDebugEnabled()) {
				LOG.debug("UpdateItemRequest: " + updateItemRequest);
			}

			updateItemRequest.setReturnValues(ReturnValue.UPDATED_OLD);

			UpdateItemResult updateItemResult = phoebe.getClient().updateItem(
					updateItemRequest);
			if (lifecycleEnabled) {
				if (updateItemResult.getAttributes() != null) {
					runOnUpdateMethods = true;
				} else {
					runOnCreateMethods = true;
				}
			}
		}

		/*
		 * Finally, after the service call has succeeded, update the in-memory
		 * object with new field values as appropriate.
		 */
		for (ValueUpdate update : inMemoryUpdates) {
			setValue(entity, update.method, update.newValue);
		}

		if (runOnCreateMethods) {
			Collection<Method> onCreateMethods = DynamoDBReflector.INSTANCE
					.getOnCreateMethods(entity.getClass());
			for (Method onCreateMethod : onCreateMethods) {
				DynamoDBReflector.INSTANCE.safeInvoke(onCreateMethod, entity);
			}

		} else if (runOnUpdateMethods) {
			Collection<Method> onUpdateMethods = DynamoDBReflector.INSTANCE
						.getOnUpdateMethods(entity.getClass());
			for (Method onUpdateMethod : onUpdateMethods) {
				DynamoDBReflector.INSTANCE.safeInvoke(onUpdateMethod, entity);
			}
		}

	}
	

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.swengle.phoebe.datastore.AsyncDatastore#createTable(java.lang.Class,
	 * long, long)
	 */
	@Override
	public <T> FutureResult<Void> createTable(final Class<T> kindClass,
			final long readCapacityUnits, final long writeCapacityUnits)
			throws DuplicateTableException {
		return new FutureResultAdapter<Void>(phoebe.getExecutorService()
				.submit(new Callable<Void>() {
					public Void call() throws Exception {
						long readCapacityUnitsToSet = 0;
						long writeCapacityUnitsToSet = 0;
						DynamoDBTableInitialCapacities initialCapacities = DynamoDBReflector.INSTANCE.getTableInitialCapacities(kindClass);
						if (initialCapacities != null) {
							readCapacityUnitsToSet = initialCapacities.readCapacityUnits();
							writeCapacityUnitsToSet = initialCapacities.writeCapacityUnits();
						} else {
							readCapacityUnitsToSet = readCapacityUnits;
							writeCapacityUnitsToSet = writeCapacityUnits;
						}
						if (readCapacityUnitsToSet < 1) {
							throw new IllegalStateException("readCapacityUnits must be greater than 0");
						}
						if (writeCapacityUnitsToSet < 1) {
							throw new IllegalStateException("writeCapacityUnits must be greater than 0");
						}
						
						String tableName = DynamoDBReflector.INSTANCE.getTable(
								kindClass).tableName();
						Method hashKeyGetter = DynamoDBReflector.INSTANCE
								.getHashKeyGetter(kindClass);
						String hashKeyAttributeName = DynamoDBReflector.INSTANCE
								.getHashKeyAttributeName(kindClass);

						KeySchemaElement hashKey = new KeySchemaElement()
								.withAttributeName(hashKeyAttributeName)
								.withAttributeType(getKeyType(hashKeyGetter));
						KeySchema ks = new KeySchema()
								.withHashKeyElement(hashKey);

						Method rangeKeyGetter = DynamoDBReflector.INSTANCE
								.getRangeKeyGetter(kindClass);
						if (rangeKeyGetter != null) {
							String rangeKeyAttributeName = DynamoDBReflector.INSTANCE
									.getRangeKeyAttributeName(kindClass);
							KeySchemaElement rangeKey = new KeySchemaElement()
									.withAttributeName(rangeKeyAttributeName)
									.withAttributeType(
											getKeyType(rangeKeyGetter));
							ks.setRangeKeyElement(rangeKey);
						}

						ProvisionedThroughput provisionedthroughput = new ProvisionedThroughput()
								.withReadCapacityUnits(readCapacityUnitsToSet)
								.withWriteCapacityUnits(writeCapacityUnitsToSet);

						CreateTableRequest request = new CreateTableRequest()
								.withTableName(tableName)
								.withKeySchema(ks)
								.withProvisionedThroughput(
										provisionedthroughput);

						if (LOG.isDebugEnabled()) {
							LOG.debug("CreateTableRequest: " + request);
						}

						try {
							phoebe.getClient().createTable(request);
							waitForTableToBecomeAvailable(tableName);
						} catch (AmazonServiceException ase) {
							if (ase.getMessage()
									.indexOf("Duplicate table name") == -1) {
								throw (ase);
							} else {
								throw new DuplicateTableException(tableName
										+ " already exists for: " + kindClass);
							}
						}
						return null;
					}
				}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.swengle.phoebe.datastore.AsyncDatastore#delete(java.lang.Class,
	 * com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.Iterable)
	 */
	@Override
	public <T> FutureResult<Iterable<DeleteResult<T>>> delete(
			Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			Iterable<String> hashKeyRangeKeys) {
		List<EntityKey<T>> entityKeyList = new ArrayList<EntityKey<T>>();
		for (String hashKeyRangeKey : hashKeyRangeKeys) {
			entityKeyList.add(EntityKey.create(kindClass,
					hashKeyRangeKeyResolver, hashKeyRangeKey));
		}
		return delete(entityKeyList);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.swengle.phoebe.datastore.AsyncDatastore#delete(java.lang.Class,
	 * com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.String)
	 */
	@Override
	public <T> FutureResult<Void> delete(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			String hashKeyRangeKey) {
		return delete(EntityKey.create(kindClass, hashKeyRangeKeyResolver,
				hashKeyRangeKey));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.swengle.phoebe.datastore.AsyncDatastore#delete(java.lang.Class,
	 * com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.String[])
	 */
	@Override
	public <T> FutureResult<Iterable<DeleteResult<T>>> delete(
			Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			String... hashKeyRangeKeys) {
		return delete(kindClass, hashKeyRangeKeyResolver,
				Arrays.asList(hashKeyRangeKeys));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#delete(java.lang.Class,
	 * java.lang.Iterable)
	 */
	@Override
	public <T> FutureResult<Iterable<DeleteResult<T>>> delete(
			Class<T> kindClass, Iterable<? extends Object> hashKeys) {
		List<EntityKey<T>> entityKeyList = new ArrayList<EntityKey<T>>();
		for (Object hashKey : hashKeys) {
			entityKeyList.add(EntityKey.create(kindClass, hashKey));
		}
		return delete(entityKeyList);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#delete(java.lang.Class,
	 * java.lang.Object)
	 */
	@Override
	public <T> FutureResult<Void> delete(Class<T> kindClass, Object hashKey) {
		return delete(kindClass, hashKey, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#delete(java.lang.Class,
	 * java.lang.Object[])
	 */
	@Override
	public <T> FutureResult<Iterable<DeleteResult<T>>> delete(
			Class<T> kindClass, Object... hashKeys) {
		return delete(kindClass, Arrays.asList(hashKeys));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#delete(java.lang.Class,
	 * java.lang.Object, java.lang.Object)
	 */
	@Override
	public <T> FutureResult<Void> delete(Class<T> kindClass, Object hashKey,
			Object rangeKey) {
		return delete(EntityKey.create(kindClass, hashKey, rangeKey));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.phoebe.dynamodb.datastore.AsyncDatastore#delete(com.phoebe.dynamodb
	 * .datastore.key.EntityKey)
	 */
	@Override
	public <T> FutureResult<Void> delete(final EntityKey<T> entityKey) {
		return new FutureResultAdapter<Void>(phoebe.getExecutorService()
				.submit(new Callable<Void>() {
					public Void call() throws Exception {
						DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
								.withKey(entityKey.getDynamoDBKey())
								.withTableName(
										entityKey.getDynamoDBTable()
												.tableName());

						if (LOG.isDebugEnabled()) {
							LOG.debug("DeleteItemRequest: " + deleteItemRequest);
						}

						Collection<Method> onDeleteMethods = null;
						if (lifecycleEnabled) {
							onDeleteMethods = DynamoDBReflector.INSTANCE
									.getOnDeleteMethods(entityKey.getKindClass());
							if (onDeleteMethods.size() > 0) {
								deleteItemRequest
										.setReturnValues(ReturnValue.ALL_OLD);
							}
						}
						DeleteItemResult deleteItemResult = phoebe.getClient()
								.deleteItem(deleteItemRequest);
						if (onDeleteMethods != null && onDeleteMethods.size() > 0 && deleteItemResult.getAttributes() != null) {
							T entity = phoebe.marshallIntoObject(
									entityKey.getKindClass(),
									deleteItemResult.getAttributes());
							for (Method onDeleteMethod : onDeleteMethods) {
								DynamoDBReflector.INSTANCE.safeInvoke(
										onDeleteMethod, entity);
							}

						}
						return null;
					}
				}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.phoebe.dynamodb.datastore.AsyncDatastore#delete(java.lang.Iterable)
	 */
	@Override
	public <T> FutureResult<Iterable<DeleteResult<T>>> delete(
			final Iterable<? extends EntityKey<? extends T>> entityKeys) {
		return new FutureResultAdapter<Iterable<DeleteResult<T>>>(phoebe
				.getExecutorService().submit(
						new Callable<Iterable<DeleteResult<T>>>() {
							@Override
							public Iterable<DeleteResult<T>> call()
									throws Exception {
								Map<EntityKey<? extends T>, FutureResult<Void>> futureResultMap = new HashMap<EntityKey<? extends T>, FutureResult<Void>>();
								for (EntityKey<? extends T> entityKey : entityKeys) {
									futureResultMap.put(entityKey,
											delete(entityKey));
								}
								List<DeleteResult<T>> result = new ArrayList<DeleteResult<T>>();
								// wait for all tasks to complete before
								// continuing
								for (Entry<EntityKey<? extends T>, FutureResult<Void>> entry : futureResultMap
										.entrySet()) {
									DeleteResult<T> deleteResult = new DeleteResult<T>(
											entry.getKey());
									result.add(deleteResult);
									try {
										entry.getValue().now();
									} catch (Exception e) {
										deleteResult.setException(e);
									}
								}
								return result;
							}
						}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.phoebe.dynamodb.datastore.AsyncDatastore#delete(com.phoebe.dynamodb
	 * .datastore.query.Query)
	 */
	@Override
	public <T> FutureResult<Iterable<DeleteResult<T>>> delete(
			final Query<T> query) {
		return new FutureResultAdapter<Iterable<DeleteResult<T>>>(phoebe
				.getExecutorService().submit(
						new Callable<Iterable<DeleteResult<T>>>() {
							@Override
							public Iterable<DeleteResult<T>> call()
									throws Exception {
								Map<EntityKey<T>, FutureResult<Void>> futureResultMap = new HashMap<EntityKey<T>, FutureResult<Void>>();
								Iterator<EntityKey<T>> entityKeyIterator = query
										.fetchEntityKeys();
								EntityKey<T> entityKey;
								while (entityKeyIterator.hasNext()) {
									entityKey = entityKeyIterator.next();
									futureResultMap.put(entityKey,
											delete(entityKey));
								}

								List<DeleteResult<T>> result = new ArrayList<DeleteResult<T>>();
								for (Entry<EntityKey<T>, FutureResult<Void>> entry : futureResultMap
										.entrySet()) {
									DeleteResult<T> deleteResult = new DeleteResult<T>(
											entry.getKey());
									result.add(deleteResult);
									try {
										entry.getValue().now();
									} catch (Exception e) {
										deleteResult.setException(e);
									}
								}

								return result;
							}
						}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.phoebe.dynamodb.datastore.AsyncDatastore#delete(java.lang.Object)
	 */
	@Override
	public <T> FutureResult<Void> delete(T entity) {
		return delete(EntityKey.create(entity));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.swengle.phoebe.datastore.AsyncDatastore#dropTable(java.lang.Class)
	 */
	@Override
	public <T> FutureResult<Void> dropTable(final Class<T> kindClass) {
		return new FutureResultAdapter<Void>(phoebe.getExecutorService()
				.submit(new Callable<Void>() {
					public Void call() throws Exception {
						String tableName = DynamoDBReflector.INSTANCE.getTable(
								kindClass).tableName();
						DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
								.withTableName(tableName);
						if (LOG.isDebugEnabled()) {
							LOG.debug("DeleteTableRequest: "
									+ deleteTableRequest);
						}
						try {
							phoebe.getClient().deleteTable(deleteTableRequest);
						} catch (AmazonServiceException ase) {
							if (ase.getErrorCode().equalsIgnoreCase(
									"ResourceNotFoundException") == false) {
								throw ase;
							} else {
								return null;
							}
						}
						waitForTableToBecomeDeleted(tableName);
						return null;
					}
				}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.swengle.phoebe.datastore.AsyncDatastore#get(java.lang.Class,
	 * com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.Iterable)
	 */
	@Override
	public <T> FutureResult<List<T>> get(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			Iterable<String> hashKeyRangeKeys) {
		List<EntityKey<T>> entityKeyList = new ArrayList<EntityKey<T>>();
		for (String hashKeyRangeKey : hashKeyRangeKeys) {
			entityKeyList.add(EntityKey.create(kindClass,
					hashKeyRangeKeyResolver, hashKeyRangeKey));
		}
		return get(entityKeyList);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.swengle.phoebe.datastore.AsyncDatastore#get(java.lang.Class,
	 * com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.String)
	 */
	@Override
	public <T> FutureResult<T> get(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			String hashKeyRangeKey) {
		return get(EntityKey.create(kindClass, hashKeyRangeKeyResolver,
				hashKeyRangeKey));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.swengle.phoebe.datastore.AsyncDatastore#get(java.lang.Class,
	 * com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.String[])
	 */
	@Override
	public <T> FutureResult<List<T>> get(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			String... hashKeyRangeKeys) {
		return get(kindClass, hashKeyRangeKeyResolver,
				Arrays.asList(hashKeyRangeKeys));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#get(java.lang.Class,
	 * java.lang.Iterable)
	 */
	@Override
	public <T> FutureResult<List<T>> get(Class<T> kindClass,
			Iterable<? extends Object> hashKeys) {
		List<EntityKey<? extends T>> entityKeyList = new ArrayList<EntityKey<? extends T>>();
		for (Object hashKey : hashKeys) {
			entityKeyList.add(EntityKey.create(kindClass, hashKey));
		}
		return get(entityKeyList);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#get(java.lang.Class,
	 * java.lang.Object)
	 */
	@Override
	public <T> FutureResult<T> get(Class<T> kindClass, Object hashKey) {
		return get(kindClass, hashKey, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#get(java.lang.Class,
	 * java.lang.Object[])
	 */
	@Override
	public <T> FutureResult<List<T>> get(Class<T> kindClass, Object... hashKeys) {
		return get(kindClass, Arrays.asList(hashKeys));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#get(java.lang.Class,
	 * java.lang.Object, java.lang.Object)
	 */
	@Override
	public <T> FutureResult<T> get(final Class<T> kindClass,
			final Object hashKey, final Object rangeKey) {
		return get(EntityKey.create(kindClass, hashKey, rangeKey));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.phoebe.dynamodb.datastore.AsyncDatastore#get(com.phoebe.dynamodb.
	 * datastore.key.EntityKey)
	 */
	@Override
	public <T> FutureResult<T> get(final EntityKey<T> entityKey) {
		return new FutureResultAdapter<T>(phoebe.getExecutorService().submit(
				new Callable<T>() {
					public T call() throws Exception {
						GetItemRequest getItemRequest = new GetItemRequest()
								.withTableName(
										entityKey.getDynamoDBTable()
												.tableName())
								.withKey(entityKey.getDynamoDBKey())
								.withConsistentRead(consistentRead);

						if (LOG.isDebugEnabled()) {
							LOG.debug("GetItemRequest: " + getItemRequest);
						}

						GetItemResult getItemResult = phoebe.getClient()
								.getItem(getItemRequest);

						if (getItemResult.getItem() == null) {
							return null;
						}

						T entity = phoebe.marshallIntoObject(
								entityKey.getKindClass(),
								getItemResult.getItem());

						if (lifecycleEnabled) {
							Collection<Method> onReadMethods = DynamoDBReflector.INSTANCE
									.getOnReadMethods(entity.getClass());
							for (Method onReadMethod : onReadMethods) {
								DynamoDBReflector.INSTANCE.safeInvoke(onReadMethod,
										entity);
							}
						}

						return entity;
					}
				}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#get(java.lang.Iterable)
	 */
	@Override
	public <T> FutureResult<List<T>> get(
			final Iterable<? extends EntityKey<? extends T>> entityKeys) {
		if (consistentRead) {
			return new FutureResultAdapter<List<T>>(phoebe.getExecutorService()
					.submit(new Callable<List<T>>() {
						public List<T> call() throws Exception {
							List<FutureResult<? extends T>> futureResultList = new ArrayList<FutureResult<? extends T>>();
							for (EntityKey<? extends T> entityKey : entityKeys) {
								futureResultList.add(get(entityKey));
							}
							Map<EntityKey<? extends T>, T> entityMap = new HashMap<EntityKey<? extends T>, T>();
							
							T entity;
							for (FutureResult<? extends T> futureResult : futureResultList) {
								entity = futureResult.now();
								entityMap.put(EntityKey.create(entity), entity);
							}
							List<T> result = new ArrayList<T>();
							for (EntityKey<? extends T> entityKey : entityKeys) {
								result.add(entityMap.get(entityKey));
							}
							return result;
						}
					}));
		} else {
			return new FutureResultAdapter<List<T>>(phoebe.getExecutorService()
					.submit(new Callable<List<T>>() {
						public List<T> call() throws Exception {
							Map<Class<? extends T>, List<Key>> keyMap = new HashMap<Class<? extends T>, List<Key>>();
							List<Key> keyList;
							int size = 0;
							for (EntityKey<? extends T> entityKey : entityKeys) {
								size++;
								keyList = keyMap.get(entityKey.getKindClass());
								if (keyList == null) {
									keyList = new ArrayList<Key>();
									keyMap.put(entityKey.getKindClass(),
											keyList);
								}
								keyList.add(entityKey.getDynamoDBKey());
							}

							if (size == 0) {
								return new ArrayList<T>();
							} else if (size > 50) {
								List<FutureResult<? extends T>> futureResultList = new ArrayList<FutureResult<? extends T>>();
								for (EntityKey<? extends T> entityKey : entityKeys) {
									futureResultList.add(get(entityKey));
								}
								Map<EntityKey<? extends T>, T> entityMap = new HashMap<EntityKey<? extends T>, T>();
								
								T entity;
								for (FutureResult<? extends T> futureResult : futureResultList) {
									entity = futureResult.now();
									entityMap.put(EntityKey.create(entity), entity);
								}
								List<T> result = new ArrayList<T>();
								for (EntityKey<? extends T> entityKey : entityKeys) {
									result.add(entityMap.get(entityKey));
								}
								return result;
							} else {
								Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
								Iterator<Entry<Class<? extends T>, List<Key>>> it = keyMap
										.entrySet().iterator();
								Entry<Class<? extends T>, List<Key>> entry;
								while (it.hasNext()) {
									entry = it.next();
									requestItems.put(DynamoDBReflector.INSTANCE
											.getTable(entry.getKey()).tableName(),
											new KeysAndAttributes().withKeys(entry
													.getValue()));
								}
	
								BatchGetItemResult batchGetItemResult;
								String tableName;
								T entity;
								Class<? extends T> kindClass;
								Map<EntityKey<? extends T>, T> entityMap = new HashMap<EntityKey<? extends T>, T>();
								do {
									BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest()
											.withRequestItems(requestItems);
	
									if (LOG.isDebugEnabled()) {
										LOG.debug("BatchGetItemRequest: "
												+ batchGetItemRequest);
									}
	
									batchGetItemResult = phoebe.getClient()
											.batchGetItem(batchGetItemRequest);
	
									Iterator<Class<? extends T>> classIterator = keyMap
											.keySet().iterator();
									while (classIterator.hasNext()) {
										kindClass = classIterator.next();
										tableName = DynamoDBReflector.INSTANCE
												.getTable(kindClass).tableName();
										BatchResponse batchResponse = batchGetItemResult
												.getResponses().get(tableName);
										
										for (Map<String, AttributeValue> item : batchResponse
												.getItems()) {
											entity = phoebe.marshallIntoObject(
													kindClass, item);
											if (lifecycleEnabled) {
												Collection<Method> onReadMethods = DynamoDBReflector.INSTANCE
														.getOnReadMethods(entity
																.getClass());
												for (Method onReadMethod : onReadMethods) {
													DynamoDBReflector.INSTANCE
															.safeInvoke(onReadMethod,
																	entity);
												}
											}
											entityMap.put(EntityKey.create(entity), entity);
										}
									}
	
									// Check for unprocessed keys which could happen
									// if
									// you exceed
									// provisioned
									// throughput or reach the limit on response
									// size.
									for (Map.Entry<String, KeysAndAttributes> pair : batchGetItemResult
											.getUnprocessedKeys().entrySet()) {
										if (LOG.isDebugEnabled()) {
											LOG.debug("Unprocessed key pair: "
													+ pair.getKey() + ", "
													+ pair.getValue());
										}
									}
									batchGetItemRequest
											.setRequestItems(batchGetItemResult
													.getUnprocessedKeys());
								} while (batchGetItemResult.getUnprocessedKeys()
										.size() > 0);
	
								List<T> result = new ArrayList<T>();
								for (EntityKey<? extends T> entityKey : entityKeys) {
									if (entityMap.containsKey(entityKey)) {
										result.add(entityMap.get(entityKey));
									}
								}
								return result;
							}
						}
					}));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.swengle.phoebe.datastore.AsyncDatastore#get(com.swengle.phoebe.query
	 * .Query)
	 */
	@Override
	public <T> FutureResult<List<T>> get(final Query<T> query) {
		return new FutureResultAdapter<List<T>>(phoebe.getExecutorService()
				.submit(new Callable<List<T>>() {
					public List<T> call() throws Exception {
						return query.asList();
					}
				}));
	}

	/**
	 * Returns a marshaller for the auto-generated key returned by the getter
	 * given.
	 */
	private AttributeValue getAutoGeneratedKeyAttributeValue(final Method getter) {
		Class<?> returnType = getter.getReturnType();
		if (String.class.isAssignableFrom(returnType)) {
			return new AttributeValue().withS(new ObjectId().toString());
		} else {
			throw new DynamoDBMappingException(
					"Unsupported type for "
							+ getter
							+ ": "
							+ returnType
							+ ".  Only Strings are supported when auto-generating keys.");
		}
	}

	private String getKeyType(Method method) {
		Class<?> methodReturnType = method.getReturnType();
		if (methodReturnType.isPrimitive()
				|| Number.class.isAssignableFrom(methodReturnType)) {
			return "N";
		} else if (String.class.isAssignableFrom(methodReturnType)) {
			return "S";
		} else {
			throw new DynamoDBMappingException(
					"Hash key property must be either a Number or a String");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.swengle.phoebe.datastore.AsyncDatastore#insert(java.lang.Iterable)
	 */
	@Override
	public <T> FutureResult<Iterable<SaveResult<T>>> insert(
			final Iterable<? extends T> entities) {
		return new FutureResultAdapter<Iterable<SaveResult<T>>>(phoebe
				.getExecutorService().submit(
						new Callable<Iterable<SaveResult<T>>>() {
							@Override
							public Iterable<SaveResult<T>> call()
									throws Exception {
								Map<T, FutureResult<Void>> futureResultMap = new HashMap<T, FutureResult<Void>>();
								for (T entity : entities) {
									futureResultMap.put(entity, insert(entity));
								}
								List<SaveResult<T>> result = new ArrayList<SaveResult<T>>();
								// wait for all tasks to complete before
								// continuing
								for (Entry<T, FutureResult<Void>> entry : futureResultMap
										.entrySet()) {
									SaveResult<T> saveResult = new SaveResult<T>(
											entry.getKey());
									result.add(saveResult);
									try {
										entry.getValue().now();
									} catch (Exception e) {
										saveResult.setException(e);
									}
								}
								return result;
							}
						}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.swengle.phoebe.datastore.AsyncDatastore#insert(java.lang.Object)
	 */
	@Override
	public <T> FutureResult<Void> insert(final T entity) {
		return new FutureResultAdapter<Void>(phoebe.getExecutorService()
				.submit(new Callable<Void>() {
					public Void call() throws Exception {
						create(entity, true);
						return null;
					}
				}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#put(java.lang.Iterable)
	 */
	@Override
	public <T> FutureResult<Iterable<SaveResult<T>>> put(
			final Iterable<? extends T> entities) {
		return new FutureResultAdapter<Iterable<SaveResult<T>>>(phoebe
				.getExecutorService().submit(
						new Callable<Iterable<SaveResult<T>>>() {
							@Override
							public Iterable<SaveResult<T>> call()
									throws Exception {
								Map<T, FutureResult<Void>> futureResultMap = new HashMap<T, FutureResult<Void>>();
								for (T entity : entities) {
									futureResultMap.put(entity, put(entity));
								}
								List<SaveResult<T>> result = new ArrayList<SaveResult<T>>();
								// wait for all tasks to complete before
								// continuing
								for (Entry<T, FutureResult<Void>> entry : futureResultMap
										.entrySet()) {
									SaveResult<T> saveResult = new SaveResult<T>(
											entry.getKey());
									result.add(saveResult);
									try {
										entry.getValue().now();
									} catch (Exception e) {
										saveResult.setException(e);
									}
								}
								return result;
							}
						}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.phoebe.dynamodb.datastore.AsyncDatastore#put(java.lang.Object)
	 */
	@Override
	public <T> FutureResult<Void> put(final T entity) {
		return new FutureResultAdapter<Void>(phoebe.getExecutorService()
				.submit(new Callable<Void>() {
					public Void call() throws Exception {
						create(entity, false);
						return null;
					}
				}));
	}

	/**
	 * Sets the value in the return object corresponding to the service result.
	 */
	private <T> void setValue(final T toReturn, final Method getter,
			AttributeValue value) {

		Method setter = DynamoDBReflector.INSTANCE.getSetter(getter);
		ArgumentUnmarshaller unmarhsaller = DynamoDBReflector.INSTANCE
				.getArgumentUnmarshaller(toReturn, getter, setter);
		unmarhsaller.typeCheck(value, setter);

		Object argument;
		try {
			argument = unmarhsaller.unmarshall(value);
		} catch (IllegalArgumentException e) {
			throw new DynamoDBMappingException("Couldn't unmarshall value "
					+ value + " for " + setter, e);
		} catch (ParseException e) {
			throw new DynamoDBMappingException(
					"Error attempting to parse date string " + value + " for "
							+ setter, e);
		}

		DynamoDBReflector.INSTANCE.safeInvoke(setter, toReturn, argument);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.phoebe.dynamodb.datastore.AsyncDatastore#update(com.phoebe.dynamodb
	 * .datastore.key.EntityKey,
	 * com.phoebe.dynamodb.datastore.query.UpdateOperations)
	 */
	@Override
	public <T> FutureResult<UpdateResult<T>> update(
			final EntityKey<T> entityKey, final UpdateOperations<T> ops) {
		return new FutureResultAdapter<UpdateResult<T>>(phoebe
				.getExecutorService().submit(new Callable<UpdateResult<T>>() {
					public UpdateResult<T> call() throws Exception {
						UpdateResult<T> updateResult = new UpdateResult<T>(
								entityKey);
						Map<String, AttributeValueUpdate> updateItems = ((UpdateOperationsImpl<T>) ops)
								.getUpdateItems();
						if (updateItems.size() == 0) {
							return updateResult;
						}

						Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
						expected.put(DynamoDBReflector.INSTANCE
								.getHashKeyAttributeName(entityKey
										.getKindClass()),
								new ExpectedAttributeValue()
										.withValue(entityKey.getDynamoDBKey()
												.getHashKeyElement()));

						UpdateItemRequest updateItemRequest = new UpdateItemRequest()
								.withTableName(
										entityKey.getDynamoDBTable()
												.tableName())
								.withKey(entityKey.getDynamoDBKey())
								.withAttributeUpdates(updateItems)
								.withExpected(expected);

						if (LOG.isDebugEnabled()) {
							LOG.debug("UpdateItemRequest: " + updateItemRequest);
						}

						Collection<Method> onUpdateMethods = null;
						if (lifecycleEnabled) {
							onUpdateMethods = DynamoDBReflector.INSTANCE
									.getOnUpdateMethods(entityKey.getKindClass());
							if (onUpdateMethods.size() > 0) {
								updateItemRequest
										.setReturnValues(ReturnValue.ALL_NEW);
							}
						}

						try {
							UpdateItemResult updateItemResult = phoebe
									.getClient().updateItem(updateItemRequest);
							if (onUpdateMethods != null && onUpdateMethods.size() > 0) {
								T entity = phoebe.marshallIntoObject(
										entityKey.getKindClass(),
										updateItemResult.getAttributes());
								for (Method onUpdateMethod : onUpdateMethods) {
									DynamoDBReflector.INSTANCE.safeInvoke(
											onUpdateMethod, entity);
								}
							}
							updateResult.setUpdated(true);
						} catch (ConditionalCheckFailedException e) {
							// ignore
							// entity does not exist, we only updated existing
							// entities
						} catch (Exception e) {
							updateResult.setException(e);
						}

						return updateResult;
					}
				}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.phoebe.dynamodb.datastore.AsyncDatastore#update(java.lang.Iterable,
	 * com.phoebe.dynamodb.datastore.query.UpdateOperations)
	 */
	@Override
	public <T> FutureResult<Iterable<UpdateResult<T>>> update(
			final Iterable<EntityKey<T>> entityKeys,
			final UpdateOperations<T> ops) {
		return new FutureResultAdapter<Iterable<UpdateResult<T>>>(phoebe
				.getExecutorService().submit(
						new Callable<Iterable<UpdateResult<T>>>() {
							@Override
							public Iterable<UpdateResult<T>> call()
									throws Exception {
								List<FutureResult<UpdateResult<T>>> futureResultList = new ArrayList<FutureResult<UpdateResult<T>>>();
								for (EntityKey<T> entityKey : entityKeys) {
									futureResultList
											.add(update(entityKey, ops));
								}

								List<UpdateResult<T>> result = new ArrayList<UpdateResult<T>>();
								for (FutureResult<UpdateResult<T>> futureResult : futureResultList) {
									result.add(futureResult.now());
								}
								return result;
							}
						}));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.phoebe.dynamodb.datastore.AsyncDatastore#update(com.phoebe.dynamodb
	 * .datastore.query.Query,
	 * com.phoebe.dynamodb.datastore.query.UpdateOperations)
	 */
	@Override
	public <T> FutureResult<Iterable<UpdateResult<T>>> update(
			final Query<T> query, final UpdateOperations<T> ops) {
		return new FutureResultAdapter<Iterable<UpdateResult<T>>>(phoebe
				.getExecutorService().submit(
						new Callable<Iterable<UpdateResult<T>>>() {
							@Override
							public Iterable<UpdateResult<T>> call()
									throws Exception {
								List<FutureResult<UpdateResult<T>>> futureResultList = new ArrayList<FutureResult<UpdateResult<T>>>();
								Iterator<EntityKey<T>> objectKeyIterator = query
										.fetchEntityKeys();
								EntityKey<T> objectKey;
								while (objectKeyIterator.hasNext()) {
									objectKey = objectKeyIterator.next();
									futureResultList
											.add(update(objectKey, ops));
								}

								List<UpdateResult<T>> result = new ArrayList<UpdateResult<T>>();
								for (FutureResult<UpdateResult<T>> futureResult : futureResultList) {
									result.add(futureResult.now());
								}
								return result;
							}
						}));
	}

	private void waitForTableToBecomeAvailable(String tableName) {
		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(1000 * 5);
			} catch (Exception e) {
			}
			try {
				DescribeTableRequest request = new DescribeTableRequest()
						.withTableName(tableName);
				TableDescription tableDescription = phoebe.getClient()
						.describeTable(request).getTable();
				String tableStatus = tableDescription.getTableStatus();
				if (tableStatus.equals(TableStatus.ACTIVE.toString()))
					return;
			} catch (AmazonServiceException ase) {
				if (ase.getErrorCode().equalsIgnoreCase(
						"ResourceNotFoundException") == false)
					throw ase;
			}
		}
		throw new RuntimeException("Table " + tableName + " never went active");
	}

	private void waitForTableToBecomeDeleted(String tableName) {
		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(1000 * 5);
			} catch (Exception e) {
			}
			try {
				DescribeTableRequest request = new DescribeTableRequest()
						.withTableName(tableName);
				phoebe.getClient().describeTable(request).getTable();
			} catch (AmazonServiceException ase) {
				if (ase.getErrorCode().equalsIgnoreCase(
						"ResourceNotFoundException") == false)
					throw ase;
				else {
					return;
				}
			}
		}
		throw new RuntimeException("Table " + tableName + " never went deleted");
	}

	/**
	 * @return the lifecycleEnabled
	 */
	public boolean isLifecycleEnabled() {
		return lifecycleEnabled;
	}

	/**
	 * @param lifecycleEnabled the lifecycleEnabled to set
	 */
	public void setLifecycleEnabled(boolean lifecycleEnabled) {
		this.lifecycleEnabled = lifecycleEnabled;
	}

}
