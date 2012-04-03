/**
 * 
 */
package com.swengle.phoebe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.swengle.phoebe.auth.STSSessionCredentialsProviderWithClientConfiguration;
import com.swengle.phoebe.datastore.AsyncDatastore;
import com.swengle.phoebe.datastore.AsyncDatastoreImpl;
import com.swengle.phoebe.datastore.Datastore;
import com.swengle.phoebe.datastore.DatastoreImpl;
import com.swengle.phoebe.query.RangeQuery;
import com.swengle.phoebe.query.RangeQueryImpl;
import com.swengle.phoebe.query.ScanQuery;
import com.swengle.phoebe.query.ScanQueryImpl;
import com.swengle.phoebe.query.UpdateOperations;
import com.swengle.phoebe.query.UpdateOperationsImpl;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class Phoebe {
	private AmazonDynamoDB client;
	private DynamoDBMapper mapper;
	private ExecutorService executorService;
	
	private Datastore datastoreNotConsistentNotLifecycleEnabled;
	private AsyncDatastore asyncDatastoreNotConsistentNotLifecycleEnabled;
	
	private Datastore datastoreNotConsistentIsLifecycleEnabled;
	private AsyncDatastore asyncDatastoreNotConsistentIsLifecycleEnabled;
	
	private Datastore datastoreIsConsistentNotLifecycleEnabled;
	private AsyncDatastore asyncDatastoreIsConsistentNotLifecycleEnabled;

	private Datastore datastoreIsConsistentIsLifecycleEnabled;
	private AsyncDatastore asyncDatastoreIsConsistentIsLifecycleEnabled;
	
	
	/**
	 * 
	 */
	public Phoebe(AWSCredentials awsCredentials) {
        this(awsCredentials, new ClientConfiguration());
    }
	
	/**
	 * 
	 */
	public Phoebe(AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
		this(new STSSessionCredentialsProviderWithClientConfiguration(
				awsCredentials, clientConfiguration), clientConfiguration);
    }
	
	/**
	 * 
	 */
	public Phoebe(AWSCredentialsProvider awsCredentialsProvider) {
		this(awsCredentialsProvider, new ClientConfiguration());
	}

	/**
	 * 
	 */
	public Phoebe(AWSCredentialsProvider awsCredentialsProvider,
			ClientConfiguration clientConfiguration) {
		client = new AmazonDynamoDBClient(awsCredentialsProvider,
					clientConfiguration);
		mapper = new DynamoDBMapper(client);
		executorService = Executors.newCachedThreadPool();
		
		asyncDatastoreNotConsistentNotLifecycleEnabled = new AsyncDatastoreImpl(this, false, false);
		datastoreNotConsistentNotLifecycleEnabled = new DatastoreImpl(asyncDatastoreNotConsistentNotLifecycleEnabled);
		
		asyncDatastoreNotConsistentIsLifecycleEnabled = new AsyncDatastoreImpl(this, false, true);
		datastoreNotConsistentIsLifecycleEnabled = new DatastoreImpl(asyncDatastoreNotConsistentIsLifecycleEnabled);
		
		asyncDatastoreIsConsistentNotLifecycleEnabled = new AsyncDatastoreImpl(this, true, false);
		datastoreIsConsistentNotLifecycleEnabled = new DatastoreImpl(asyncDatastoreIsConsistentNotLifecycleEnabled);
		
		asyncDatastoreIsConsistentIsLifecycleEnabled = new AsyncDatastoreImpl(this, true, true); 
		datastoreIsConsistentIsLifecycleEnabled = new DatastoreImpl(asyncDatastoreIsConsistentIsLifecycleEnabled);
	}

	/**
	 * @return the DynamoDBMapper mapper
	 */
	public DynamoDBMapper getMapper() {
		return mapper;
	}

	/**
	 * @return the AmazonDynamoDB client
	 */
	public AmazonDynamoDB getClient() {
		return client;
	}

	/**
	 * @return the executorService
	 */
	public ExecutorService getExecutorService() {
		return executorService;
	}

	/**
	 * @param executorService the executorService to set
	 */
	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

	/**
	 * @return the datastore
	 */
	public Datastore getDatastore() {
		return getDatastore(false, true);
	}
	
	public Datastore getDatastore(boolean consistentRead) {
		return getDatastore(consistentRead, true);
	}

	public Datastore getDatastore(boolean consistentRead, boolean lifecycleEnabled) {
		if (!consistentRead && !lifecycleEnabled) {
			return datastoreNotConsistentNotLifecycleEnabled;
		} else if (!consistentRead && lifecycleEnabled) {
			return datastoreNotConsistentIsLifecycleEnabled;
		} else if (consistentRead && !lifecycleEnabled) {
			return datastoreIsConsistentNotLifecycleEnabled;
		}
		return datastoreIsConsistentIsLifecycleEnabled;
	}


	/**
	 * @return the asyncDatastore
	 */
	public AsyncDatastore getAsyncDatastore() {
		return getAsyncDatastore(false, true);
	}
	
	public AsyncDatastore getAsyncDatastore(boolean consistentRead) {
		return getAsyncDatastore(consistentRead, true);
	}

	public AsyncDatastore getAsyncDatastore(boolean consistentRead, boolean lifecycleEnabled) {
		if (!consistentRead && !lifecycleEnabled) {
			return asyncDatastoreNotConsistentNotLifecycleEnabled;
		} else if (!consistentRead && lifecycleEnabled) {
			return asyncDatastoreNotConsistentIsLifecycleEnabled;
		} else if (consistentRead && !lifecycleEnabled) {
			return asyncDatastoreIsConsistentNotLifecycleEnabled;
		}
		return asyncDatastoreIsConsistentIsLifecycleEnabled;
	}
	
	public <T> ScanQuery<T> createScanQuery(Class<T> kindClass) {
		return new ScanQueryImpl<T>(this, kindClass);
	}
	
	public <T> RangeQuery<T> createRangeQuery(Class<T> kindClass, Object hashKey) {
		return new RangeQueryImpl<T>(this, kindClass, hashKey);
	}
	
	public <T> RangeQuery<T> createConsistentReadRangeQuery(Class<T> kindClass, Object hashKey) {
		return new RangeQueryImpl<T>(this, kindClass, hashKey, true);
	}
	
	public <T> UpdateOperations<T> createUpdateOperations(Class<T> kindClass) {
		return new UpdateOperationsImpl<T>(kindClass);
	}
	
	public <T> T marshallIntoObject(Class<T> kindClass, Map<String, AttributeValue> itemAttributes) {
		T entity = mapper.marshallIntoObject(kindClass, itemAttributes);
		return entity;
	}
	
	public <T> List<T> marshallIntoObjectList(Class<? extends T> kindClass,  List<Map<String, AttributeValue>> items) {
		List<T> objList = new ArrayList<T>();
		for (Map<String, AttributeValue> item : items) {
			objList.add(marshallIntoObject(kindClass, item));
		}
		return objList;
	}


}
