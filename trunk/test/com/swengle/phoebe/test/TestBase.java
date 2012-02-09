/**
 * 
 */
package com.swengle.phoebe.test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.BeforeClass;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;
import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.auth.STSSessionCredentialsProviderWithClientConfiguration;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.reflect.DynamoDBReflector;
import com.swengle.phoebe.test.model.ObjectWithHashKey;
import com.swengle.phoebe.test.model.ObjectWithHashKeyAndRangeKey;

/**
 * @author Administrator
 * 
 */
public class TestBase {
	public static Phoebe PHOEBE;
	private static final Log LOG = LogFactory.getLog(TestBase.class);

	
	@BeforeClass
	public static void setUpTables() throws IOException {
		AWSCredentials credentials = new PropertiesCredentials(
				TestBase.class.getClassLoader().getResourceAsStream("AwsCredentials.properties"));
		PHOEBE = new Phoebe(credentials);
		PHOEBE.getClient().setEndpoint(
				"http://dynamodb.us-east-1.amazonaws.com");
		createTestTables();
	}
	
	@Before
	public void setUp() {

	}
	
	protected void emptyAll() {
		empty(ObjectWithHashKey.class);
		empty(ObjectWithHashKeyAndRangeKey.class);
	}

	@SuppressWarnings("unused")
	private void deleteTestTables() {
		deleteTable(DynamoDBReflector.INSTANCE.getTable(ObjectWithHashKey.class).tableName());
		deleteTable(DynamoDBReflector.INSTANCE.getTable(ObjectWithHashKeyAndRangeKey.class).tableName());
	}


	private static void createTestTables() {
		Method getter = DynamoDBReflector.INSTANCE.getHashKeyGetter(ObjectWithHashKey.class);
		String objectWithHashKeyAttrName = DynamoDBReflector.INSTANCE.getAttributeName(getter);
		getter = DynamoDBReflector.INSTANCE.getHashKeyGetter(ObjectWithHashKeyAndRangeKey.class);
		String objectWithHashKeyAndRangeKeyHashAttrName = DynamoDBReflector.INSTANCE.getAttributeName(getter);
		getter = DynamoDBReflector.INSTANCE.getRangeKeyGetter(ObjectWithHashKeyAndRangeKey.class);
		String objectWithHashKeyAndRangeKeyRangeAttrName = DynamoDBReflector.INSTANCE.getAttributeName(getter);
		
		
		createTable(ObjectWithHashKey.class, 128L, 128L,
				objectWithHashKeyAttrName, "S");
		createTable(ObjectWithHashKeyAndRangeKey.class, 128L, 128L,
				objectWithHashKeyAndRangeKeyHashAttrName, "S",
				objectWithHashKeyAndRangeKeyRangeAttrName, "S");
	}
	
	private void deleteTable(String tableName) {
		DeleteTableRequest deleteTableRequest = new DeleteTableRequest().withTableName(tableName);
		try {
			PHOEBE.getClient().deleteTable(deleteTableRequest);
		} catch (AmazonServiceException ase) {
			if (ase.getErrorCode().equalsIgnoreCase(
					"ResourceNotFoundException") == false) {
				throw ase;
			} else {
				return;
			}
		}
		waitForTableToBecomeDelete(tableName);
	}

	private static <T> void createTable(Class<T> kindClass, long readCapacityUnits,
			long writeCapacityUnits, String hashKeyName, String hashKeyType) {

		createTable(kindClass, readCapacityUnits, writeCapacityUnits,
				hashKeyName, hashKeyType, null, null);
	}

	private static <T> void createTable(Class<T> kindClass, long readCapacityUnits,
			long writeCapacityUnits, String hashKeyName, String hashKeyType,
			String rangeKeyName, String rangeKeyType) {

		String tableName = DynamoDBReflector.INSTANCE.getTable(kindClass).tableName();
		try {
			KeySchemaElement hashKey = new KeySchemaElement()
					.withAttributeName(hashKeyName).withAttributeType(
							hashKeyType);
			KeySchema ks = new KeySchema().withHashKeyElement(hashKey);

			if (rangeKeyName != null) {
				KeySchemaElement rangeKey = new KeySchemaElement()
						.withAttributeName(rangeKeyName).withAttributeType(
								rangeKeyType);
				ks.setRangeKeyElement(rangeKey);
			}

			// Provide initial provisioned throughput values as Java long data
			// types
			ProvisionedThroughput provisionedthroughput = new ProvisionedThroughput()
					.withReadCapacityUnits(readCapacityUnits)
					.withWriteCapacityUnits(writeCapacityUnits);

			CreateTableRequest request = new CreateTableRequest()
					.withTableName(tableName).withKeySchema(ks)
					.withProvisionedThroughput(provisionedthroughput);

			PHOEBE.getClient().createTable(request);
			waitForTableToBecomeAvailable(tableName);

		} catch (AmazonServiceException ase) {
			if (ase.getMessage().indexOf("Duplicate table name") == -1) {
				LOG.error("Failed to create table " + tableName, ase);
				System.exit(1);
			} else {
				LOG.info(tableName + " already exists, no need to create");
			}
		}
	}
	
	private <T> void empty(Class<T> kindClass) {
		LOG.info("Emptying the table for: " + kindClass);
		List<EntityKey<T>> objectKeys = PHOEBE.createScanQuery(kindClass).asEntityKeyList();
		LOG.info("Found " + objectKeys.size() + " entities to remove.");
		PHOEBE.getAsyncDatastore().delete(objectKeys).now();
		LOG.info("Done emptying the table for: " + kindClass);
	}

	private static void waitForTableToBecomeAvailable(String tableName) {
		LOG.info("Waiting for " + tableName + " to become ACTIVE...");

		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(1000 * 20);
			} catch (Exception e) {
			}
			try {
				DescribeTableRequest request = new DescribeTableRequest()
						.withTableName(tableName);
				TableDescription tableDescription = PHOEBE.getClient()
						.describeTable(request).getTable();
				String tableStatus = tableDescription.getTableStatus();
				LOG.info("  - current state: " + tableStatus);
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

	
	private void waitForTableToBecomeDelete(String tableName) {
		LOG.info("Waiting for " + tableName + " to become DELETED...");

		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(1000 * 20);
			} catch (Exception e) {
			}
			try {
				DescribeTableRequest request = new DescribeTableRequest()
						.withTableName(tableName);
				PHOEBE.getClient()
						.describeTable(request).getTable();
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
	
}
