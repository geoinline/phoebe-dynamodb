/**
 * 
 */
package com.swengle.phoebe.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.query.RangeQuery;
import com.swengle.phoebe.query.UpdateOperations;
import com.swengle.phoebe.result.UpdateResult;
import com.swengle.phoebe.test.model.ObjectWithHashKeyAndRangeKey;

/**
 * @author Administrator
 *
 */
public class TestRangeQuery extends TestBase {
	private static final String TOP_RANGE_ID = new ObjectId().toString();
	private static final String MIDDLE_RANGE_ID = new ObjectId().toString();
	private static final String BOTTOM_RANGE_ID = new ObjectId().toString();
	private Phoebe phoebe = TestBase.PHOEBE;
	
	@Test
	public void testQuerying() {
		emptyAll();
		Set<String> set = new HashSet<String>();
		set.add("A");
		set.add("B");
		set.add("C");
		
		// object 1
		ObjectWithHashKeyAndRangeKey obj = new ObjectWithHashKeyAndRangeKey();
		obj.setId("RangeTest");
		obj.setRangeId(TOP_RANGE_ID);
		obj.setFoo("foobar");
		obj.setBar(TOP_RANGE_ID);
		obj.setSet(set);
		phoebe.getDatastore().put(obj);
		
		// object 2
		obj.setRangeId(MIDDLE_RANGE_ID);
		obj.setBar(MIDDLE_RANGE_ID);
		obj.setSet(set);
		phoebe.getDatastore().put(obj);
		
		// object 2
		obj.setRangeId(BOTTOM_RANGE_ID);
		obj.setBar(BOTTOM_RANGE_ID);
		obj.setSet(set);
		phoebe.getDatastore().put(obj);
		
		// object 4
		obj.setRangeId(new ObjectId().toString());
		obj.setBar(null);
		obj.setSet(set);
		phoebe.getDatastore().put(obj);
		
		// object 5
		obj.setRangeId(new ObjectId().toString());
		obj.setBar(null);
		obj.getSet().remove("B");
		phoebe.getDatastore().put(obj);
		
		RangeQuery<ObjectWithHashKeyAndRangeKey> query = phoebe.createRangeQuery(ObjectWithHashKeyAndRangeKey.class, "RangeTest");
		Assert.assertTrue(query.count() == 5);
		
		query = phoebe.createRangeQuery(ObjectWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().greaterThan(TOP_RANGE_ID);
		Assert.assertTrue(query.count() == 4);

		query = phoebe.createRangeQuery(ObjectWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().greaterThanOrEq(TOP_RANGE_ID);
		Assert.assertTrue(query.count() == 5);
		
		query = phoebe.createRangeQuery(ObjectWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().lessThan(BOTTOM_RANGE_ID);
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createRangeQuery(ObjectWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().lessThanOrEq(BOTTOM_RANGE_ID);
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createRangeQuery(ObjectWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().equal(BOTTOM_RANGE_ID);
		Assert.assertTrue(query.count() == 1);
		
		query = phoebe.createRangeQuery(ObjectWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().between(TOP_RANGE_ID, BOTTOM_RANGE_ID);
		Assert.assertTrue(query.count() == 3);
		
		
		query = phoebe.createRangeQuery(ObjectWithHashKeyAndRangeKey.class, "RangeTest");;
		query.withExclusiveStartKey().equal("RangeTest", MIDDLE_RANGE_ID);
		Assert.assertTrue(query.count() == 3);

	}
	

	@Test
	public void testUpdating() {
		RangeQuery<ObjectWithHashKeyAndRangeKey> queryAll = phoebe.createRangeQuery(ObjectWithHashKeyAndRangeKey.class, "RangeTest");
		Assert.assertTrue(queryAll.count() == 5);
		
		RangeQuery<ObjectWithHashKeyAndRangeKey> query = phoebe.createRangeQuery(ObjectWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().between(TOP_RANGE_ID, BOTTOM_RANGE_ID);
		Assert.assertTrue(query.count() == 3);
		
		// test set op
		UpdateOperations<ObjectWithHashKeyAndRangeKey> ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.set("foo", "newfoobar");
		Iterable<UpdateResult<ObjectWithHashKeyAndRangeKey>> updateResultIterable = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResultIterable);
		List<ObjectWithHashKeyAndRangeKey> listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (ObjectWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read.getFoo().equals("newfoobar"));
		}
		
		// test inc
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.inc("counter");
		updateResultIterable = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResultIterable);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (ObjectWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read.getCounter() == 1);
		}

		
		// test inc+3
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.inc("counter", 3);
		updateResultIterable = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResultIterable);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (ObjectWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read.getCounter() == 4);
		}

		
		// test inc-2
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.inc("counter", -2);
		updateResultIterable = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResultIterable);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (ObjectWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read.getCounter() == 2);
		}
		
		// test dec
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.dec("counter");
		updateResultIterable = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResultIterable);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (ObjectWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read.getCounter() == 1);
		}

		
		// test remove
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.remove("set", "B");
		updateResultIterable = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResultIterable);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (ObjectWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read != null);
			Assert.assertTrue(read.getSet().size() == 2);
			Assert.assertTrue(read.getSet().contains("B") == false);
		}

		// test add
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.add("set", "D");
		updateResultIterable = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResultIterable);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (ObjectWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read != null);
			Assert.assertTrue(read.getSet().size() == 3);
			Assert.assertTrue(read.getSet().contains("D") == true);
		}

		// test addAll
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.addAll("set", Arrays.asList("E", "F"));
		updateResultIterable = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResultIterable);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (ObjectWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read != null);
			Assert.assertTrue(read.getSet().size() == 5);
			Assert.assertTrue(read.getSet().contains("E") == true);
			Assert.assertTrue(read.getSet().contains("F") == true);
		}
		
		// test removeAll
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.removeAll("set", Arrays.asList("A", "C"));
		updateResultIterable = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResultIterable);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (ObjectWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read != null);
			Assert.assertTrue(read.getSet().size() == 3);
			Assert.assertTrue(read.getSet().contains("A") == false);
			Assert.assertTrue(read.getSet().contains("C") == false);
		}
		
		// test unset
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.unset("foo");
		updateResultIterable = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResultIterable);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (ObjectWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read != null);
			Assert.assertTrue(read.getFoo() == null);
		}
		
	}
	
	private void checkUpdateResults(Iterable<UpdateResult<ObjectWithHashKeyAndRangeKey>> updateResultIterable) {
		Iterator<UpdateResult<ObjectWithHashKeyAndRangeKey>> updateResultIterator = updateResultIterable.iterator();
		UpdateResult<ObjectWithHashKeyAndRangeKey> updateResult;
		int count = 0;
		while (updateResultIterator.hasNext()) {
			count++;
			updateResult = updateResultIterator.next();
			Assert.assertTrue(updateResult.getException() == null);
			Assert.assertTrue(updateResult.isUpdated() == true);
		}
		Assert.assertTrue(count == 3);
	}


}
