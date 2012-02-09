/**
 * 
 */
package com.swengle.phoebe.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.datastore.Datastore;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.query.UpdateOperations;
import com.swengle.phoebe.result.UpdateResult;
import com.swengle.phoebe.test.model.ObjectWithHashKeyAndRangeKey;

/**
 * @author Administrator
 *
 */
public class TestUpdate extends TestBase {
	private static final String HASH_ID = "UpdateTest";
	private static final String TOP_RANGE_ID = new ObjectId().toString();
	private static final String MIDDLE_RANGE_ID = new ObjectId().toString();
	private Phoebe phoebe = TestBase.PHOEBE;
	private Datastore datastore = TestBase.PHOEBE.getConsistentReadDatastore();

	@Test
	public void testUpdating() {
		emptyAll();
		// object 1
		ObjectWithHashKeyAndRangeKey obj = new ObjectWithHashKeyAndRangeKey();
		obj.setId(HASH_ID);
		obj.setRangeId(TOP_RANGE_ID);
		obj.setFoo("foobar");
		obj.setBar(TOP_RANGE_ID);
		phoebe.getDatastore().put(obj);
		
		// object 2
		obj.setRangeId(MIDDLE_RANGE_ID);
		obj.setBar(MIDDLE_RANGE_ID);
		Set<String> set = new HashSet<String>();
		set.add("A");
		set.add("B");
		set.add("C");
		obj.setSet(set);
		datastore.put(obj);
		
		// testing set op
		UpdateOperations<ObjectWithHashKeyAndRangeKey> ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.set("foo", "newfoobar");

		// test non-existing
		UpdateResult<ObjectWithHashKeyAndRangeKey> updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, "NEW"), ops);
		Assert.assertTrue(updateResult.getException() == null);
		Assert.assertTrue(updateResult.isUpdated() == false);
		
		ObjectWithHashKeyAndRangeKey read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, "NEW");
		Assert.assertTrue(read == null);

		// test existing
		updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, TOP_RANGE_ID), ops);
		Assert.assertTrue(updateResult.getException() == null);
		Assert.assertTrue(updateResult.isUpdated() == true);
		
		read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, TOP_RANGE_ID);
		Assert.assertTrue(read != null);
		Assert.assertTrue(read.getFoo().equals("newfoobar"));
		
		// test inc
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.inc("counter");
		updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, TOP_RANGE_ID), ops);
		
		read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, TOP_RANGE_ID);
		Assert.assertTrue(read != null);
		Assert.assertTrue(read.getCounter() == 1);
		
		// test inc+3
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.inc("counter", 3);
		updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, TOP_RANGE_ID), ops);
		
		read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, TOP_RANGE_ID);
		Assert.assertTrue(read != null);
		Assert.assertTrue(read.getCounter() == 4);
		
		// test inc-2
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.inc("counter", -2);
		updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, TOP_RANGE_ID), ops);
		
		read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, TOP_RANGE_ID);
		Assert.assertTrue(read != null);
		Assert.assertTrue(read.getCounter() == 2);
		
		// test dec
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.dec("counter");
		updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, TOP_RANGE_ID), ops);
		
		read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, TOP_RANGE_ID);
		Assert.assertTrue(read != null);
		Assert.assertTrue(read.getCounter() == 1);
		
		// test remove
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.remove("set", "B");
		updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, MIDDLE_RANGE_ID), ops);
		
		read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, MIDDLE_RANGE_ID);
		Assert.assertTrue(read != null);
		Assert.assertTrue(read.getSet().size() == 2);
		Assert.assertTrue(read.getSet().contains("B") == false);
		
		// test add
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.add("set", "D");
		updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, MIDDLE_RANGE_ID), ops);
		
		read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, MIDDLE_RANGE_ID);
		Assert.assertTrue(read != null);
		Assert.assertTrue(read.getSet().size() == 3);
		Assert.assertTrue(read.getSet().contains("D") == true);
		
		// test addAll
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.addAll("set", Arrays.asList("E", "F"));
		updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, MIDDLE_RANGE_ID), ops);
		
		read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, MIDDLE_RANGE_ID);
		Assert.assertTrue(read != null);
		Assert.assertTrue(read.getSet().size() == 5);
		Assert.assertTrue(read.getSet().contains("E") == true);
		Assert.assertTrue(read.getSet().contains("F") == true);
		
		// test removeAll
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.removeAll("set", Arrays.asList("A", "C"));
		updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, MIDDLE_RANGE_ID), ops);
		
		read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, MIDDLE_RANGE_ID);
		Assert.assertTrue(read != null);
		Assert.assertTrue(read.getSet().size() == 3);
		Assert.assertTrue(read.getSet().contains("A") == false);
		Assert.assertTrue(read.getSet().contains("C") == false);
		
		// test unset
		ops = phoebe.createUpdateOperations(ObjectWithHashKeyAndRangeKey.class);
		ops.unset("foo");
		updateResult = datastore.update(EntityKey.create(ObjectWithHashKeyAndRangeKey.class, HASH_ID, MIDDLE_RANGE_ID), ops);
		
		read = datastore.get(ObjectWithHashKeyAndRangeKey.class, HASH_ID, MIDDLE_RANGE_ID);
		Assert.assertTrue(read != null);
		Assert.assertTrue(read.getFoo() == null);
		
	}

}
