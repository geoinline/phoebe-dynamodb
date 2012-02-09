/**
 * 
 */
package com.swengle.phoebe.test;

import org.junit.Before;
import org.junit.Test;

import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.query.UpdateOperations;
import com.swengle.phoebe.test.model.ObjectWithHashKey;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class TestUpdateTemp extends TestBase {
	private Phoebe phoebe = TestBase.PHOEBE;
	
	@Before
	public void setUp() {
		emptyAll();
	}

	@Test
	public void test() {
		ObjectWithHashKey objectWithHashKey = new ObjectWithHashKey();
		objectWithHashKey.setId("test_id");
		phoebe.getDatastore().put(objectWithHashKey);
		
		ObjectWithHashKey read = phoebe.getConsistentReadDatastore().get(ObjectWithHashKey.class, "test_id");
		System.out.println(read);
		
		objectWithHashKey.setFoo("foo");
		phoebe.getDatastore().put(objectWithHashKey);
		
		read = phoebe.getConsistentReadDatastore().get(ObjectWithHashKey.class, "test_id");
		System.out.println(read);
		
		UpdateOperations<ObjectWithHashKey> ops = phoebe.createUpdateOperations(ObjectWithHashKey.class);
		ops.unset("foo");
		phoebe.getDatastore().update(EntityKey.create(objectWithHashKey), ops);
		
		read = phoebe.getConsistentReadDatastore().get(ObjectWithHashKey.class, "test_id");
		System.out.println(read.getFoo());
	}

}
