package com.swengle.phoebe.query;

import java.util.Iterator;
import java.util.List;

import com.swengle.phoebe.key.EntityKey;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public interface QueryResults<T> {
	/**
	 * <p>Execute the query and get the results as a List.  The list will be equivalent to a simple ArrayList;
	 * you can iterate through it multiple times without incurring additional datastore cost.</p>
	 *
	 * <p>Note that you must be careful about limit()ing the size of the list returned; you can
	 * easily exceed the practical memory limits of the JVM by querying for a very large dataset.</p>
	 */
    List<T> asList();
    
    /**
	 * <p>Execute a keys-only query and get the results as a List.  This is more efficient than
	 * fetching the actual results.</p>
	 *
	 * <p>The size and scope considerations of list() apply; don't fetch more data than you
	 * can fit in a simple ArrayList.</p>
	 */
    List<EntityKey<T>> asEntityKeyList();
    
    /**
	 * <p>Count the total number of values in the result.  <em>limit</em> is obeyed if applicable.</p>
	 */
	int count();
    
    /**
	 * Execute the query and get the results.
	 */
	Iterator<T> fetch(); 
    
    /**
	 * Execute the query and get the keys for the entities.
	 */
	Iterator<EntityKey<T>> fetchEntityKeys();
	
	/**
     * Gets the first entity in the result set.
	 *
	 * @return the only instance in the result, or null if the result set is empty.
     */
    T get();
	
	/**
	 * Get the entityKey of the first entity in the result set.
	 *
	 * @return the key of the first instance in the result, or null if the result set is empty.
	 */
    EntityKey<T> getEntityKey();
}
