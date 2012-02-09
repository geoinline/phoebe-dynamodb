package com.swengle.phoebe.query;

import java.util.Collection;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public interface Query<T> extends QueryResults<T> {
    /** Limits the fields retrieved */
    Query<T> retrievedFields(Collection<String> fields);
    /** Limits the fields retrieved */
    Query<T> retrievedFields(String...fields);
    /** Get the kind class */
    Class<T> getKindClass();
}
