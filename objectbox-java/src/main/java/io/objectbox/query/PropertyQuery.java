/*
 * Copyright 2017 ObjectBox Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.objectbox.query;


import java.util.concurrent.Callable;

import io.objectbox.Property;

/**
 * Query for a specific property; create using {@link Query#property(Property)}.
 * Note: Property values do currently not consider any order defined for the main {@link Query} object
 * (subject to change in a future version).
 */
@SuppressWarnings("WeakerAccess") // WeakerAccess: allow inner class access without accessor
public class PropertyQuery {
    final Query query;
    final long queryHandle;
    final Property property;
    final int propertyId;

    boolean distinct;
    boolean noCaseIfDistinct = true;
    boolean enableNull;
    boolean unique;

    double nullValueDouble;
    float nullValueFloat;
    String nullValueString;
    long nullValueLong;

    PropertyQuery(Query query, Property property) {
        this.query = query;
        queryHandle = query.handle;
        this.property = property;
        propertyId = property.id;
    }

    /** Clears all values (e.g. distinct and null value). */
    public PropertyQuery reset() {
        distinct = false;
        noCaseIfDistinct = true;
        unique = false;
        enableNull = false;
        nullValueDouble = 0;
        nullValueFloat = 0;
        nullValueString = null;
        nullValueLong = 0;
        return this;
    }

    /**
     * Only distinct values should be returned (e.g. 1,2,3 instead of 1,1,2,3,3,3).
     * <p>
     * Note: strings default to case-insensitive comparision;
     * to change that call {@link #distinct(QueryBuilder.StringOrder)}.
     */
    public PropertyQuery distinct() {
        distinct = true;
        return this;
    }

    /**
     * For string properties you can specify {@link io.objectbox.query.QueryBuilder.StringOrder#CASE_SENSITIVE} if you
     * want to have case sensitive distinct values (e.g. returning "foo","Foo","FOO" instead of "foo").
     */
    public PropertyQuery distinct(QueryBuilder.StringOrder stringOrder) {
        if (property.type != String.class) {
            throw new RuntimeException("Reserved for string properties, but got " + property);
        }
        distinct = true;
        noCaseIfDistinct = stringOrder == QueryBuilder.StringOrder.CASE_INSENSITIVE;
        return this;
    }

    /**
     * For find methods returning single values, e.g. {@link #findInt()}, this will additional verify that the
     * resulting value is unique.
     * If there is any other resulting value resulting from this query, an exception will be thrown.
     * <p>
     * Can be combined with {@link #distinct()}.
     * <p>
     * Will be ignored for find methods returning multiple values, e.g. {@link #findInts()}.
     */
    public PropertyQuery unique() {
        unique = true;
        return this;
    }

    /**
     * By default, null values are not returned by find methods (primitive arrays cannot contains nulls).
     * However, using this function, you can define an alternative value that will be returned for null values.
     * E.g. -1 for ins/longs or "NULL" for strings.
     */
    public PropertyQuery nullValue(Object nullValue) {
        if (nullValue == null) {
            throw new IllegalArgumentException("Null values are not allowed");
        }
        boolean isString = nullValue instanceof String;
        boolean isNumber = nullValue instanceof Number;
        if (!isString && !isNumber) {
            throw new IllegalArgumentException("Unsupported value class: " + nullValue.getClass());
        }

        enableNull = true;
        nullValueString = isString ? (String) nullValue : null;
        boolean isFloat = nullValue instanceof Float;
        nullValueFloat = isFloat ? (Float) nullValue : 0;
        boolean isDouble = nullValue instanceof Double;
        nullValueDouble = isDouble ? (Double) nullValue : 0;
        nullValueLong = isNumber && !isFloat && !isDouble ? ((Number) nullValue).longValue() : 0;
        return this;
    }

    /**
     * Find the values for the given string property for objects matching the query.
     * <p>
     * Note: null values are excluded from results.
     * <p>
     * Note: results are not guaranteed to be in any particular order.
     * <p>
     * See also: {@link #distinct}, {@link #distinct(QueryBuilder.StringOrder)}
     *
     * @return Found strings
     */
    public String[] findStrings() {
        return (String[]) query.callInReadTx(new Callable<String[]>() {
            @Override
            public String[] call() {
                boolean distinctNoCase = distinct && noCaseIfDistinct;
                long cursorHandle = query.cursorHandle();
                return query.nativeFindStrings(queryHandle, cursorHandle, propertyId, distinct, distinctNoCase,
                        enableNull, nullValueString);
            }
        });
    }

    /**
     * Find the values for the given long property for objects matching the query.
     * <p>
     * Note: null values are excluded from results.
     * <p>
     * Note: results are not guaranteed to be in any particular order.
     * <p>
     * See also: {@link #distinct}
     *
     * @return Found longs
     */
    public long[] findLongs() {
        return (long[]) query.callInReadTx(new Callable<long[]>() {
            @Override
            public long[] call() {
                return query.nativeFindLongs(queryHandle, query.cursorHandle(), propertyId, distinct,
                        enableNull, nullValueLong);
            }
        });
    }

    /**
     * Find the values for the given int property for objects matching the query.
     * <p>
     * Note: null values are excluded from results.
     * <p>
     * Note: results are not guaranteed to be in any particular order.
     * <p>
     * See also: {@link #distinct}
     */
    public int[] findInts() {
        return (int[]) query.callInReadTx(new Callable<int[]>() {
            @Override
            public int[] call() {
                return query.nativeFindInts(queryHandle, query.cursorHandle(), propertyId, distinct,
                        enableNull, (int) nullValueLong);
            }
        });
    }

    /**
     * Find the values for the given int property for objects matching the query.
     * <p>
     * Note: null values are excluded from results.
     * <p>
     * Note: results are not guaranteed to be in any particular order.
     * <p>
     * See also: {@link #distinct}
     */
    public short[] findShorts() {
        return (short[]) query.callInReadTx(new Callable<short[]>() {
            @Override
            public short[] call() {
                return query.nativeFindShorts(queryHandle, query.cursorHandle(), propertyId, distinct,
                        enableNull, (short) nullValueLong);
            }
        });
    }

    /**
     * Find the values for the given int property for objects matching the query.
     * <p>
     * Note: null values are excluded from results.
     * <p>
     * Note: results are not guaranteed to be in any particular order.
     * <p>
     * See also: {@link #distinct}
     */
    public char[] findChars() {
        return (char[]) query.callInReadTx(new Callable<char[]>() {
            @Override
            public char[] call() {
                return query.nativeFindChars(queryHandle, query.cursorHandle(), propertyId, distinct,
                        enableNull, (char) nullValueLong);
            }
        });
    }

    /**
     * Find the values for the given byte property for objects matching the query.
     * <p>
     * Note: null values are excluded from results.
     * <p>
     * Note: results are not guaranteed to be in any particular order.
     */
    public byte[] findBytes() {
        return (byte[]) query.callInReadTx(new Callable<byte[]>() {
            @Override
            public byte[] call() {
                return query.nativeFindBytes(queryHandle, query.cursorHandle(), propertyId, distinct,
                        enableNull, (byte) nullValueLong);
            }
        });
    }

    /**
     * Find the values for the given int property for objects matching the query.
     * <p>
     * Note: null values are excluded from results.
     * <p>
     * Note: results are not guaranteed to be in any particular order.
     * <p>
     * See also: {@link #distinct}
     */
    public float[] findFloats() {
        return (float[]) query.callInReadTx(new Callable<float[]>() {
            @Override
            public float[] call() {
                return query.nativeFindFloats(queryHandle, query.cursorHandle(), propertyId, distinct,
                        enableNull, nullValueFloat);
            }
        });
    }

    /**
     * Find the values for the given int property for objects matching the query.
     * <p>
     * Note: null values are excluded from results.
     * <p>
     * Note: results are not guaranteed to be in any particular order.
     * <p>
     * See also: {@link #distinct}
     */
    public double[] findDoubles() {
        return (double[]) query.callInReadTx(new Callable<double[]>() {
            @Override
            public double[] call() {
                return query.nativeFindDoubles(queryHandle, query.cursorHandle(), propertyId, distinct,
                        enableNull, nullValueDouble);
            }
        });
    }

    public String findString() {
        return (String) query.callInReadTx(new Callable<String>() {
            @Override
            public String call() {
                boolean distinctCase = distinct && !noCaseIfDistinct;
                return query.nativeFindString(queryHandle, query.cursorHandle(), propertyId, unique, distinct,
                        distinctCase, enableNull, nullValueString);
            }
        });
    }

    private Object findNumber() {
        return query.callInReadTx(new Callable<Object>() {
            @Override
            public Object call() {
                return query.nativeFindNumber(queryHandle, query.cursorHandle(), propertyId, unique, distinct,
                        enableNull, nullValueLong, nullValueFloat, nullValueDouble);
            }
        });
    }

    public Long findLong() {
        return (Long) findNumber();
    }

    public Integer findInt() {
        return (Integer) findNumber();
    }

    public Short findShort() {
        return (Short) findNumber();
    }

    public Character findChar() {
        return (Character) findNumber();
    }

    public Byte findByte() {
        return (Byte) findNumber();
    }

    public Boolean findBoolean() {
        return (Boolean) findNumber();
    }

    public Float findFloat() {
        return (Float) findNumber();
    }

    public Double findDouble() {
        return (Double) findNumber();
    }


    /** Sums up all values for the given property over all Objects matching the query. */
    public long sum() {
        return (Long) query.callInReadTx(new Callable<Long>() {
            @Override
            public Long call() {
                return query.nativeSum(queryHandle, query.cursorHandle(), propertyId);
            }
        });
    }

    /** Sums up all values for the given property over all Objects matching the query. */
    public double sumDouble() {
        return (Double) query.callInReadTx(new Callable<Double>() {
            @Override
            public Double call() {
                return query.nativeSumDouble(queryHandle, query.cursorHandle(), propertyId);
            }
        });
    }

    /** Finds the maximum value for the given property over all Objects matching the query. */
    public long max() {
        return (Long) query.callInReadTx(new Callable<Long>() {
            @Override
            public Long call() {
                return query.nativeMax(queryHandle, query.cursorHandle(), propertyId);
            }
        });
    }

    /** Finds the maximum value for the given property over all Objects matching the query. */
    public double maxDouble() {
        return (Double) query.callInReadTx(new Callable<Double>() {
            @Override
            public Double call() {
                return query.nativeMaxDouble(queryHandle, query.cursorHandle(), propertyId);
            }
        });
    }

    /** Finds the minimum value for the given property over all Objects matching the query. */
    public long min() {
        return (Long) query.callInReadTx(new Callable<Long>() {
            @Override
            public Long call() {
                return query.nativeMin(queryHandle, query.cursorHandle(), propertyId);
            }
        });
    }

    /** Finds the minimum value for the given property over all Objects matching the query. */
    public double minDouble() {
        return (Double) query.callInReadTx(new Callable<Double>() {
            @Override
            public Double call() {
                return query.nativeMinDouble(queryHandle, query.cursorHandle(), propertyId);
            }
        });
    }

    /** Calculates the average of all values for the given property over all Objects matching the query. */
    public double avg() {
        return (Double) query.callInReadTx(new Callable<Double>() {
            @Override
            public Double call() {
                return query.nativeAvg(queryHandle, query.cursorHandle(), propertyId);
            }
        });
    }

}
