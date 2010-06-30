/*
 * Copyright 2010 Outerthought bvba
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
package org.lilycms.hbaseindex;

import java.util.ArrayList;
import java.util.List;

/**
 * Description of query.
 *
 * <p>A query is performed by instantiating this object, adding conditions
 * to it, and then passing it to {@link Index#performQuery}.
 *
 * <p>A query can contain equals conditions on zero or more fields,
 * and at most one range condition. The range condition should always
 * be on the last used field. A query does not need to use all fields
 * defined in the index, but you have to use them 'left to right'.
 *
 * <p>The structural validity of the query will be checked once the
 * query is supplied to {@link Index#performQuery}, not while adding
 * the individual conditions. 
 */
public class Query {
    private List<EqualsCondition> eqConditions = new ArrayList<EqualsCondition>();
    private RangeCondition rangeCondition;

    public static final Object MIN_VALUE = new Object() {
        @Override
        public String toString() {
            return "Range condition minimum value.";
        }
    };

    public static final Object MAX_VALUE = new Object() {
        @Override
        public String toString() {
            return "Range condition maximum value.";
        }
    };

    /**
     * Adds an equals condition.
     *
     * <p>The order in which the conditions are added to the query
     * does not matter.
     *
     * @param fieldName matching the name of the field in the {@link IndexDefinition}
     * @param value value of the correct type, or null
     */
    public void addEqualsCondition(String fieldName, Object value) {
        eqConditions.add(new EqualsCondition(fieldName, value));
    }

    /**
     * Sets the range condition to search on the given field for >= fromValue and
     * <= toValue. To use exclusive bounds, see the other setRangeCondition method.
     */
    public void setRangeCondition(String fieldName, Object fromValue, Object toValue) {
        rangeCondition = new RangeCondition(fieldName, fromValue, toValue, true, true);
    }

    /**
     * Sets the range condition.
     *
     * <p>The fromValue and toValue can be:
     * <ul>
     *   <li>a value of the correct type, corresponding to the index definition
     *   <li>null, which searches from or to null. Thus supplying null does NOT mean
     *       that there is no lower or upper bound on the range.
     *   <li>{@link #MIN_VALUE} or {@link #MAX_VALUE}, which are values that are
     *       smaller or larger than any other value.
     * </ul>
     * @param lowerBoundInclusive true means >= fromValue, false means > fromValue
     * @param upperBoundInclusive true means <= toValue, false means < toValue
     */
    public void setRangeCondition(String fieldName, Object fromValue, Object toValue, boolean lowerBoundInclusive,
            boolean upperBoundInclusive) {
        rangeCondition = new RangeCondition(fieldName, fromValue, toValue, lowerBoundInclusive, upperBoundInclusive);
    }

    public List<EqualsCondition> getEqConditions() {
        return eqConditions;
    }

    public EqualsCondition getCondition(String field) {
        for (EqualsCondition cond : eqConditions) {
            if (cond.name.equals(field)) {
                return cond;
            }
        }
        return null;
    }

    public RangeCondition getRangeCondition() {
        return rangeCondition;
    }

    public static class EqualsCondition {
        private String name;
        private Object value;

        public EqualsCondition(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public Object getValue() {
            return value;
        }
    }

    public static class RangeCondition {
        private String name;
        private Object fromValue;
        private Object toValue;
        private boolean lowerBoundInclusive;
        private boolean upperBoundInclusive;

        public RangeCondition(String name, Object fromValue, Object toValue, boolean lowerBoundInclusive,
                boolean upperBoundInclusive) {
            this.name = name;
            this.fromValue = fromValue;
            this.toValue = toValue;
            this.lowerBoundInclusive = lowerBoundInclusive;
            this.upperBoundInclusive = upperBoundInclusive;
        }

        public String getName() {
            return name;
        }

        public Object getFromValue() {
            return fromValue;
        }

        public Object getToValue() {
            return toValue;
        }

        public boolean isLowerBoundInclusive() {
            return lowerBoundInclusive;
        }

        public boolean isUpperBoundInclusive() {
            return upperBoundInclusive;
        }
    }
}
