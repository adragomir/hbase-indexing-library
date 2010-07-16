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
package org.lilycms.util.xml;

import javax.xml.xpath.XPathExpressionException;

/**
 * Compiled XPathExpressions are not thread-safe (and not re-entrant either).
 * This class helps by managing a thread-based cache of an XPath expression.
 * Instances of this class should typically be stored in static fields.
 */
public class LocalXPathExpression {
    private String expression;

    private ThreadLocal<HXPathExpression> LOCAL = new ThreadLocal<HXPathExpression>() {
        protected HXPathExpression initialValue() {
            HXPathExpression expr;
            try {
                expr = new HXPathExpression(LocalXPathFactory.newXPath().compile(expression));
            } catch (XPathExpressionException e) {
                throw new RuntimeException(e);
            }
            return expr;
        }
    };

    public LocalXPathExpression(String expression) {
        this.expression = expression;
    }

    public HXPathExpression get() {
        return LOCAL.get();
    }
}
