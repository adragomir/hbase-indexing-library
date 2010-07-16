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

import java.util.Set;
import java.util.HashSet;

public class XmlMediaTypeHelper {
    private static Set xmlMediaTypes;

    static {
        xmlMediaTypes = new HashSet();
        xmlMediaTypes.add("text/xml");
        xmlMediaTypes.add("application/xml");
    }

    /**
     * Returns true if the media type is recognized as the media type of some XML format.
     */
    public static boolean isXmlMediaType(String mediaType) {
        return xmlMediaTypes.contains(mediaType) || mediaType.endsWith("+xml");
    }
}
