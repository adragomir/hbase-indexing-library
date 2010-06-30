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
package org.lilycms.util;

import java.util.Locale;
import java.util.StringTokenizer;

public class LocaleHelper {
    public static Locale parseLocale(String localeString) {
        StringTokenizer localeParser = new StringTokenizer(localeString, "-_");

        String lang = null, country = null, variant = null;

        if (localeParser.hasMoreTokens())
            lang = localeParser.nextToken();
        if (localeParser.hasMoreTokens())
            country = localeParser.nextToken();
        if (localeParser.hasMoreTokens())
            variant = localeParser.nextToken();

        if (lang != null && country != null && variant != null)
            return new Locale(lang, country, variant);
        else if (lang != null && country != null)
            return new Locale(lang, country);
        else if (lang != null)
            return new Locale(lang);
        else
            return new Locale("");
    }

    public static String getString(Locale locale) {
        return getString(locale, "_");
    }

    public static String getString(Locale locale, String separator) {
        boolean hasLanguage = !locale.getLanguage().equals("");
        boolean hasCountry = !locale.getCountry().equals("");
        boolean hasVariant = !locale.getVariant().equals("");

        if (hasLanguage && hasCountry && hasVariant)
            return locale.getLanguage() + separator + locale.getCountry() + separator + locale.getVariant();
        else if (hasLanguage && hasCountry)
            return locale.getLanguage() + separator + locale.getCountry();
        else if (hasLanguage)
            return locale.getLanguage();
        else
            return "";
    }
}
