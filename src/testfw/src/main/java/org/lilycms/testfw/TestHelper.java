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
package org.lilycms.testfw;

import org.apache.log4j.*;
import org.apache.log4j.varia.LevelRangeFilter;

import java.io.IOException;

public class TestHelper {
    /**
     * Sets up logging such that errors are logged to the console, and info level
     * logging is sent to a file in target directory.
     */
    public static void setupLogging() throws IOException {
        final String LAYOUT = "[%t] %-5p %c - %m%n";

        Logger logger = Logger.getRootLogger();
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);

        //
        // Log to a file
        //
        FileAppender appender = new FileAppender();
        appender.setLayout(new PatternLayout(LAYOUT));

        // Maven sets a property basedir, but if the testcases are run outside Maven (e.g. by an IDE),
        // then fall back to the working directory
        String targetDir = System.getProperty("basedir");
        if (targetDir == null)
            targetDir = System.getProperty("user.dir");
        String logFileName = targetDir + "/target/log.txt";

        System.out.println("Log output will go to " + logFileName);

        appender.setFile(logFileName, false, false, 0);

        appender.activateOptions();
        logger.addAppender(appender);

        //
        // Add a console appender to show ERROR level errors on the console
        //
        ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.setLayout(new PatternLayout(LAYOUT));

        LevelRangeFilter errorFilter = new LevelRangeFilter();
        errorFilter.setAcceptOnMatch(true);
        errorFilter.setLevelMin(Level.ERROR);
        errorFilter.setLevelMax(Level.ERROR);
        consoleAppender.addFilter(errorFilter);
        consoleAppender.activateOptions();

        logger.addAppender(consoleAppender);
    }
}
