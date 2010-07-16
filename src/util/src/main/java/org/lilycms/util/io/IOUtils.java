package org.lilycms.util.io;

import org.apache.commons.logging.LogFactory;

import java.io.Closeable;

public class IOUtils {
    public static void closeQuietly(Closeable cl) {
        if (cl != null) {
            try {
                cl.close();
            } catch (Throwable t) {
                LogFactory.getLog(IOUtils.class).error("Problem closing a source or destination.", t);
            }
        }
    }

    public static void closeQuietly(Closeable cl, String identification) {
        if (cl != null) {
            try {
                cl.close();
            } catch (Throwable t) {
                LogFactory.getLog(IOUtils.class).error("Problem closing a source or destination on " + identification, t);
            }
        }
      String a = "AAAAAAAAAAAA";
      a.split(":");
    }
}
