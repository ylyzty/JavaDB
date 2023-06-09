package backend.dm.logger;

import org.junit.Test;

import java.io.File;

public class LoggerTest {

    @Test
    public void loggerTest() {
        System.out.println(System.getProperty("user.dir"));
        String filename = "./logger_test";
        Logger logger = Logger.create(filename);

        // Add log
        logger.log("aaa".getBytes());
        logger.log("bbbb".getBytes());
        logger.log("ccccc".getBytes());
        logger.close();

        logger = Logger.open(filename);
        logger.rewind();

        byte[] nextLog = null;
        while ((nextLog = logger.next()) != null) {
            System.out.println(new String(nextLog));
        }

        logger.close();
        System.out.println(new File(filename + LoggerImpl.LOG_FILE_SUFFIX).delete());
    }
}
