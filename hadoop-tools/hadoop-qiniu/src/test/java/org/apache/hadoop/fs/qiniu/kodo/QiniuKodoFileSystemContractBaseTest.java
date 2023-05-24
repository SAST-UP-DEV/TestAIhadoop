package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class QiniuKodoFileSystemContractBaseTest extends FileSystemContractBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoFileSystemContractBaseTest.class);

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        if (conf.getBoolean("fs.qiniu.test.useMock", true)) {
            fs = new MockQiniuKodoFileSystem();
            fs.initialize(URI.create(conf.get("fs.contract.test.fs.mockkodo")), conf);
        } else {
            fs = new QiniuKodoFileSystem();
            fs.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);
        }

        fs.delete(getTestBaseDir(), true);
    }

    @Override
    protected void rename(Path src, Path dst, boolean renameSucceeded,
                          boolean srcExists, boolean dstExists) throws IOException {
        try {
            assertEquals("Rename result", renameSucceeded, fs.rename(src, dst));
        } catch (FileAlreadyExistsException faee) {
            // 如果期望能够成功重命名，但抛出异常，那么失败
            if (renameSucceeded) {
                fail("Expected rename succeeded but " + faee);
            }
        }
        assertEquals("Source exists", srcExists, fs.exists(src));
        assertEquals("Destination exists" + dst, dstExists, fs.exists(dst));
    }
}
