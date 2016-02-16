package org.aksw.gerbil.dataset.check;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.aksw.gerbil.dataset.check.impl.FileBasedCachingEntityCheckerManager;
import org.aksw.gerbil.transfer.nif.data.Annotation;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FileBasedCachingEntityCheckerManagerImplTest implements EntityChecker {

    private static final String CORRECT_URI = "http://aksw.org/correct";
    private static final String WRONG_URI = "http://aksw.org/wrong";
    private static final long CACHE_DURATION = 2000;

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> testConfigs = new ArrayList<Object[]>();
        testConfigs.add(new Object[] { new String[] { CORRECT_URI }, new boolean[] { true } });
        testConfigs.add(new Object[] { new String[] { WRONG_URI }, new boolean[] { false } });
        testConfigs.add(new Object[] { new String[] { CORRECT_URI, WRONG_URI }, new boolean[] { true, false } });
        return testConfigs;
    }

    private String uris[];
    private boolean expectingSameUri[];
    private boolean expectCall;

    public FileBasedCachingEntityCheckerManagerImplTest(String uris[], boolean expectingSameUri[]) {
        this.uris = uris;
        this.expectingSameUri = expectingSameUri;
    }

    @Test
    public void test() throws IOException, InterruptedException {
        File cacheFile = File.createTempFile("temp_", ".cache");
        FileBasedCachingEntityCheckerManager manager = FileBasedCachingEntityCheckerManager.create(CACHE_DURATION,
                cacheFile);
        manager.registerEntityChecker("http://aksw.org/", this);

        expectCall = true;
        runSingleTest(manager);

        expectCall = false;
        runSingleTest(manager);

        Thread.sleep(CACHE_DURATION);

        expectCall = true;
        runSingleTest(manager);
        manager.storeCache();

        FileBasedCachingEntityCheckerManager manager2 = FileBasedCachingEntityCheckerManager.create(CACHE_DURATION,
                cacheFile);
        expectCall = false;
        runSingleTest(manager2);

    }

    private void runSingleTest(EntityCheckerManager manager) {
        Annotation annotation = new Annotation(new HashSet<String>(Arrays.asList(uris)));
        manager.checkMeanings(Arrays.asList(annotation));
        for (int i = 0; i < uris.length; ++i) {
            if (expectingSameUri[i]) {
                Assert.assertTrue("Expected the URI to be there after the check.", annotation.containsUri(uris[i]));
            } else {
                Assert.assertFalse("Expected the URI to be changed during the check.", annotation.containsUri(uris[i]));
            }
        }
    }

    @Override
    public boolean entityExists(String uri) {
        Assert.assertTrue("Didn't expected to be called.", expectCall);
        return uri.equals(CORRECT_URI);
    }
}
