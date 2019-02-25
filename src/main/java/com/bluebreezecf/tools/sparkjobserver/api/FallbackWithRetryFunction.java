package com.bluebreezecf.tools.sparkjobserver.api;
import org.javatuples.Pair;

@FunctionalInterface
public interface FallbackWithRetryFunction{
    /**
     *
     * @return pair (maxRetryTimes, retryUsingHttpUrl)
     */
      public Pair<Integer, String> getFallback();
}
