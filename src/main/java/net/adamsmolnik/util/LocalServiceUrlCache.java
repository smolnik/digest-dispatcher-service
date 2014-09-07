package net.adamsmolnik.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Singleton;

/**
 * @author ASmolnik
 *
 */
@Singleton
public class LocalServiceUrlCache {

    private ConcurrentMap<String, String> urlMap = new ConcurrentHashMap<>();

    public void put(String serviceFullPath, String url) {
        urlMap.put(serviceFullPath, url);
    }

    public String getUrl(String serviceFullPath) {
        return urlMap.get(serviceFullPath);
    }

    public void remove(String serviceFullPath) {
        urlMap.remove(serviceFullPath);
    }

    @Override
    public String toString() {
        return "LocalServiceUrlCache [urlMap=" + urlMap + "]";
    }
}
