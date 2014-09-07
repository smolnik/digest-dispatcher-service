package net.adamsmolnik.control.dispatcher;

import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import net.adamsmolnik.control.fallback.ServerInstance;
import net.adamsmolnik.control.fallback.ServerInstanceBuilder;
import net.adamsmolnik.control.fallback.SetupParams;
import net.adamsmolnik.exceptions.ServiceException;
import net.adamsmolnik.model.digest.DigestRequest;
import net.adamsmolnik.model.digest.DigestResponse;
import net.adamsmolnik.setup.ServiceNameResolver;
import net.adamsmolnik.util.Configuration;
import net.adamsmolnik.util.LocalServiceUrlCache;
import net.adamsmolnik.util.Log;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * @author ASmolnik
 *
 */
@Dependent
public class DigestDispatcher {

    @Inject
    private ServerInstanceBuilder sib;

    @Inject
    private LocalServiceUrlCache cache;

    @Inject
    private Log log;

    @Inject
    private ServiceNameResolver snr;

    @Inject
    private Configuration conf;

    private final String serviceContext = "/digest-service-no-limit";

    private final String servicePath = "/ds/digest";

    private AmazonS3Client s3;

    private String bucketName;

    @PostConstruct
    private void init() {
        bucketName = conf.getGlobalValue("bucketName");
        s3 = new AmazonS3Client();
    }

    public DigestResponse execute(DigestRequest digestRequest) {
        ObjectMetadata md = s3.getObjectMetadata(bucketName, digestRequest.objectKey);
        long size = md.getContentLength();
        String serviceFullPath = serviceContext + servicePath;
        String serviceUrl = cache.getUrl(serviceFullPath);
        if (serviceUrl != null) {
            return send(digestRequest, serviceUrl);
        }
        String url = "http://digest.adamsmolnik.com" + serviceFullPath;
        if (size < 1000000) {
            return send(digestRequest, url);
        }
        SetupParams sp = new SetupParams().withLabel("time-limited server instance for " + snr.getServiceName()).withInstanceType("t2.micro")
                .withImageId("ami-e4ba1d8c").withServiceContext(serviceContext);
        ServerInstance newInstance = sib.build(sp);
        String newServiceUrl = "http://" + newInstance.getPublicIpAddress() + ":8080" + serviceFullPath;
        final DigestResponse response;
        try {
            response = trySending(digestRequest, newServiceUrl, 3, 5);
        } catch (Exception e) {
            log.err(e);
            throw new ServiceException(e);
        }
        cache.put(serviceFullPath, newServiceUrl);
        newInstance.scheduleCleanup(10, TimeUnit.MINUTES);
        return response;
    }

    private DigestResponse trySending(DigestRequest digestRequest, String serviceUrl, int numberOfAttempts, int attemptIntervalSecs) throws Exception {
        int attemptCounter = 0;
        Exception exception = null;
        while (attemptCounter < numberOfAttempts) {
            ++attemptCounter;
            try {
                return send(digestRequest, serviceUrl);
            } catch (Exception ex) {
                if (attemptCounter == numberOfAttempts) {
                    throw ex;
                } else {
                    log.info("Attempt (" + attemptCounter + ") to send failed for url " + serviceUrl + " and request " + digestRequest
                            + " with reason: " + ex.getLocalizedMessage());
                }
                exception = ex;
            }
            TimeUnit.SECONDS.sleep(attemptIntervalSecs);
        }
        throw exception;
    }

    private DigestResponse send(DigestRequest digestRequest, String digestServiceUrl) {
        Client client = ClientBuilder.newClient();
        Entity<DigestRequest> request = Entity.json(digestRequest);
        Response response = client.target(digestServiceUrl).request().post(request);
        return response.readEntity(DigestResponse.class);
    }
}
