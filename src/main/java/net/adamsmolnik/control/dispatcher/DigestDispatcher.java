package net.adamsmolnik.control.dispatcher;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import net.adamsmolnik.entity.EntityReference;
import net.adamsmolnik.exceptions.ServiceException;
import net.adamsmolnik.model.digest.DigestRequest;
import net.adamsmolnik.model.digest.DigestResponse;
import net.adamsmolnik.newinstance.ServerInstance;
import net.adamsmolnik.newinstance.ServerInstanceBuilder;
import net.adamsmolnik.newinstance.SetupParams;
import net.adamsmolnik.newinstance.SetupParamsView;
import net.adamsmolnik.provider.EntityProvider;
import net.adamsmolnik.sender.Sender;
import net.adamsmolnik.sender.SendingParams;
import net.adamsmolnik.setup.ServiceNameResolver;
import net.adamsmolnik.util.LocalServiceUrlCache;
import net.adamsmolnik.util.Log;

/**
 * @author ASmolnik
 *
 */
@Dependent
public class DigestDispatcher {

    @Inject
    private ServerInstanceBuilder<SetupParamsView, ServerInstance> sib;

    @Inject
    private LocalServiceUrlCache cache;

    @Inject
    private Log log;

    @Inject
    private ServiceNameResolver snr;

    @Inject
    private EntityProvider entityProvider;

    @Inject
    private Sender<DigestRequest, DigestResponse> sender;

    private final String serviceContext = "/digest-service-no-limit";

    private final String servicePath = "/ds/digest";

    private final String serviceFullPath = serviceContext + servicePath;

    private final Class<DigestResponse> responseClass = DigestResponse.class;
    
    public DigestResponse execute(DigestRequest digestRequest) {
        Map<String, String> md = entityProvider.getMetadata(new EntityReference(digestRequest.objectKey));
        long size = Long.valueOf(md.get("contentLength"));
        String cachedServiceUrl = cache.getUrl(serviceFullPath);
        if (cachedServiceUrl != null) {
            return sender.send(cachedServiceUrl, digestRequest, responseClass);
        }
        String basicServiceUrl = buildServiceUrl( "digest.adamsmolnik.com");
        if (size < 50_000_000) {
            return sender.send(basicServiceUrl, digestRequest, responseClass);
        }
        SetupParams sp = new SetupParams().withLabel("time-limited server instance for " + snr.getServiceName()).withInstanceType("t2.micro")
                .withImageId("ami-e4ba1d8c").withServiceContext(serviceContext);
        ServerInstance newInstance = sib.build(sp);
        String newServiceUrl = buildServiceUrl(newInstance.getPublicIpAddress() + ":8080");
        final DigestResponse response;
        try {
            response = sender.trySending(newServiceUrl, digestRequest, responseClass, new SendingParams().withNumberOfAttempts(3)
                    .withAttemptIntervalSecs(5).withLogExceptiomAttemptConsumer((message) -> {
                        log.err(message);
                    }));
        } catch (Exception e) {
            log.err(e);
            throw new ServiceException(e);
        }
        cache.put(serviceFullPath, newServiceUrl);
        newInstance.scheduleCleanup(10, TimeUnit.MINUTES);
        return response;
    }
    
    private String buildServiceUrl(String serverAddress) {
        return "http://" + serverAddress + serviceFullPath;
    }

}
