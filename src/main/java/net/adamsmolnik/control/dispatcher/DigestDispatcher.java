package net.adamsmolnik.control.dispatcher;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.StatusType;
import net.adamsmolnik.exceptions.ServiceException;
import net.adamsmolnik.model.digest.DigestRequest;
import net.adamsmolnik.model.digest.DigestResponse;
import net.adamsmolnik.newinstance.ServerInstance;
import net.adamsmolnik.newinstance.ServerInstanceBuilder;
import net.adamsmolnik.newinstance.SetupParams;
import net.adamsmolnik.newinstance.SetupParamsView;
import net.adamsmolnik.sender.Sender;
import net.adamsmolnik.sender.SendingParams;
import net.adamsmolnik.util.Configuration;
import net.adamsmolnik.util.LocalServiceUrlCache;
import net.adamsmolnik.util.Log;
import net.adamsmolnik.util.Util;

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
    private Configuration conf;

    @Inject
    private Sender<DigestRequest, DigestResponse> sender;

    private String serviceName;

    private String basicServerDomain;

    private final String serviceContext = "/digest-service-no-limit";

    private final String servicePath = "/ds/digest";

    private final String serviceFullPath = serviceContext + servicePath;

    private final Class<DigestResponse> responseClass = DigestResponse.class;

    private long sizeThreshold;

    @PostConstruct
    private void init() {
        serviceName = conf.getServiceName();
        basicServerDomain = conf.getServiceValue("basicServerDomain");
        sizeThreshold = Long.valueOf(conf.getServiceValue("sizeThreshold"));
    }

    public DigestResponse execute(DigestRequest digestRequest) {
        long size = fetchObjectSize(digestRequest);

        String cachedServiceUrl = cache.getUrl(serviceFullPath);
        if (cachedServiceUrl != null) {
            return sender.send(cachedServiceUrl, digestRequest, responseClass);
        }
        String basicServiceUrl = buildServiceUrl(basicServerDomain);
        if (size < sizeThreshold) {
            return sender.send(basicServiceUrl, digestRequest, responseClass);
        }
        SetupParams sp = new SetupParams().withLabel("time-limited server instance " + " (spawn by  " + Util.getLocalHost() + ") for " + serviceName)
                .withInstanceType("t2.small").withImageId("ami-7623811e").withServiceContext(serviceContext);
        try (ServerInstance newInstance = sib.build(sp)) {
            String newServiceUrl = buildServiceUrl(newInstance.getPublicIpAddress());

            DigestResponse response = sender.trySending(newServiceUrl, digestRequest, responseClass, new SendingParams().withNumberOfAttempts(3)
                    .withAttemptIntervalSecs(5).withLogExceptiomAttemptConsumer(log::err));
            cache.put(serviceFullPath, newServiceUrl);
            newInstance.scheduleCleanup(10, TimeUnit.MINUTES);
            return response;
        } catch (Exception e) {
            log.err(e);
            throw new ServiceException(e);
        }
    }

    private long fetchObjectSize(DigestRequest digestRequest) {
        Client client = ClientBuilder.newClient();
        String fetchSizeUrl;
        try {
            fetchSizeUrl = buildServiceContextUrl(basicServerDomain) + "/ds/objects/"
                    + URLEncoder.encode(digestRequest.objectKey, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException unsupportedEx) {
            throw new ServiceException(unsupportedEx);
        }
        Response sizeResponse = client.target(fetchSizeUrl).queryParam("metadata", "size").request().get();
        StatusType statusType = sizeResponse.getStatusInfo();
        if (statusType.getStatusCode() == Status.OK.getStatusCode()) {
            return Long.valueOf(sizeResponse.readEntity(String.class));
        } else {
            throw new ServiceException("Retrieving the size with url " + fetchSizeUrl + " failed with status " + statusType + " and content "
                    + sizeResponse.readEntity(String.class));
        }
    }

    private String buildServiceContextUrl(String serverAddress) {
        return "http://" + serverAddress + serviceContext;
    }

    private String buildServiceUrl(String serverAddress) {
        return "http://" + serverAddress + serviceFullPath;
    }

}
