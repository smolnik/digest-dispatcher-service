package net.adamsmolnik.control.fallback;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import net.adamsmolnik.exceptions.ServiceException;
import net.adamsmolnik.util.Log;
import net.adamsmolnik.util.Scheduler;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.InstanceStatusSummary;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

/**
 * @author ASmolnik
 *
 */
@Dependent
public class AwsGlassfishServerInstanceBuilder implements ServerInstanceBuilder {

    private class ServerInstanceImpl implements ServerInstance {

        private final String id;

        private final String publicIpAddress;

        private ServerInstanceImpl(Instance newInstance) {
            this.id = newInstance.getInstanceId();
            this.publicIpAddress = newInstance.getPublicIpAddress();
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public String getPublicIpAddress() {
            return publicIpAddress;
        }

        @Override
        public void scheduleCleanup(int delay, TimeUnit unit) {
            AwsGlassfishServerInstanceBuilder.this.scheduleCleanup(id, delay, unit);
        }

    }

    @Inject
    private Scheduler scheduler;

    @Inject
    private Log log;

    private AmazonEC2Client ec2;

    @PostConstruct
    private void init() {
        ec2 = new AmazonEC2Client();
    }

    @Override
    public ServerInstance build(SetupParamsView spv) {
        Instance newInstanceInitiated = null;
        Instance newInstanceReady = null;
        try {
            newInstanceInitiated = setupNewInstance(spv);
            waitUntilNewInstanceGetsReady(newInstanceInitiated, 600);
            newInstanceReady = fetchInstanceDetails(newInstanceInitiated.getInstanceId());
            String newAppUrl = buildAppUrl(newInstanceReady, spv.getServiceContext());
            sendHealthCheckUntilGetsHealthy(newAppUrl);
            return new ServerInstanceImpl(newInstanceReady);
        } catch (Exception ex) {
            log(ex);
            if (newInstanceInitiated != null) {
                cleanup(newInstanceInitiated.getInstanceId());
            }
            throw new ServiceException(ex);
        }
    }

    private void sendHealthCheckUntilGetsHealthy(String newAppUrl) {
        String healthCheckUrl = newAppUrl + "/hc";
        AtomicInteger hcExceptionCounter = new AtomicInteger();
        scheduler.scheduleAndWaitFor(() -> {
            try {
                URL url = new URL(healthCheckUrl);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("GET");
                con.connect();
                int rc = con.getResponseCode();
                log.info("Healthcheck response code of " + rc + " received for " + healthCheckUrl);
                return HttpURLConnection.HTTP_OK == rc ? Optional.of(rc) : Optional.empty();
            } catch (Exception ex) {
                int c = hcExceptionCounter.incrementAndGet();
                log.err("HC attempt (" + c + ") has failed due to " + ex.getLocalizedMessage());
                log.err(ex);
                if (c > 2) {
                    throw new ServiceException(ex);
                }
                return Optional.empty();
            }
        }, 15, 300, TimeUnit.SECONDS);
    }

    private Instance fetchInstanceDetails(String instanceId) {
        return ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(instanceId)).getReservations().get(0).getInstances().get(0);
    }

    private Instance setupNewInstance(SetupParamsView spv) {
        RunInstancesRequest request = new RunInstancesRequest();
        request.withImageId(spv.getImageId())
                .withInstanceType(spv.getInstanceType())
                .withMinCount(1)
                .withMaxCount(1)
                .withKeyName("adamsmolnik-net-key-pair")
                .withSecurityGroupIds("sg-7be68f1e")
                .withSecurityGroups("adamsmolnik.com")
                .withIamInstanceProfile(
                        new IamInstanceProfileSpecification()
                                .withArn("arn:aws:iam::542175458111:instance-profile/glassfish-40-x-java8-InstanceProfile-7HFPC4EC3Z0V"));
        RunInstancesResult result = ec2.runInstances(request);
        Instance instance = result.getReservation().getInstances().get(0);

        List<Tag> tags = new ArrayList<>();
        Tag t = new Tag();
        t.setKey("Name");
        t.setValue(spv.getLabel());
        tags.add(t);
        CreateTagsRequest ctr = new CreateTagsRequest();
        ctr.setTags(tags);
        ctr.withResources(instance.getInstanceId());
        ec2.createTags(ctr);
        return instance;
    }

    private InstanceStatus waitUntilNewInstanceGetsReady(Instance instance, int timeoutSec) {
        return scheduler.scheduleAndWaitFor(() -> {
            String instanceId = instance.getInstanceId();
            List<InstanceStatus> instanceStatuses = ec2.describeInstanceStatus(new DescribeInstanceStatusRequest().withInstanceIds(instanceId))
                    .getInstanceStatuses();
            if (!instanceStatuses.isEmpty()) {
                InstanceStatus is = instanceStatuses.get(0);
                return isReady(is.getInstanceStatus(), is.getSystemStatus()) ? Optional.of(is) : Optional.empty();
            }
            return Optional.empty();
        }, 15, timeoutSec, TimeUnit.SECONDS);
    }

    private String buildAppUrl(Instance newInstance, String serviceContext) {
        return "http://" + newInstance.getPublicIpAddress() + ":8080" + serviceContext;
    }

    private void scheduleCleanup(String instanceId, int delay, TimeUnit unit) {
        scheduler.schedule(() -> {
            cleanup(instanceId);
        }, delay, unit);
    }

    private void cleanup(String instanceId) {
        ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(instanceId));
    }

    private static boolean isReady(InstanceStatusSummary isSummary, InstanceStatusSummary ssSummary) {
        return "ok".equals(isSummary.getStatus()) && "ok".equals(ssSummary.getStatus());
    }

    private void log(Exception ex) {
        ex.printStackTrace();
    }

}
