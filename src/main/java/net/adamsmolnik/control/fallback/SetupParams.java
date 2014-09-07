package net.adamsmolnik.control.fallback;

/**
 * @author ASmolnik
 *
 */
public class SetupParams implements SetupParamsView {

    private String label;

    private String instanceType;

    private String imageId;

    private String serviceContext;

    public SetupParams withLabel(String value) {
        this.label = value;
        return this;
    }

    public SetupParams withInstanceType(String value) {
        this.instanceType = value;
        return this;
    }

    public SetupParams withImageId(String value) {
        this.imageId = value;
        return this;
    }

    public SetupParams withServiceContext(String value) {
        this.serviceContext = value;
        return this;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getInstanceType() {
        return instanceType;
    }

    @Override
    public String getImageId() {
        return imageId;
    }

    @Override
    public String getServiceContext() {
        return serviceContext;
    }

}
