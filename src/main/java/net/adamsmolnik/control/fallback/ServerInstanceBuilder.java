package net.adamsmolnik.control.fallback;

/**
 * @author ASmolnik
 *
 */
public interface ServerInstanceBuilder {

    ServerInstance build(SetupParamsView spv);

}