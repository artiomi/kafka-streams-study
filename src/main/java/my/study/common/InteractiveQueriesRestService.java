package my.study.common;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

@Slf4j
@Path("state")
public class InteractiveQueriesRestService {

  private final KafkaStreams streams;
  private final HostInfo hostInfo;
  private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
  private Server jettyServer;

  public InteractiveQueriesRestService(final KafkaStreams streams,
      final HostInfo hostInfo) {
    this.streams = streams;
    this.hostInfo = hostInfo;

  }

  @GET
  @Path("/keyvalue/{storeName}/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public long byKey(@PathParam("storeName") final String storeName,
      @PathParam("key") final String key) {
    ReadOnlyKeyValueStore<String, Long> store = streams.store(
        StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

    return store.get(key);
  }

  public void start(final int port) {
    final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");

    jettyServer = new Server();
    jettyServer.setHandler(context);

    final ResourceConfig rc = new ResourceConfig();
    rc.register(this);
    rc.register(JacksonFeature.class);

    final ServletContainer sc = new ServletContainer(rc);
    final ServletHolder holder = new ServletHolder(sc);
    context.addServlet(holder, "/*");

    final ServerConnector connector = new ServerConnector(jettyServer);
    connector.setHost(hostInfo.host());
    connector.setPort(port);
    jettyServer.addConnector(connector);

    try {
      context.start();
      jettyServer.start();
    } catch (final Exception exception) {
      log.error("Unavailable: " + hostInfo.host() + ":" + hostInfo.port());
      throw new RuntimeException(exception.toString());
    }
  }

  /**
   * Stop the Jetty Server
   *
   * @throws Exception if jetty can't stop
   */
  public void stop() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
  }

}
