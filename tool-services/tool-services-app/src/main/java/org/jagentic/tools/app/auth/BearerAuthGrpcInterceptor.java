package org.jagentic.tools.app.auth;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.quarkus.grpc.GlobalInterceptor;

/** Applies {@link BearerTokenAuth} to every gRPC call via the {@code authorization} metadata key. */
@ApplicationScoped
@GlobalInterceptor
public class BearerAuthGrpcInterceptor implements ServerInterceptor {

  static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  @Inject
  BearerTokenAuth auth;

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    if (auth.accepts(headers.get(AUTHORIZATION))) {
      return next.startCall(call, headers);
    }
    call.close(Status.UNAUTHENTICATED.withDescription("missing or invalid bearer token"), new Metadata());
    return new ServerCall.Listener<>() {};
  }
}
