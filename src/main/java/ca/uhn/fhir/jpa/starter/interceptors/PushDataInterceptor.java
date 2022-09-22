package ca.uhn.fhir.jpa.starter.interceptors;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.stereotype.Component;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.jpa.starter.mappers.MappingTableManager;
import ca.uhn.fhir.rest.api.server.RequestDetails;

@Component
@Interceptor
public class PushDataInterceptor {
  private MappingTableManager manager;

  public PushDataInterceptor(MappingTableManager manager) {
    this.manager = manager;
  }

  @Hook(Pointcut.STORAGE_PRESTORAGE_RESOURCE_CREATED)
  public void pushCreateData(IBaseResource resource, RequestDetails details) {
    String resourceName = details.getResourceName();
    /* Push to mapping table */
    manager.pushResource(resourceName, resource, false);
    throw new AbortDatabaseOperationException();
  }

  @Hook(Pointcut.STORAGE_PRESTORAGE_RESOURCE_UPDATED)
  public void pushUpdateData(IBaseResource resource, RequestDetails details) {
    String resourceName = details.getResourceName();
    /* Push to mapping table */
    manager.pushResource(resourceName, resource, false);
    throw new AbortDatabaseOperationException();
  }

  @Hook(Pointcut.STORAGE_PRESTORAGE_RESOURCE_DELETED)
  public void pushDeleteData(IBaseResource resource, RequestDetails details) {
    String resourceName = details.getResourceName();
    /* Push to mapping table */
    manager.pushResource(resourceName, resource, true);
    throw new AbortDatabaseOperationException();
  }
}
