package ca.uhn.fhir.jpa.starter.interceptors;

import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.jpa.api.dao.DaoRegistry;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.starter.mappers.MappingTableManager;
import ca.uhn.fhir.rest.api.server.RequestDetails;

@Component
@Interceptor
public class PullDataInterceptor {
  private MappingTableManager manager;
  private DaoRegistry daoRegistry;

  public PullDataInterceptor(MappingTableManager manager, DaoRegistry daoRegistry) {
    this.manager = manager;
    this.daoRegistry = daoRegistry;
  }

  @Hook(Pointcut.SERVER_INCOMING_REQUEST_POST_PROCESSED)
  public boolean pullDataBeforeRequest(RequestDetails details, HttpServletRequest req, HttpServletResponse resp) {
    String resourceName = details.getResourceName();

    try {
      /* 1 Pull mapping table */
      List<Pair<IBaseResource, Boolean>> data = manager.pullResource(resourceName);
      Iterator<Pair<IBaseResource, Boolean>> it = data.iterator();

      /* 2 Update Fhir Database using DAO and Mapper */
      IFhirResourceDao<IBaseResource> dao = daoRegistry.getResourceDao(resourceName);
      while (it.hasNext()) {
        Pair<IBaseResource, Boolean> pair = it.next();
        IBaseResource resource = pair.getFirst();
        Boolean deleted = pair.getSecond();

        if (deleted.booleanValue()) {
          dao.delete(resource.getIdElement());
        } else {
          dao.update(resource);
        }
      }

      if (req.getMethod().equals(HttpMethod.GET.toString())) {
        /* 3 Delete Mapping table */
        manager.deleteClean(resourceName);
      }
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }
}
