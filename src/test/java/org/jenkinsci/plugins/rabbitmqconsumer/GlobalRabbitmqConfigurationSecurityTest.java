package org.jenkinsci.plugins.rabbitmqconsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.gargoylesoftware.htmlunit.HttpMethod;
import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.util.NameValuePair;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import hudson.security.ACL;
import hudson.security.AuthorizationStrategy;
import hudson.security.Permission;
import hudson.security.csrf.DefaultCrumbIssuer;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import jenkins.model.Jenkins;
import mockit.Mock;
import mockit.MockUp;
import net.sf.json.JSONObject;
import org.acegisecurity.Authentication;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.For;
import org.jvnet.hudson.test.Issue;
import org.jvnet.hudson.test.JenkinsRule;

@For(GlobalRabbitmqConfiguration.class)
@Issue("SECURITY-2778")
public class GlobalRabbitmqConfigurationSecurityTest {

    private static final String ENDPOINT = "descriptorByName/"
            + GlobalRabbitmqConfiguration.class.getName() + "/testConnection";
    private static final String SERVICE_URI = "amqp://127.0.0.1:5672/%2F";

    @Rule
    public JenkinsRule j = new JenkinsRule();

    private final AtomicInteger connectionAttempts = new AtomicInteger();

    @Before
    public void setUp() throws Exception {
        j.jenkins.setSecurityRealm(j.createDummySecurityRealm());
        j.jenkins.setAuthorizationStrategy(new AuthorizationStrategy() {
            private final ACL acl = new ACL() {
                @Override
                public boolean hasPermission(Authentication authentication, Permission permission) {
                    return "admin".equals(authentication.getName()) || permission == Jenkins.READ;
                }
            };

            @Override
            public ACL getRootACL() {
                return acl;
            }

            @Override
            public Collection<String> getGroups() {
                return Collections.emptyList();
            }
        });
        j.jenkins.setCrumbIssuer(new DefaultCrumbIssuer(false));
        new MockUp<ConnectionFactory>() {
            @Mock
            public Connection newConnection() {
                connectionAttempts.incrementAndGet();
                return null;
            }
        };
    }

    @Test
    public void getRequiresPostBeforeConnecting() throws Exception {
        JenkinsRule.WebClient admin = client("admin");
        assertEquals(405, request(admin, HttpMethod.GET, false, SERVICE_URI).getWebResponse().getStatusCode());
        assertEquals(0, connectionAttempts.get());
    }

    @Test
    public void postRequiresAdministratorBeforeValidationOrConnecting() throws Exception {
        JenkinsRule.WebClient reader = client("reader");
        assertEquals(403, request(reader, HttpMethod.POST, true, SERVICE_URI).getWebResponse().getStatusCode());
        assertEquals(403, request(reader, HttpMethod.POST, true, "not-an-amqp-uri").getWebResponse().getStatusCode());
        assertEquals(0, connectionAttempts.get());
    }

    @Test
    public void administratorPostRequiresCrumbBeforeConnecting() throws Exception {
        JenkinsRule.WebClient admin = client("admin");
        assertEquals(403, request(admin, HttpMethod.POST, false, SERVICE_URI).getWebResponse().getStatusCode());
        assertEquals(0, connectionAttempts.get());
    }

    @Test
    public void administratorPostWithCrumbCanTestConnection() throws Exception {
        JenkinsRule.WebClient admin = client("admin");
        Page response = request(admin, HttpMethod.POST, true, SERVICE_URI);
        assertEquals(200, response.getWebResponse().getStatusCode());
        assertTrue(response.getWebResponse().getContentAsString().contains(Messages.Success()));
        assertEquals(1, connectionAttempts.get());
    }

    private JenkinsRule.WebClient client(String user) throws Exception {
        JenkinsRule.WebClient client = j.createWebClient().login(user);
        client.getOptions().setThrowExceptionOnFailingStatusCode(false);
        return client;
    }

    private Page request(JenkinsRule.WebClient client, HttpMethod method, boolean withCrumb,
            String serviceUri) throws Exception {
        WebRequest request = new WebRequest(new URL(j.getURL(), ENDPOINT), method);
        request.setRequestParameters(Arrays.asList(new NameValuePair("serviceUri", serviceUri),
                new NameValuePair("userName", "fixture-user"), new NameValuePair("userPassword", "fixture-password")));
        if (withCrumb) {
            Page response = client.getPage(new URL(j.getURL(), "crumbIssuer/api/json"));
            assertEquals(200, response.getWebResponse().getStatusCode());
            JSONObject crumb = JSONObject.fromObject(response.getWebResponse().getContentAsString());
            request.setAdditionalHeader(crumb.getString("crumbRequestField"), crumb.getString("crumb"));
        }
        return client.getPage(request);
    }
}
