/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.hadoop.delegation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLConnection;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.delegation.action.DelegatedActionControllerService;
import org.apache.nifi.delegation.authorization.DelegatedAuthorizationService;
import org.apache.nifi.hadoop.KerberosTicketRenewer;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

@DynamicProperty(name = "Kerberos Principal Name",
        value = "Path to the specified Kerberos principal's keytab",
        supportsExpressionLanguage = true,
        description = "Specifies a Kerberos principal name and the path to its keytab. "
                + "If Expression Language is used, evaluation will be performed upon the controller service being enabled. "
                + "Note that no flow file input (attributes, e.g.) is available for use in Expression Language constructs for these properties.")
public class HadoopDelegatedAuthorizedActionService extends AbstractControllerService implements DelegatedActionControllerService,
        DelegatedAuthorizationService<HadoopIdentity, KerberosCredential, HadoopAuthorization, HadoopContext> {

    private Map<String, IsolatedKerberosCredential> authenticatedPrincipals = Maps.newConcurrentMap();
    private Map<String, KerberosTicketRenewer> ticketRenewers = Maps.newConcurrentMap();
    private ExecutorService ticketRenewalService = Executors.newFixedThreadPool(1);

    @Override
    public <R> R performAction(Callable<R> callable) throws UnsupportedOperationException {
        throw new UnsupportedOperationException(String.format("%s requires an identity for perform an action, while this attempt to perform an action was not associated with an identity.",
                HadoopDelegatedAuthorizedActionService.class.getSimpleName()));
    }

    @Override
    public <R> R performAction(String identity, Callable<R> callable) throws IOException, InterruptedException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException {
        IsolatedKerberosCredential isolatedKerberosCredential = authenticatedPrincipals.get(identity);
        Preconditions.checkArgument(isolatedKerberosCredential != null, "Kerberos credentials for %s have not been defined", identity);
        ClassLoader initialClassLoader = Thread.currentThread().getContextClassLoader();
        final R result;
        try {
            Thread.currentThread().setContextClassLoader(isolatedKerberosCredential.getAuthorizationClassLoader());
            result = isolatedKerberosCredential.performAction(callable);
        } finally {
            Thread.currentThread().setContextClassLoader(initialClassLoader);
        }
        return result;
    }

    @OnEnabled
    public void enableControllerService(final ConfigurationContext context) throws InitializationException, IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException {
        Preconditions.checkNotNull(context, "No ConfigurationContext has been provided");
        for (PropertyDescriptor propertyDescriptor : context.getProperties().keySet()) {
            String principal = propertyDescriptor.getName();
            if (!authenticatedPrincipals.containsKey(principal)) {
                // no IsolatedKerberosCredential objects exists for this principal
                // create a new classloader for this principal
                ClassLoader initialClassLoader = Thread.currentThread().getContextClassLoader();
                ClassLoader principalClassLoader = new HadoopDelegatedAuthorizedActionServiceClassLoader(initialClassLoader);
                final IsolatedKerberosCredential isolatedKerberosCredential;
                try {
                    // set the classloader for the current thread to the principal classloader
                    Thread.currentThread().setContextClassLoader(principalClassLoader);

                    Class<?> aClass = Class.forName("org.apache.hadoop.security.UserGroupInformation", true, principalClassLoader);
                    getLogger().info("enableControllerService, class.forname UGI classloader = " + aClass.getClassLoader());

                    // obtain a Kerberos ticket for the principal
                    // TODO Does UserGroupInformation need to use a Configuration?
                    KerberosCredential keytab = new KerberosCredential(new File(context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue()));
                    HadoopAuthorization hadoopAuthorization = requestAuthorization(new HadoopIdentity(principal), keytab, null);
                    getLogger().info("{} was loaded by classloader {}", new Object[]{hadoopAuthorization.getAuthorization().getClass(),
                            hadoopAuthorization.getAuthorization().getClass().getClassLoader()});

                    // create new IsolatedKerberosCredential for this principal
                    isolatedKerberosCredential = new IsolatedKerberosCredential(keytab, hadoopAuthorization, principalClassLoader);

                    // submit a task to renew the Kerberos ticket for this principal
                    if (ticketRenewers.containsKey(principal)) {
                        throw new IllegalStateException("Found Kerberos ticket renewer(s) configured from a previous enabling of this controller service");
                    }
                    // TODO If renewal fails, need to obtain new ticket
//                    KerberosTicketRenewer kerberosTicketRenewer = new KerberosTicketRenewer(hadoopAuthorization.getAuthorization(), 60000, getLogger());
//                    ticketRenewers.put(principal, kerberosTicketRenewer);
//                    ticketRenewalService.submit(kerberosTicketRenewer);
                } catch (IOException e) {
                    throw new InitializationException(String.format("Unable to obtain Kerberos ticket for %s", principal), e);
                } finally {
                    Thread.currentThread().setContextClassLoader(initialClassLoader);
                }
                // register the IsolatedKerberosCredential with the given principal
                authenticatedPrincipals.put(principal, isolatedKerberosCredential);
            } else {
                throw new IllegalStateException("Found Kerberos principal(s) configured from a previous enabling of this controller service");
            }
        }
    }

    @OnDisabled
    public void disableControllerService(final ConfigurationContext context) throws IOException {
        Preconditions.checkNotNull(context, "No ConfigurationContext has been provided");
        for (PropertyDescriptor propertyDescriptor : context.getProperties().keySet()) {
            String principal = propertyDescriptor.getName();
            KerberosTicketRenewer kerberosTicketRenewer = ticketRenewers.remove(principal);
            if (kerberosTicketRenewer != null) {
                kerberosTicketRenewer.stop();
            }
            authenticatedPrincipals.remove(principal);
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    @Override
    public HadoopAuthorization requestAuthorization(HadoopIdentity identity, KerberosCredential credential, HadoopContext context) throws
            IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        System.out.println("requestAuthorization for " + identity.getIdentity() + " using classloader " + Thread.currentThread().getContextClassLoader());
        Class<?> aClass = Class.forName("org.apache.hadoop.security.UserGroupInformation", true, Thread.currentThread().getContextClassLoader());
//        Class<?> aClass = Class.forName("org.apache.hadoop.security.UserGroupInformation", true, authenticatedPrincipals.get(identity.getIdentity()).getAuthorizationClassLoader());
        if (context != null) {
            Method setConfiguration = aClass.getMethod("setConfiguration");
            setConfiguration.invoke(null, context.getContext());
        }
        Method loginUserFromKeytabAndReturnUGI = aClass.getMethod("loginUserFromKeytabAndReturnUGI", String.class, String.class);
        Object invoke = loginUserFromKeytabAndReturnUGI.invoke(null, identity.getIdentity().trim(), credential.getCredential().getAbsolutePath());
        return new HadoopAuthorization(identity.getIdentity(), invoke);
    }

    @Override
    public HadoopAuthorization requestAuthorizationRenewal(HadoopAuthorization previousAuthorization, HadoopContext context) throws IOException, InterruptedException, ClassNotFoundException,
            NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> aClass = Class.forName("org.apache.hadoop.security.UserGroupInformation", true,
                authenticatedPrincipals.get(previousAuthorization.getIdentity()).getAuthorizationClassLoader());
//        Class<?> aClass = Class.forName("org.apache.hadoop.security.UserGroupInformation", true,
//                authenticatedPrincipals.get(previousAuthorization.getIdentity()).getAuthorizationClassLoader());
        Method doAs = aClass.getMethod("doAs", PrivilegedExceptionAction.class);
        Object invoke = doAs.invoke(previousAuthorization.getAuthorization(), (PrivilegedExceptionAction<Void>) () -> {
            Method checkTGTAndReloginFromKeytab = aClass.getMethod("checkTGTAndReloginFromKeytab");
            checkTGTAndReloginFromKeytab.invoke(previousAuthorization);
            return null;
        });
        return previousAuthorization;
    }

    private static class HadoopDelegatedAuthorizedActionServiceClassLoader extends ClassLoader {
        private final ClassLoader parentClassLoader;

        HadoopDelegatedAuthorizedActionServiceClassLoader(ClassLoader parentClassLoader) {
            super(parentClassLoader);
            this.parentClassLoader = parentClassLoader;
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (!name.contains("org.apache.hadoop")) {
//            if (!name.contains("org.apache.hadoop.security.UserGroupInformation")) {
                return super.loadClass(name);
            } else {
                System.out.println("custom classloading name = [" + name + "] with [" + this + "]");
                String ugiName = name.replace('.', File.separatorChar) + ".class";
                URL ugiUrl = parentClassLoader.getResource(ugiName);
                if (ugiUrl == null) {
                    throw new ClassNotFoundException("Could not find URL for " + name);
                }
                InputStream input = null;
                try {
                    URLConnection connection = ugiUrl.openConnection();
                    input = connection.getInputStream();
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int data = input.read();

                    while (data != -1) {
                        buffer.write(data);
                        data = input.read();
                    }

                    byte[] classData = buffer.toByteArray();

                    return defineClass(name, classData, 0, classData.length);
                } catch (IOException e) {
                    throw new ClassNotFoundException("Could not load class " + name, e);
                } finally {
                    if (input != null) {
                        try {
                            input.close();
                        } catch (IOException e) {
                        }
                    }
                }
            }
        }
    }
}
