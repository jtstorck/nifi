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
package org.apache.nifi.hadoop;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.delegation.DelegatedAction;
import org.apache.nifi.delegation.DelegatedActionService;
import org.apache.nifi.reporting.InitializationException;

import com.google.common.collect.Maps;

public class HadoopDelegatedActionService implements DelegatedActionService {

    private Map<String, IsolatedKerberosCredentials> configuredPrincipals = Maps.newConcurrentMap();

    @Override
    public <Result> Result performAction(DelegatedAction action) {
        throw new RuntimeException("Unable to perform delegated action",
                new OperationNotSupportedException(String.format("%s requires an identity for perform an action, while this attempt to perform an action was not associated with an identity.",
                        HadoopDelegatedActionService.class.getSimpleName())));
    }

    @Override
    public <Result> Result performAction(String identity, DelegatedAction action) throws IOException, InterruptedException {
        return configuredPrincipals.get(identity).getUgi().doAs((PrivilegedExceptionAction<Result>) action::run);
    }

    @Override
    public Collection<ValidationResult> validate(ValidationContext context) {
        return null;
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(String name) {
        return null;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return null;
    }

    @Override
    public String getIdentifier() {
        return null;
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

    }

    private static class IsolatedKerberosCredentials {
        private final String keytab;
        private final UserGroupInformation ugi;
        private final ClassLoader classLoader;

        public IsolatedKerberosCredentials(String keytab, UserGroupInformation ugi, ClassLoader classLoader) {
            this.keytab = keytab;
            this.ugi = ugi;
            this.classLoader = classLoader;
        }

        public String getKeytab() {
            return keytab;
        }

        public UserGroupInformation getUgi() {
            return ugi;
        }

        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }
}
