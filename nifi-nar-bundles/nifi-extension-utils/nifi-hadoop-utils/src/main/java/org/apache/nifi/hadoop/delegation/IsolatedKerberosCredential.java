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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

import org.apache.nifi.delegation.action.DelegatedActionService;

import com.google.common.base.Preconditions;

class IsolatedKerberosCredential implements DelegatedActionService {

    private final KerberosCredential kerberosCredential;
    private final HadoopAuthorization hadoopAuthorization;
    private final ClassLoader classLoader;

    public IsolatedKerberosCredential(KerberosCredential kerberosCredential, HadoopAuthorization hadoopAuthorization, ClassLoader classLoader) {
        this.kerberosCredential = Preconditions.checkNotNull(kerberosCredential, "Keytab file must not be null");
        this.hadoopAuthorization = Preconditions.checkNotNull(hadoopAuthorization, "hadoopAuthorization must not be null");
        this.classLoader = Preconditions.checkNotNull(classLoader, "classLoader must not be null");
        Preconditions.checkState(this.hadoopAuthorization.getAuthorization().getClass().getClassLoader() == classLoader,
                "classloader of hadoopAuthorization must match the classLoader argument");
//        Preconditions.checkState(this.getClass().getClassLoader() == classLoader,
//                "classloader of this class must match the classLoader argument");
    }

    public ClassLoader getAuthorizationClassLoader() {
        return hadoopAuthorization.getAuthorization().getClass().getClassLoader();
    }

    @Override
    public <R> R performAction(Callable<R> callable) throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        ClassLoader initialClassLoader = Thread.currentThread().getContextClassLoader();
        final R result;
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            Class<?> ugiClass = Class.forName(HadoopAuthorization.ORG_APACHE_HADOOP_SECURITY_USER_GROUP_INFORMATION, true, classLoader);
            Method doAs = ugiClass.getMethod("doAs", PrivilegedExceptionAction.class);
            result = (R) doAs.invoke(hadoopAuthorization.getAuthorization(), (PrivilegedExceptionAction<R>) callable::call);
        } finally {
            Thread.currentThread().setContextClassLoader(initialClassLoader);
        }
        return result;
    }

    @Override
    public <R> R performAction(String identity, Callable<R> callable) throws IOException, InterruptedException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException {
        ClassLoader initialClassLoader = Thread.currentThread().getContextClassLoader();
        final R result;
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            Class<?> ugiClass = Class.forName(HadoopAuthorization.ORG_APACHE_HADOOP_SECURITY_USER_GROUP_INFORMATION, true, classLoader);
            Method doAs = ugiClass.getMethod("doAs", PrivilegedExceptionAction.class);
            result = (R) doAs.invoke(hadoopAuthorization.getAuthorization(), (PrivilegedExceptionAction<R>) callable::call);
        } finally {
            Thread.currentThread().setContextClassLoader(initialClassLoader);
        }
        return result;
    }
}
