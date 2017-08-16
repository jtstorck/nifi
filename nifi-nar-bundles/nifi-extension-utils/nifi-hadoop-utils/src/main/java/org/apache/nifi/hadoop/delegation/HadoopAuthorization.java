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

import org.apache.nifi.delegation.authorization.DelegatedAuthorization;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class HadoopAuthorization implements DelegatedAuthorization {
    public static final String ORG_APACHE_HADOOP_SECURITY_USER_GROUP_INFORMATION = "org.apache.hadoop.security.UserGroupInformation";
    private final String identity;
    private final Object ugi;

//    public HadoopAuthorization(Object ugiObject) {
//        Preconditions.checkNotNull(ugi, "ugi can not be null");
//        this.ugi = ugi;
//    }

    public HadoopAuthorization(String identity, Object ugiObject) throws ClassNotFoundException {
        Class<?> ugiClass = Class.forName(ORG_APACHE_HADOOP_SECURITY_USER_GROUP_INFORMATION);
        Preconditions.checkArgument(ugiObject.getClass().getName().equals(ORG_APACHE_HADOOP_SECURITY_USER_GROUP_INFORMATION) , "ugiObject must be of type {}", ORG_APACHE_HADOOP_SECURITY_USER_GROUP_INFORMATION);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(identity), "identity can not be null or empty");
        this.identity = identity;
        this.ugi = ugiObject;
    }

    public String getIdentity() {
        return identity;
    }

    @Override
    public Object getAuthorization() {
        return ugi;
    }

}
