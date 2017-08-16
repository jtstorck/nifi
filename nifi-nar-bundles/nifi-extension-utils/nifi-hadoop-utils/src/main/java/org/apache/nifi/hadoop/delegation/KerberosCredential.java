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

import java.io.File;

import org.apache.nifi.delegation.authorization.DelegatedCredential;

import com.google.common.base.Preconditions;

public class KerberosCredential implements DelegatedCredential<File> {
    private final File keytab;

    public KerberosCredential(File keytab) {
        Preconditions.checkNotNull(keytab, "keytab can not be null");
        this.keytab = keytab;
    }

    @Override
    public File getCredential() {
        return keytab;
    }
}
