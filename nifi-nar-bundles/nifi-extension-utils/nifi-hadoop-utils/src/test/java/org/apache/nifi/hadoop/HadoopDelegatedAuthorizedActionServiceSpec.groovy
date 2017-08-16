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
package org.apache.nifi.hadoop

import com.google.common.collect.Lists
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.delegation.action.DelegatedActionControllerService
import org.apache.nifi.delegation.action.DelegatedActionService
import org.apache.nifi.hadoop.delegation.HadoopDelegatedAuthorizedActionService
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.util.TestRunners
import spock.lang.Specification

import java.util.concurrent.Callable

class HadoopDelegatedAuthorizedActionServiceSpec extends Specification {
    def "PerformAction with no identity"() {
        given:
        def delegatedActionService = new HadoopDelegatedAuthorizedActionService()

        when:
        delegatedActionService.performAction((Callable) {
            null
        })

        then:
        thrown(UnsupportedOperationException)
    }

    def "PerformAction with an identity, no krb credentials defined"() {
        given:
        def delegatedActionService = new HadoopDelegatedAuthorizedActionService()

        when:
        delegatedActionService.performAction('identity', (Callable) {
            null
        })

        then:
        thrown(IllegalArgumentException)
    }

    def "PerformAction with an identity"() {
        given:
        def delegatedActionService = new HadoopDelegatedAuthorizedActionService()
        def testRunner = TestRunners.newTestRunner TestProcessor
        testRunner.addControllerService 'test-delegated-action-service', delegatedActionService, [
                identity1: '/Users/jstorck/hdf-qe/nifi_user1.headless.keytab',
                identity2: '/Users/jstorck/hdf-qe/nifi_user2.headless.keytab'
        ]

        when:
        testRunner.enableControllerService delegatedActionService

        then:
        noExceptionThrown()

        when:
        testRunner.assertValid delegatedActionService

        then:
        noExceptionThrown()

        when:
        def lookupResult = (DelegatedActionService) testRunner.processContext.controllerServiceLookup.getControllerService('test-delegated-action-service')

        then:
        lookupResult != null

        when:
        Class identity1class = lookupResult.performAction 'identity1', (Callable) {
            Class.forName 'org.apache.hadoop.security.UserGroupInformation'
        }

        then:
        noExceptionThrown()

        when:
        Class identity2class = lookupResult.performAction 'identity2', (Callable) {
            Class.forName 'org.apache.hadoop.security.UserGroupInformation'
        }

        then:
        noExceptionThrown()
        identity1class.getClassLoader() != identity2class.getClassLoader()
        identity1class.class != identity2class.class
    }

    def "EnableControllerService"() {
    }

    def "DisableControllerService"() {
    }

    def "GetSupportedDynamicPropertyDescriptor"() {
    }

    def "ProvideCredential"() {
    }

    def "UGI isolation"() {
    }

    def "TGT expires, acquire new TGT"() {
    }

    static class TestProcessor extends AbstractProcessor {

        @Override
        void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Lists.newArrayList(new PropertyDescriptor.Builder()
                    .name("HadoopDelegatedAuthorizedActionService test processor")
                    .description("HadoopDelegatedAuthorizedActionService test processor")
                    .identifiesControllerService(DelegatedActionControllerService.class)
                    .required(true)
                    .build())
        }
    }
}


