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
package org.apache.nifi.delegation.action;

import java.util.concurrent.Callable;

import org.apache.nifi.controller.ControllerService;

/**
 * A service that accepts delegated requests to perform actions, optionally on behalf of a given identity.
 */
public interface DelegatedActionService {

    /**
     * Performs a request provided by the given callable.
     * @param callable the action to perform
     * @param <R> the type specifying the result type of the given callable
     * @return the result of the callable
     * @throws Exception
     */
    <R> R performAction(Callable<R> callable) throws Exception;

    /**
     * Performs a request provided by the given callable on behalf of an identity.
     * @param identity the identity for which the action will be performed
     * @param callable the action to perform for the given identity
     * @param <R> the type specifying the result type of the given callable
     * @return the result of the callable
     * @throws Exception
     */
    <R> R performAction(String identity, Callable<R> callable) throws Exception;

}
