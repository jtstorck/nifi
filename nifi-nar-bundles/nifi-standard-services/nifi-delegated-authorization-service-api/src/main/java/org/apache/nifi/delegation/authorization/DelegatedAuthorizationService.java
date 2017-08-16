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
package org.apache.nifi.delegation.authorization;

import java.io.IOException;

/**
 * A service that accepts delegated requests to authorize an identity, given credentials and context for the authorization request.
 *
 * @param <I>   The type specifying the identity
 * @param <C>   The type specifying the credentials to obtain authorization
 * @param <A>   The type specifying the authorization to return
 * @param <CTX> The type specifying the context necessary for this authorization
 */
public interface DelegatedAuthorizationService<I extends DelegatedIdentity, C extends DelegatedCredential, A extends DelegatedAuthorization, CTX extends DelegatedContext> {

    /**
     * Attempts to authorize an identity.
     *
     * @param identity   The identity to authorize
     * @param credential The credential used to authorize the identity
     * @param context    The context to supply with the authorization request
     * @return The authorization result
     * @throws IOException
     */
    A requestAuthorization(I identity, C credential, CTX context) throws Exception;

    A requestAuthorizationRenewal(A previousAuthorization, CTX context) throws Exception;

}
