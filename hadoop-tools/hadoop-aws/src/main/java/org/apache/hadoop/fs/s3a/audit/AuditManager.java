/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.audit;

import java.util.List;

import com.amazonaws.handlers.RequestHandler2;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.service.Service;

/**
 * Interface for Audit Managers.
 * The Audit Manager is the binding between S3AFS and the instantiated
 * plugin point -it adds
 * <ol>
 *   <li>per-thread tracking of audit spans </li>
 *   <li>The wiring up to the AWS SDK</li>
 *   <li>State change tracking for copy operations (does not address issue)</li>
 * </ol>
 */
@InterfaceAudience.Private
public interface AuditManager extends Service, AuditSpanSource,
    AuditSpanCallbacks {

  /**
   * Get the wrapped active span.
   * @return the currently active span.
   */
  AuditSpan getActiveThreadSpan();

  /**
   * Create the request handler(s) for this audit service.
   * The list returned is mutable; new handlers may be added.
   * @return list of handlers for the SDK.
   */
  List<RequestHandler2> createRequestHandlers();

}
