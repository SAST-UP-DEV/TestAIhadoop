/**
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

package org.apache.hadoop.fs.azurebfs.contract;

import org.junit.Rule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.rules.AbfsTestsRule;
import org.apache.hadoop.fs.azurebfs.rules.AbfsTestable;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.contract.AbstractContractMkdirTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * Contract test for mkdir operation.
 */
public class ITestAbfsFileSystemContractMkdir extends AbstractContractMkdirTest
    implements AbfsTestable {
  private final boolean isSecure;
  private final ABFSContractTestBinding binding;

  @Rule
  public AbfsTestsRule abfsTestsRule = new AbfsTestsRule(this);

  public ITestAbfsFileSystemContractMkdir() throws Exception {
    binding = new ABFSContractTestBinding();
    this.isSecure = binding.isSecureMode();
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    return binding.getRawConfiguration();
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new AbfsFileSystemContract(conf, isSecure);
  }

  @Override
  public void setAuthType(AuthType authType) {
    binding.setAuthType(authType);
  }

  @Override
  public Configuration getInitialConfiguration() {
    return binding.getInitialConfiguration();
  }

  @Override
  public void initFSEndpointForNewFS() throws Exception {
    binding.initFSEndpointForNewFS();
  }

}
