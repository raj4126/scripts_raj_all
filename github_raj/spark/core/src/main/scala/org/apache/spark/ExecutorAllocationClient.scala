/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

/**
 * A client that communicates with the cluster manager to request or kill executors.
 * This is currently supported only in YARN mode.
 */
private[spark] trait ExecutorAllocationClient {

  /** Get the list of currently active executors */
  private[spark] def getExecutorIds(): Seq[String]

  /**
   * Whether an executor is active. An executor is active when it can be used to execute tasks
   * for jobs submitted by the application.
   *
   * @return whether the executor with the given ID is currently active.
   */
  def isExecutorActive(id: String): Boolean

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   *
   * @param resourceProfileIdToNumExecutors The total number of executors we'd like to have per
   *                                        ResourceProfile id. The cluster manager shouldn't kill
   *                                        any running executor to reach this number, but, if all
   *                                        existing executors were to die, this is the number
   *                                        of executors we'd want to be allocated.
   * @param numLocalityAwareTasksPerResourceProfileId The number of tasks in all active stages that
   *                                                  have a locality preferences per
   *                                                  ResourceProfile id. This includes running,
   *                                                  pending, and completed tasks.
   * @param hostToLocalTaskCount A map of ResourceProfile id to a map of hosts to the number of
   *                             tasks from all active stages that would like to like to run on
   *                             that host. This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  private[spark] def requestTotalExecutors(
      resourceProfileIdToNumExecutors: Map[Int, Int],
      numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
      hostToLocalTaskCount: Map[Int, Map[String, Int]]): Boolean

  /**
   * Request an additional number of executors from the cluster manager for the default
   * ResourceProfile.
   * @return whether the request is acknowledged by the cluster manager.
   */
  def requestExecutors(numAdditionalExecutors: Int): Boolean

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * @param executorIds identifiers of executors to kill
   * @param adjustTargetNumExecutors whether the target number of executors will be adjusted down
   *                                 after these executors have been killed
   * @param countFailures if there are tasks running on the executors when they are killed, whether
    *                     to count those failures toward task failure limits
   * @param force whether to force kill busy executors, default false
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
  def killExecutors(
    executorIds: Seq[String],
    adjustTargetNumExecutors: Boolean,
    countFailures: Boolean,
    force: Boolean = false): Seq[String]

  /**
   * Request that the cluster manager kill every executor on the specified host.
   *
   * @return whether the request is acknowledged by the cluster manager.
   */
  def killExecutorsOnHost(host: String): Boolean

  /**
   * Request that the cluster manager kill the specified executor.
   * @return whether the request is acknowledged by the cluster manager.
   */
  def killExecutor(executorId: String): Boolean = {
    val killedExecutors = killExecutors(Seq(executorId), adjustTargetNumExecutors = true,
      countFailures = false)
    killedExecutors.nonEmpty && killedExecutors(0).equals(executorId)
  }
}