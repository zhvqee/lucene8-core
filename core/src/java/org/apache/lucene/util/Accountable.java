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
package org.apache.lucene.util;


import java.util.Collection;
import java.util.Collections;

/**
 * An object whose RAM usage can be computed.
 * 用于计算内存使用接口
 *
 * @lucene.internal
 */
public interface Accountable {

  /**
   * 返回内存的使用大小
   * Return the memory usage of this object in bytes. Negative values are illegal.
   */
  long ramBytesUsed();

  /**
   * 获取内嵌资源使用情况
   * Returns nested resources of this class. 
   * The result should be a point-in-time snapshot (to avoid race conditions).
   * @see Accountables
   */
  default Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

}
