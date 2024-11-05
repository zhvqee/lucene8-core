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
package org.apache.lucene.codecs;


import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.MathUtil;

/**
 * This abstract class writes skip lists with multiple levels.
 * 
 * <pre>
 *
 * Example for skipInterval = 3:
 *                                                     c            (skip level 2)
 *                 c                 c                 c            (skip level 1) 
 *     x     x     x     x     x     x     x     x     x     x      (skip level 0)
 * d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d  (posting list)
 *     3     6     9     12    15    18    21    24    27    30     (df)
 * 
 * d - document
 * x - skip data
 * c - skip data with child pointer
 * 
 * Skip level i contains every skipInterval-th entry from skip level i-1.
 * Therefore the number of entries on level i is: floor(df / ((skipInterval ^ (i + 1))).
 * 
 * Each skip entry on a level {@code i>0} contains a pointer to the corresponding skip entry in list i-1.
 * This guarantees a logarithmic amount of skips to find the target document.
 * 
 * While this class takes care of writing the different skip levels,
 * subclasses must define the actual format of the skip data.
 * </pre>
 * @lucene.experimental
 */

public abstract class MultiLevelSkipListWriter {

  //当前跳表的层数
  /** number of levels in this skip list */
  protected final int numberOfSkipLevels;

  //跳表间隔，0层跳表间隔
  /** the skip interval in the list with level = 0 */
  private final int skipInterval;

  //非0层间隔
  /** skipInterval used for level &gt; 0 */
  private final int skipMultiplier;

  //内存跳表
  /** for every skip level a different buffer is used  */
  private RAMOutputStream[] skipBuffer;

  /** Creates a {@code MultiLevelSkipListWriter}. */
  protected MultiLevelSkipListWriter(int skipInterval, int skipMultiplier, int maxSkipLevels, int df) {
    this.skipInterval = skipInterval;
    this.skipMultiplier = skipMultiplier;
    
    int numberOfSkipLevels;
    // calculate the maximum number of skip levels for this document frequency
    if (df <= skipInterval) {
      numberOfSkipLevels = 1;
    } else {
      numberOfSkipLevels = 1+MathUtil.log(df/skipInterval, skipMultiplier);
    }
    
    // make sure it does not exceed maxSkipLevels
    if (numberOfSkipLevels > maxSkipLevels) {
      numberOfSkipLevels = maxSkipLevels;
    }
    // 计算等到跳表的层数
    this.numberOfSkipLevels = numberOfSkipLevels;
  }
  
  /** Creates a {@code MultiLevelSkipListWriter}, where
   *  {@code skipInterval} and {@code skipMultiplier} are
   *  the same. */
  protected MultiLevelSkipListWriter(int skipInterval, int maxSkipLevels, int df) {
    this(skipInterval, skipInterval, maxSkipLevels, df);
  }

  //分配每一层的内存
  /** Allocates internal skip buffers. */
  protected void init() {
    skipBuffer = new RAMOutputStream[numberOfSkipLevels];
    for (int i = 0; i < numberOfSkipLevels; i++) {
      skipBuffer[i] = new RAMOutputStream();
    }
  }

  /** Creates new buffers or empties the existing ones */
  protected void resetSkip() {
    if (skipBuffer == null) {
      init();
    } else {
      for (int i = 0; i < skipBuffer.length; i++) {
        skipBuffer[i].reset(); // reset 输出流
      }
    }      
  }

  /**
   * Subclasses must implement the actual skip data encoding in this method.
   *  
   * @param level the level skip data shall be writing for
   * @param skipBuffer the skip buffer to write to
   */
  protected abstract void writeSkipData(int level, IndexOutput skipBuffer) throws IOException;

  // 这里输入的是df 是词频文档频次，即某个分词下有df 个文档数量
  /**
   * Writes the current skip data to the buffers. The current document frequency determines
   * the max level is skip data is to be written to. 
   * 
   * @param df the current document frequency 
   * @throws IOException If an I/O error occurs
   */
  public void bufferSkip(int df) throws IOException {

    assert df % skipInterval == 0;
    int numLevels = 1;
    df /= skipInterval; // 首先得到第0层有df 个文档，因为第0层间隔skipInterval 选一个文档
   
    // determine max level
    //计算跳表层数
    while ((df % skipMultiplier) == 0 && numLevels < numberOfSkipLevels) {
      numLevels++;
      df /= skipMultiplier;
    }
    
    long childPointer = 0;

    //从开始第0层开始写到最高层，数据
    for (int level = 0; level < numLevels; level++) {
      //第level写数据，构建第Level 层
      writeSkipData(level, skipBuffer[level]);
      
      long newChildPointer = skipBuffer[level].getFilePointer();
      
      if (level != 0) {
        // store child pointers for all levels except the lowest
        // 如果不是第一层，需要记录每一层下一次写入的childPointer
        skipBuffer[level].writeVLong(childPointer);
      }
      
      //remember the childPointer for the next level
      childPointer = newChildPointer;
    }
  }

  /**
   * 持久化 跳表到指定的output,返回跳表的文件开始位置
   *
   */
  /**
   * Writes the buffered skip lists to the given output.
   * 
   * @param output the IndexOutput the skip lists shall be written to 
   * @return the pointer the skip list starts
   */
  public long writeSkip(IndexOutput output) throws IOException {
    long skipPointer = output.getFilePointer();
    //System.out.println("skipper.writeSkip fp=" + skipPointer);
    if (skipBuffer == null || skipBuffer.length == 0) return skipPointer;

    //从最高层开始写，
    for (int level = numberOfSkipLevels - 1; level > 0; level--) {
      //length 是 第level-1层的文件的结尾
      long length = skipBuffer[level].getFilePointer();
      if (length > 0) {
        output.writeVLong(length);
        skipBuffer[level].writeTo(output);
      }
    }
    //第0层写
    skipBuffer[0].writeTo(output);
    
    return skipPointer;
  }
}
