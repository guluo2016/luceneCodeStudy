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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/** This class is passed each token produced by the analyzer
 *  on each field during indexing, and it stores these
 *  tokens in a hash table, and allocates separate byte
 *  streams per token.  Consumers of this class, eg {@link
 *  FreqProxTermsWriter} and {@link TermVectorsConsumer},
 *  write their own byte streams under each term. */
abstract class TermsHash {

  final TermsHash nextTermsHash;

  //存储的是指针，指向bytepool的起始位置
  final IntBlockPool intPool;

  /**
  针对每一个term（同一个Field），均会在bytepool中为其分配两段连续的内存区域，在lucene中称之为slice
  第一个slice用于存储term的原始数据(以ascII码方式进行存储)、term的文档id、term的词频(同一个Filed而言)
  第二个slice用于存储term的位置信息，在同一个filed中，每出现一次，均在bytepool中占用一个byte，里面存储的是位置信息左移一位后的数据
  **/
  final ByteBlockPool bytePool;

  /**
  从下面的构造方法可以看出来，termbytePool中存储的数据和bytepool中是一样的
  **/
  ByteBlockPool termBytePool;
  final Counter bytesUsed;

  final DocumentsWriterPerThread.DocState docState;

  final boolean trackAllocations;

  
  TermsHash(final DocumentsWriterPerThread docWriter, boolean trackAllocations, TermsHash nextTermsHash) {
    this.docState = docWriter.docState;
    this.trackAllocations = trackAllocations; 
    this.nextTermsHash = nextTermsHash;
    this.bytesUsed = trackAllocations ? docWriter.bytesUsed : Counter.newCounter();
    intPool = new IntBlockPool(docWriter.intBlockAllocator);
    bytePool = new ByteBlockPool(docWriter.byteBlockAllocator);

    if (nextTermsHash != null) {
      // We are primary
      termBytePool = bytePool;
      nextTermsHash.termBytePool = bytePool;
    }
  }

  public void abort() {
    try {
      reset();
    } finally {
      if (nextTermsHash != null) {
        nextTermsHash.abort();
      }
    }
  }

  // Clear all state
  void reset() {
    // we don't reuse so we drop everything and don't fill with 0
    intPool.reset(false, false); 
    bytePool.reset(false, false);
  }

  void flush(Map<String,TermsHashPerField> fieldsToFlush, final SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    if (nextTermsHash != null) {
      Map<String,TermsHashPerField> nextChildFields = new HashMap<>();
      for (final Map.Entry<String,TermsHashPerField> entry : fieldsToFlush.entrySet()) {
        nextChildFields.put(entry.getKey(), entry.getValue().nextPerField);
      }
      nextTermsHash.flush(nextChildFields, state, sortMap);
    }
  }

  abstract TermsHashPerField addField(FieldInvertState fieldInvertState, FieldInfo fieldInfo);

  void finishDocument() throws IOException {
    if (nextTermsHash != null) {
      nextTermsHash.finishDocument();
    }
  }

  void startDocument() throws IOException {
    if (nextTermsHash != null) {
      nextTermsHash.startDocument();
    }
  }
}
