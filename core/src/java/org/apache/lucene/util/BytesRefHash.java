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


import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.ByteBlockPool.DirectAllocator;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SHIFT;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

/**
 * {@link BytesRefHash} is a special purpose hash-map like data-structure
 * optimized for {@link BytesRef} instances. BytesRefHash maintains mappings of
 * byte arrays to ids (Map&lt;BytesRef,int&gt;) storing the hashed bytes
 * efficiently in continuous storage. The mapping to the id is
 * encapsulated inside {@link BytesRefHash} and is guaranteed to be increased
 * for each added {@link BytesRef}.
 * 
 * <p>
 * Note: The maximum capacity {@link BytesRef} instance passed to
 * {@link #add(BytesRef)} must not be longer than {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2. 
 * The internal storage is limited to 2GB total byte storage.
 * </p>
 * 
 * @lucene.internal
 */
/**
在这里构建一个自定义Hash表
这个Hash表中包含一个Ids的int数组和一个特殊自定义数组ByteStartArray
**/
public final class BytesRefHash {

  public static final int DEFAULT_CAPACITY = 16;

  // the following fields are needed by comparator,
  // so package private to prevent access$-methods:
  final ByteBlockPool pool;
  int[] bytesStart;

  private final BytesRef scratch1 = new BytesRef();
  private int hashSize;
  private int hashHalfSize;
  private int hashMask;
  private int count;
  private int lastCount = -1;
  private int[] ids;
  private final BytesStartArray bytesStartArray;
  private Counter bytesUsed;

  /**
   * Creates a new {@link BytesRefHash} with a {@link ByteBlockPool} using a
   * {@link DirectAllocator}.
   */
  public BytesRefHash() { 
    this(new ByteBlockPool(new DirectAllocator()));
  }
  
  /**
   * Creates a new {@link BytesRefHash}
   */
  public BytesRefHash(ByteBlockPool pool) {
    this(pool, DEFAULT_CAPACITY, new DirectBytesStartArray(DEFAULT_CAPACITY));
  }

  /**
   * Creates a new {@link BytesRefHash}
   */
  public BytesRefHash(ByteBlockPool pool, int capacity, BytesStartArray bytesStartArray) {
    hashSize = capacity;
    hashHalfSize = hashSize >> 1;
    hashMask = hashSize - 1;
    this.pool = pool;
    ids = new int[hashSize];

    //初始情况下，ids中的所有元素值均为-1
    Arrays.fill(ids, -1);
    this.bytesStartArray = bytesStartArray;
    bytesStart = bytesStartArray.init();
    bytesUsed = bytesStartArray.bytesUsed() == null? Counter.newCounter() : bytesStartArray.bytesUsed();
    bytesUsed.addAndGet(hashSize * Integer.BYTES);
  }

  /**
   * Returns the number of {@link BytesRef} values in this {@link BytesRefHash}.
   * 
   * @return the number of {@link BytesRef} values in this {@link BytesRefHash}.
   */
  public int size() {
    return count;
  }

  /**
   * Populates and returns a {@link BytesRef} with the bytes for the given
   * bytesID.
   * <p>
   * Note: the given bytesID must be a positive integer less than the current
   * size ({@link #size()})
   * 
   * @param bytesID
   *          the id
   * @param ref
   *          the {@link BytesRef} to populate
   * 
   * @return the given BytesRef instance populated with the bytes for the given
   *         bytesID
   */
  public BytesRef get(int bytesID, BytesRef ref) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID < bytesStart.length: "bytesID exceeds byteStart len: " + bytesStart.length;
    pool.setBytesRef(ref, bytesStart[bytesID]);
    return ref;
  }

  /**
   * Returns the ids array in arbitrary order. Valid ids start at offset of 0
   * and end at a limit of {@link #size()} - 1
   * <p>
   * Note: This is a destructive operation. {@link #clear()} must be called in
   * order to reuse this {@link BytesRefHash} instance.
   * </p>
   *
   * @lucene.internal
   */
  public int[] compact() {
    assert bytesStart != null : "bytesStart is null - not initialized";
    int upto = 0;
    for (int i = 0; i < hashSize; i++) {
      if (ids[i] != -1) {
        if (upto < i) {
          ids[upto] = ids[i];
          ids[i] = -1;
        }
        upto++;
      }
    }

    assert upto == count;
    lastCount = count;
    return ids;
  }

  /**
   * Returns the values array sorted by the referenced byte values.
   * <p>
   * Note: This is a destructive operation. {@link #clear()} must be called in
   * order to reuse this {@link BytesRefHash} instance.
   * </p>
   */
  public int[] sort() {
    final int[] compact = compact();
    new StringMSBRadixSorter() {

      BytesRef scratch = new BytesRef();

      @Override
      protected void swap(int i, int j) {
        int tmp = compact[i];
        compact[i] = compact[j];
        compact[j] = tmp;
      }

      @Override
      protected BytesRef get(int i) {
        pool.setBytesRef(scratch, bytesStart[compact[i]]);
        return scratch;
      }

    }.sort(0, count);
    return compact;
  }

  private boolean equals(int id, BytesRef b) {
    pool.setBytesRef(scratch1, bytesStart[id]);
    return scratch1.bytesEquals(b);
  }

  private boolean shrink(int targetSize) {
    // Cannot use ArrayUtil.shrink because we require power
    // of 2:
    int newSize = hashSize;
    while (newSize >= 8 && newSize / 4 > targetSize) {
      newSize /= 2;
    }
    if (newSize != hashSize) {
      bytesUsed.addAndGet(Integer.BYTES * -(hashSize - newSize));
      hashSize = newSize;
      ids = new int[hashSize];
      Arrays.fill(ids, -1);
      hashHalfSize = newSize / 2;
      hashMask = newSize - 1;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Clears the {@link BytesRef} which maps to the given {@link BytesRef}
   */
  public void clear(boolean resetPool) {
    lastCount = count;
    count = 0;
    if (resetPool) {
      pool.reset(false, false); // we don't need to 0-fill the buffers
    }
    bytesStart = bytesStartArray.clear();
    if (lastCount != -1 && shrink(lastCount)) {
      // shrink clears the hash entries
      return;
    }
    Arrays.fill(ids, -1);
  }

  public void clear() {
    clear(true);
  }
  
  /**
   * Closes the BytesRefHash and releases all internally used memory
   */
  public void close() {
    clear(true);
    ids = null;
    bytesUsed.addAndGet(Integer.BYTES * -hashSize);
  }

  /**
   * Adds a new {@link BytesRef}
   * 
   * @param bytes
   *          the bytes to hash
   * @return the id the given bytes are hashed if there was no mapping for the
   *         given bytes, otherwise <code>(-(id)-1)</code>. This guarantees
   *         that the return value will always be &gt;= 0 if the given bytes
   *         haven't been hashed before.
   * 
   * @throws MaxBytesLengthExceededException
   *           if the given bytes are {@code > 2 +}
   *           {@link ByteBlockPool#BYTE_BLOCK_SIZE}
   */


  /**
  BytesRefHash
  Lucene在构建Postings的时候， 采用一种类似HashMap结构，这个类HashMap的结构便是BytesRefHash,它是一个非通用的Map实现。 
  它的非通用性表现在BytesRefHash存储的键值对分别是Term和TermID，其次它并没有实现Map接口，也没有实现Map的相关操作。

  Term在Lucene中通常会被表示为BytesRef，而BytesRef的内部是一个byte[]，这是一个可以复用的对象。当通过TermID在BytesRefHash
  获取词元的时候，便将ByteBlockPool的byte[]拷贝到BytesRef的byte[]，同时指定有效长度。整个BytesRefHash生存周期中仅持有一个
  BytsRef，所以该BytesRef的byte[]长度是词元的长度。

  BytesRefHash用来存储Term和TermID之间的映射关系，如果Term已经存在，返回对应TermID；否则将Term存储并且生成TermID后返回。
  Term在存储过程BytesRefHash将BytesRef的有效数据拷贝在ByteBlockPool上，从而实现紧凑的key值存储。TermID是从0开始自增长的
  连续数值，存储在int[]上。BytesRefHash非散列哈希表，从而TermID的存储也是紧凑的。

  因为BytesRefHash为了尽可能避免用到对象类型，所以直接采用int[]存储TermID，实际上也就很难直接采用散列表的数据结构来解决
  HashCode冲突的问题。
  **/
  public int add(BytesRef bytes) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    final int length = bytes.length;

    // final position
    //获取分词的hash值
    final int hashPos = findHash(bytes);

    //ids中所有元素的初始值为-1
    //如果不是-1，那么说明对应hash表中已经有存放有元素了。需要进行冲突处理
    int e = ids[hashPos];
    
    //e为-1说明，ids中对应位置之前没有元素，新添加的是term是一个新元素
    if (e == -1) {
      // new entry
      final int len2 = 2 + bytes.length;
      if (len2 + pool.byteUpto > BYTE_BLOCK_SIZE) {
        if (len2 > BYTE_BLOCK_SIZE) {
          throw new MaxBytesLengthExceededException("bytes can be at most "
              + (BYTE_BLOCK_SIZE - 2) + " in length; got " + bytes.length);
        }
        pool.nextBuffer();
      }
      final byte[] buffer = pool.buffer;
      final int bufferUpto = pool.byteUpto;
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: "
            + bytesStart.length;
      }

      //count的初始值是0，代表这个hash表中有多少个term元素
      e = count++;

      //记录了term的开始位置
      bytesStart[e] = bufferUpto + pool.byteOffset;

      // We first encode the length, followed by the
      // bytes. Length is encoded as vInt, but will consume
      // 1 or 2 bytes at most (we reject too-long terms,
      // above).

      //terms的长度不能超过2个字节(vInt 数据位14字节，2^14-1)
      if (length < 128) {
        // 1 byte to store length
        buffer[bufferUpto] = (byte) length;
        pool.byteUpto += length + 1;
        assert length >= 0: "Length must be positive: " + length;

        //将term对应的二进制流放到buffer中
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 1,
            length);
      } else {
        // 2 byte to store length
        buffer[bufferUpto] = (byte) (0x80 | (length & 0x7f));
        buffer[bufferUpto + 1] = (byte) ((length >> 7) & 0xff);
        pool.byteUpto += length + 2;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 2,
            length);
      }
      assert ids[hashPos] == -1;
      ids[hashPos] = e;

      if (count == hashHalfSize) {
        rehash(2 * hashSize, true);
      }
      return e;
    }
    return -(e + 1);
  }
  
  /**
   * Returns the id of the given {@link BytesRef}.
   * 
   * @param bytes
   *          the bytes to look for
   * 
   * @return the id of the given bytes, or {@code -1} if there is no mapping for the
   *         given bytes.
   */
  public int find(BytesRef bytes) {
    return ids[findHash(bytes)];
  }

  /**
  指定BytesRefHash 这个Hash表的Hash算法
  **/
  private int findHash(BytesRef bytes) {
    assert bytesStart != null : "bytesStart is null - not initialized";

    //得到bytes的hash值
    int code = doHash(bytes.bytes, bytes.offset, bytes.length);

    // final position
    int hashPos = code & hashMask;
    

    //ids初始值为各个元素都为-1
    int e = ids[hashPos];


    //这里说明，ids中对应位置已经有值存在了，需要进行冲突处理
    //冲突然处理的办法就是，code后移意味，再确定位置（Hash表处理冲突的常用方法之一）
    if (e != -1 && !equals(e, bytes)) {
      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      do {
        code++;
        hashPos = code & hashMask;
        e = ids[hashPos];
      } while (e != -1 && !equals(e, bytes));
    }
    
    return hashPos;
  }

  /** Adds a "arbitrary" int offset instead of a BytesRef
   *  term.  This is used in the indexer to hold the hash for term
   *  vectors, because they do not redundantly store the byte[] term
   *  directly and instead reference the byte[] term
   *  already stored by the postings BytesRefHash.  See
   *  add(int textStart) in TermsHashPerField. */
  public int addByPoolOffset(int offset) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    // final position
    int code = offset;
    int hashPos = offset & hashMask;
    int e = ids[hashPos];
    if (e != -1 && bytesStart[e] != offset) {
      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      do {
        code++;
        hashPos = code & hashMask;
        e = ids[hashPos];
      } while (e != -1 && bytesStart[e] != offset);
    }
    if (e == -1) {
      // new entry
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: "
            + bytesStart.length;
      }
      e = count++;
      bytesStart[e] = offset;
      assert ids[hashPos] == -1;
      ids[hashPos] = e;

      if (count == hashHalfSize) {
        rehash(2 * hashSize, false);
      }
      return e;
    }
    return -(e + 1);
  }

  /**
   * Called when hash is too small ({@code > 50%} occupied) or too large ({@code < 20%}
   * occupied).
   */
  private void rehash(final int newSize, boolean hashOnData) {
    final int newMask = newSize - 1;
    bytesUsed.addAndGet(Integer.BYTES * (newSize));
    final int[] newHash = new int[newSize];
    Arrays.fill(newHash, -1);
    for (int i = 0; i < hashSize; i++) {
      final int e0 = ids[i];
      if (e0 != -1) {
        int code;
        if (hashOnData) {
          final int off = bytesStart[e0];
          final int start = off & BYTE_BLOCK_MASK;
          final byte[] bytes = pool.buffers[off >> BYTE_BLOCK_SHIFT];
          final int len;
          int pos;
          if ((bytes[start] & 0x80) == 0) {
            // length is 1 byte
            len = bytes[start];
            pos = start + 1;
          } else {
            len = (bytes[start] & 0x7f) + ((bytes[start + 1] & 0xff) << 7);
            pos = start + 2;
          }
          code = doHash(bytes, pos, len);
        } else {
          code = bytesStart[e0];
        }

        int hashPos = code & newMask;
        assert hashPos >= 0;
        if (newHash[hashPos] != -1) {
          // Conflict; use linear probe to find an open slot
          // (see LUCENE-5604):
          do {
            code++;
            hashPos = code & newMask;
          } while (newHash[hashPos] != -1);
        }
        newHash[hashPos] = e0;
      }
    }

    hashMask = newMask;
    bytesUsed.addAndGet(Integer.BYTES * (-ids.length));
    ids = newHash;
    hashSize = newSize;
    hashHalfSize = newSize / 2;
  }

  // TODO: maybe use long?  But our keys are typically short...
  private int doHash(byte[] bytes, int offset, int length) {
    return StringHelper.murmurhash3_x86_32(bytes, offset, length, StringHelper.GOOD_FAST_HASH_SEED);
  }

  /**
   * reinitializes the {@link BytesRefHash} after a previous {@link #clear()}
   * call. If {@link #clear()} has not been called previously this method has no
   * effect.
   */
  public void reinit() {
    if (bytesStart == null) {
      bytesStart = bytesStartArray.init();
    }
    
    if (ids == null) {
      ids = new int[hashSize];
      bytesUsed.addAndGet(Integer.BYTES * hashSize);
    }
  }

  /**
   * Returns the bytesStart offset into the internally used
   * {@link ByteBlockPool} for the given bytesID
   * 
   * @param bytesID
   *          the id to look up
   * @return the bytesStart offset into the internally used
   *         {@link ByteBlockPool} for the given id
   */
  public int byteStart(int bytesID) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID >= 0 && bytesID < count : bytesID;
    return bytesStart[bytesID];
  }

  /**
   * Thrown if a {@link BytesRef} exceeds the {@link BytesRefHash} limit of
   * {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2.
   */
  @SuppressWarnings("serial")
  public static class MaxBytesLengthExceededException extends RuntimeException {
    MaxBytesLengthExceededException(String message) {
      super(message);
    }
  }

  /** Manages allocation of the per-term addresses. */
  public abstract static class BytesStartArray {
    /**
     * Initializes the BytesStartArray. This call will allocate memory
     * 
     * @return the initialized bytes start array
     */
    public abstract int[] init();

    /**
     * Grows the {@link BytesStartArray}
     * 
     * @return the grown array
     */
    public abstract int[] grow();

    /**
     * clears the {@link BytesStartArray} and returns the cleared instance.
     * 
     * @return the cleared instance, this might be <code>null</code>
     */
    public abstract int[] clear();

    /**
     * A {@link Counter} reference holding the number of bytes used by this
     * {@link BytesStartArray}. The {@link BytesRefHash} uses this reference to
     * track it memory usage
     * 
     * @return a {@link AtomicLong} reference holding the number of bytes used
     *         by this {@link BytesStartArray}.
     */
    public abstract Counter bytesUsed();
  }

  /** A simple {@link BytesStartArray} that tracks
   *  memory allocation using a private {@link Counter}
   *  instance.  */
  public static class DirectBytesStartArray extends BytesStartArray {
    // TODO: can't we just merge this w/
    // TrackingDirectBytesStartArray...?  Just add a ctor
    // that makes a private bytesUsed?

    protected final int initSize;
    private int[] bytesStart;
    private final Counter bytesUsed;
    
    public DirectBytesStartArray(int initSize, Counter counter) {
      this.bytesUsed = counter;
      this.initSize = initSize;      
    }
    
    public DirectBytesStartArray(int initSize) {
      this(initSize, Counter.newCounter());
    }

    @Override
    public int[] clear() {
      return bytesStart = null;
    }

    @Override
    public int[] grow() {
      assert bytesStart != null;
      return bytesStart = ArrayUtil.grow(bytesStart, bytesStart.length + 1);
    }

    @Override
    public int[] init() {
      return bytesStart = new int[ArrayUtil.oversize(initSize, Integer.BYTES)];
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }
}
