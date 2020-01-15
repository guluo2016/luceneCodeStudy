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
package org.apache.lucene.analysis;


import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.AttributeSource;

/** A Tokenizer is a TokenStream whose input is a Reader.
  <p>
  This is an abstract class; subclasses must override {@link #incrementToken()}
  <p>
  NOTE: Subclasses overriding {@link #incrementToken()} must
  call {@link AttributeSource#clearAttributes()} before
  setting attributes.
 */
  /**
  分词器，用于进行分词处理
  分词的具体逻辑在incrementToken(),这个方法在其父类TokenStream中定义，但是Tokenizer中并未实现，会在其子类中实现分词逻辑
  当我们自定义分词器时，需要继承Tokenizer类，并重写incrementToken()，实现自定义分词逻辑


  分词器中使用Reader来保存需要分词的流数据，保存成的是字节流input
  **/
public abstract class Tokenizer extends TokenStream {  
  /** The text source for this Tokenizer. */
  protected Reader input = ILLEGAL_STATE_READER;
  
  /** Pending reader: not actually assigned to input until reset() */
  private Reader inputPending = ILLEGAL_STATE_READER;

  /**
   * Construct a tokenizer with no input, awaiting a call to {@link #setReader(java.io.Reader)}
   * to provide input.
   */
  protected Tokenizer() {
    //
  }

  /**
   * Construct a tokenizer with no input, awaiting a call to {@link #setReader(java.io.Reader)} to
   * provide input.
   * @param factory attribute factory.
   */
  protected Tokenizer(AttributeFactory factory) {
    super(factory);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <b>NOTE:</b> 
   * The default implementation closes the input Reader, so
   * be sure to call <code>super.close()</code> when overriding this method.
   */
  @Override
  public void close() throws IOException {
    input.close();
    // LUCENE-2387: don't hold onto Reader after close, so
    // GC can reclaim
    inputPending = input = ILLEGAL_STATE_READER;
  }
  
  /** Return the corrected offset. If {@link #input} is a {@link CharFilter} subclass
   * this method calls {@link CharFilter#correctOffset}, else returns <code>currentOff</code>.
   * @param currentOff offset as seen in the output
   * @return corrected offset based on the input
   * @see CharFilter#correctOffset
   */
  protected final int correctOffset(int currentOff) {
    return (input instanceof CharFilter) ? ((CharFilter) input).correctOffset(currentOff) : currentOff;
  }

  /** Expert: Set a new reader on the Tokenizer.  Typically, an
   *  analyzer (in its tokenStream method) will use
   *  this to re-use a previously created tokenizer. */
  public final void setReader(Reader input) {
    if (input == null) {
      throw new NullPointerException("input must not be null");
    } else if (this.input != ILLEGAL_STATE_READER) {
      throw new IllegalStateException("TokenStream contract violation: close() call missing");
    }
    this.inputPending = input;
    setReaderTestPoint();
  }
  
  /**
  目的就是为了重置TokenStream流
  Tokenizer重写了reset()方法，首先调用父类的reset()方法，重置TokenStream流
  然后将Reader流赋给input，至此Tokenizer中才真正将text对应的字符流封装起来

  
  为什么会调用这个方法：
  在分词之前，这个Reader流可能会被使用，因此指向当前字符的指针也会不停的变化，当我们重新进行分词时，如果当前指针可能不是放在流的起始位置，而是可能在任意
  位置，为了避免这个问题，在分词之前首先调用一下reset()方法，从而保证这个当前指针在使用的时候是放在流的起始位置的

  这个方法总是在incrementToken()方法被调用之前，被调用。在调用这个方法之前，Reader流是暂时放在inputPending中的
  调用reset()方法之后，就开始调用incrementToken()进行分词了
  在incrementToken()内部，会通过该intput获取reader流，并给予该流进行分词
  **/
  @Override
  public void reset() throws IOException {
    super.reset();
    input = inputPending;
    inputPending = ILLEGAL_STATE_READER;
  }

  // only used for testing
  void setReaderTestPoint() {}
  
  private static final Reader ILLEGAL_STATE_READER = new Reader() {
    @Override
    public int read(char[] cbuf, int off, int len) {
      throw new IllegalStateException("TokenStream contract violation: reset()/close() call missing, " +
          "reset() called multiple times, or subclass does not call super.reset(). " +
          "Please see Javadocs of TokenStream class for more information about the correct consuming workflow.");
    }

    @Override
    public void close() {} 
  };
}

