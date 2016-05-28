/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.util;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;



/**
 * Light-weight object pool based on a thread-local stack.
 *
 * @param <T>
 *            the type of the pooled object
 */
public abstract class Recycler<T> {

	private static final InternalLogger logger = InternalLoggerFactory.getInstance(Recycler.class);

	@SuppressWarnings("rawtypes")
	private static final Handle NOOP_HANDLE = new Handle() {
		@Override
		public void recycle(Object object) {
			// NOOP
		}
	};
	private static final AtomicInteger ID_GENERATOR = new AtomicInteger(Integer.MIN_VALUE);
	private static final int OWN_THREAD_ID = ID_GENERATOR.getAndIncrement();
	// TODO: Some arbitrary large number - should adjust as we get more
	// production experience.
	private static final int DEFAULT_INITIAL_MAX_CAPACITY = 262144;
	private static final int DEFAULT_MAX_CAPACITY;
	private static final int INITIAL_CAPACITY;

	static {
		// In the future, we might have different maxCapacity for different
		// object types.
		// e.g. io.netty.recycler.maxCapacity.writeTask
		// io.netty.recycler.maxCapacity.outboundBuffer
		//栈的大小是个随意值，以后根据工程经验设置更合适的大小
		int maxCapacity = SystemPropertyUtil.getInt("io.netty.recycler.maxCapacity", DEFAULT_INITIAL_MAX_CAPACITY);
		if (maxCapacity < 0) {
			// TODO: Some arbitrary large number - should adjust as we get more
			// production experience.
			maxCapacity = 262144;
		}

		DEFAULT_MAX_CAPACITY = maxCapacity;
		if (logger.isDebugEnabled()) {
			if (DEFAULT_MAX_CAPACITY == 0) {
				logger.debug("-Dio.netty.recycler.maxCapacity: disabled");
			} else {
				logger.debug("-Dio.netty.recycler.maxCapacity: {}", DEFAULT_MAX_CAPACITY);
			}
		}

		INITIAL_CAPACITY = Math.min(DEFAULT_MAX_CAPACITY, 256);
	}

	private final int maxCapacity;
	private final FastThreadLocal<Stack<T>> threadLocal = new FastThreadLocal<Stack<T>>() {
		@Override
		protected Stack<T> initialValue() {
			return new Stack<T>(Recycler.this, Thread.currentThread(), maxCapacity);
		}
	};

	protected Recycler() {
		this(DEFAULT_MAX_CAPACITY);
	}

	protected Recycler(int maxCapacity) {
		this.maxCapacity = Math.max(0, maxCapacity);
	}

	@SuppressWarnings("unchecked")
	public final T get() {
		if (maxCapacity == 0) {
			return newObject((Handle<T>) NOOP_HANDLE);
		}
		Stack<T> stack = threadLocal.get();
		DefaultHandle<T> handle = stack.pop();
		if (handle == null) {//如果没有从私有栈里找到可用的对象，直接创建一个新对象
			handle = stack.newHandle();
			handle.value = newObject(handle);
		}
		return (T) handle.value;
	}

	public final boolean recycle(T o, Handle<T> handle) {
		if (handle == NOOP_HANDLE) {
			return false;
		}

		DefaultHandle<T> h = (DefaultHandle<T>) handle;
		if (h.stack.parent != this) {
			return false;
		}

		h.recycle(o);
		return true;
	}

	final int threadLocalCapacity() {
		return threadLocal.get().elements.length;
	}

	final int threadLocalSize() {
		return threadLocal.get().size;
	}

	protected abstract T newObject(Handle<T> handle);

	public interface Handle<T> {
		void recycle(T object);
	}

	static final class DefaultHandle<T> implements Handle<T> {
		private int lastRecycledId;//这里不是violate，所以有可见性问题
		private int recycleId;

		private Stack<?> stack;
		private Object value;

		DefaultHandle(Stack<?> stack) {
			this.stack = stack;
		}

		@Override
		public void recycle(Object object) {
			if (object != value) {
				throw new IllegalArgumentException("object does not belong to handle");
			}
			Thread thread = Thread.currentThread();
			if (thread == stack.thread) {//如果式属于当前线程的，直接放在这个线程的Stack中
				stack.push(this);
				return;
			}
			// we don't want to have a ref to the queue as the value in our weak
			// map
			// so we null it out; to ensure there are no races with restoring it
			// later
			// we impose a memory ordering here (no-op on x86)
			/**
			 * 重新循环使用一个buffer对象的时候，如果这个buffer对象不是从当前线程Stack<T>里获取的，
			 * 需要把这个buffer返回给分配这个buffer对象的线程Stack<T>.
			 */
			Map<Stack<?>, WeakOrderQueue> delayedRecycled = DELAYED_RECYCLED.get();
			WeakOrderQueue queue = delayedRecycled.get(stack);
			if (queue == null) {
				delayedRecycled.put(stack, queue = new WeakOrderQueue(stack, thread));
			}
			queue.add(this);
		}
	}

	/**
	 * 维护了一个线程的Stack<T>对象为Key的资源链表，这个资源链表中的对象是从这个Stack<T>里分配的，主要用来把不属于自己的资源归还原主。
	 */
	private static final FastThreadLocal<Map<Stack<?>, WeakOrderQueue>> DELAYED_RECYCLED = new FastThreadLocal<Map<Stack<?>, WeakOrderQueue>>() {
		@Override
		protected Map<Stack<?>, WeakOrderQueue> initialValue() {
			return new WeakHashMap<Stack<?>, WeakOrderQueue>();// WeakHashMap的Entry继承自WeakReference并且把key和内部的ReferenceQueue注册在一起，当某个线程销毁的时候，相应的Stack<T>对象会被销毁，并且从其他线程的map中移除引用防止内存泄漏。
		}
	};

	// a queue that makes only moderate guarantees about visibility: items are
	// seen in the correct order,
	// but we aren't absolutely guaranteed to ever see anything at all, thereby
	// keeping the queue cheap to maintain
	private static final class WeakOrderQueue {
		private static final int LINK_CAPACITY = 16;

		// Let Link extend AtomicInteger for intrinsics. The Link itself will be
		// used as writerIndex.
		@SuppressWarnings("serial")
		private static final class Link extends AtomicInteger {
			private final DefaultHandle<?>[] elements = new DefaultHandle[LINK_CAPACITY];
			private int readIndex;
			private Link next;
		}

		// chain of data items
		private Link head, tail;
		// pointer to another queue of delayed items for the same stack
		private WeakOrderQueue next;
		private final WeakReference<Thread> owner;//这个引用不会防止线程的对象的回收
		private final int id = ID_GENERATOR.getAndIncrement();//queue的id

		WeakOrderQueue(Stack<?> stack, Thread thread) {
			head = tail = new Link();
			owner = new WeakReference<Thread>(thread);
			synchronized (stack) {//将队列放入Stack<T>对象的WeakOrderQueue链表中，利于拥有Stack<T>的线程获取不到T时候，从这个链表中获取，这个是其他线程使用完归还的。
				next = stack.head;
				stack.head = this;
			}
		}
		/**
		 * 将从别的线程Stack<T>中获取的T对象返还给那个线程的Stack<T>中
		 * @param handle
		 */
		void add(DefaultHandle<?> handle) {
			handle.lastRecycledId = id;//每个handler都放在一个指定的queue里，这个queue有一个确定的id。

			Link tail = this.tail;
			int writeIndex;
			if ((writeIndex = tail.get()) == LINK_CAPACITY) {//如果尾部的Link中的elements是满的，就在后面继续追加一个Link，然后把writeIndex置为0
				this.tail = tail = tail.next = new Link();//链接到后面
				writeIndex = tail.get();//获取新创建的Link的值（0）
			}
			tail.elements[writeIndex] = handle;//将对象放入到槽里
			handle.stack = null;//归还的时候将stack置空
			// we lazy set to ensure that setting stack to null appears before
			// we unnull it in the owning thread;
			// this also means we guarantee visibility of an element in the
			// queue if we see the index updated
			tail.lazySet(writeIndex + 1);//增加writeIndex
		}
		/**
		 * 判断是不是还有可用的对象。
		 * @return
		 */
		boolean hasFinalData() {
			return tail.readIndex != tail.get();
		}
		
		// transfer as many items as we can from this queue to the stack,
		// returning true if any were transferred
		/**
		 * 每次只收集一个Link的对象，如果要多次，需要循环调用
		 * @param dst
		 * @return
		 */
		@SuppressWarnings("rawtypes")
		boolean transfer(Stack<?> dst) {

			Link head = this.head;
			if (head == null) {
				return false;
			}

			if (head.readIndex == LINK_CAPACITY) {//如果head已经使用完毕，继续下一个
				if (head.next == null) {
					return false;
				}
				this.head = head = head.next;//将没有可用的Link 从链表中删除，继续下一个
			}

			final int srcStart = head.readIndex;
			int srcEnd = head.get();
			final int srcSize = srcEnd - srcStart;
			if (srcSize == 0) {//如果没有可用的对象，直接返回
				return false;
			}

			final int dstSize = dst.size;
			final int expectedCapacity = dstSize + srcSize;//回收之后的栈容量

			if (expectedCapacity > dst.elements.length) {//如果容量大与最大容量的话，扩大栈空间
				final int actualCapacity = dst.increaseCapacity(expectedCapacity);
				srcEnd = Math.min(srcStart + actualCapacity - dstSize, srcEnd);//如果到达最大栈容量还不够装入的话，直接返回还可以放入的数量，如果可以放入的话，直接返回这个Link的writeIndex
			}

			if (srcStart != srcEnd) {
				final DefaultHandle[] srcElems = head.elements;
				final DefaultHandle[] dstElems = dst.elements;
				int newDstSize = dstSize;//Stack的递增索引
				for (int i = srcStart; i < srcEnd; i++) {//Link可用的对象索引
					DefaultHandle element = srcElems[i];
					if (element.recycleId == 0) {
						element.recycleId = element.lastRecycledId;//第一次传送的时候，recycleId＝0，如果中间被其他线程操作，或者多次回收，导致这俩值不统一
					} else if (element.recycleId != element.lastRecycledId) {
						throw new IllegalStateException("recycled already");
					}
					element.stack = dst;
					dstElems[newDstSize++] = element;
					srcElems[i] = null;
				}
				dst.size = newDstSize;

				if (srcEnd == LINK_CAPACITY && head.next != null) {
					this.head = head.next;
				}

				head.readIndex = srcEnd;
				return true;
			} else {
				// The destination stack is full already.
				return false;
			}
		}
	}

	// 线程私有的，栈式维护对象池
	static final class Stack<T> {

		// we keep a queue of per-thread queues, which is appended to once only,
		// each time a new thread other
		// than the stack owner recycles: when we run out of items in our stack
		// we iterate this collection
		// to scavenge those that can be reused. this permits us to incur
		// minimal thread synchronisation whilst
		// still recycling all items.
		final Recycler<T> parent;//某个循环对象
		final Thread thread;
		private DefaultHandle<?>[] elements;
		private final int maxCapacity;
		private int size;//表示可用的对象数量

		private volatile WeakOrderQueue head;
		private WeakOrderQueue cursor, prev;

		Stack(Recycler<T> parent, Thread thread, int maxCapacity) {
			this.parent = parent;
			this.thread = thread;
			this.maxCapacity = maxCapacity;
			elements = new DefaultHandle[Math.min(INITIAL_CAPACITY, maxCapacity)];
		}

		int increaseCapacity(int expectedCapacity) {
			int newCapacity = elements.length;
			int maxCapacity = this.maxCapacity;
			do {
				newCapacity <<= 1;
			} while (newCapacity < expectedCapacity && newCapacity < maxCapacity);//超过最大值或者满足期望大小时退出

			newCapacity = Math.min(newCapacity, maxCapacity);//这里保证扩展的容量不会大与最大值ß
			if (newCapacity != elements.length) {
				elements = Arrays.copyOf(elements, newCapacity);
			}

			return newCapacity;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		DefaultHandle<T> pop() {
			int size = this.size;
			if (size == 0) {//如果没有可用的对象时，看一看是不是有其他线程帮忙释放里一些可用对象
				if (!scavenge()) {
					return null;
				}
				size = this.size;
			}
			size--;
			DefaultHandle ret = elements[size];
			if (ret.lastRecycledId != ret.recycleId) {//增加元素有两个途径，一个时push进来，另一个时通过WeakOrderQueue scavenge过来，
				throw new IllegalStateException("recycled multiple times");
			}
			ret.recycleId = 0;
			ret.lastRecycledId = 0;
			this.size = size;
			return ret;
		}

		boolean scavenge() {
			// continue an existing scavenge, if any
			if (scavengeSome()) {
				return true;
			}

			// reset our scavenge cursor
			prev = null;
			cursor = head;
			return false;
		}
		
		boolean scavengeSome() {
			WeakOrderQueue cursor = this.cursor;
			if (cursor == null) {
				cursor = head;
				if (cursor == null) {
					return false;
				}
			}

			boolean success = false;
			WeakOrderQueue prev = this.prev;
			do {
				if (cursor.transfer(this)) {
					success = true;
					break;
				}

				WeakOrderQueue next = cursor.next;
				if (cursor.owner.get() == null) {//线程被gc了
					// If the thread associated with the queue is gone, unlink
					// it, after
					// performing a volatile read to confirm there is no data
					// left to collect.
					// We never unlink the first queue, as we don't want to
					// synchronize on updating the head.
					if (cursor.hasFinalData()) {
						for (;;) {
							if (cursor.transfer(this)) {
								success = true;
							} else {
								break;
							}
						}
					}
					if (prev != null) {
						prev.next = next;//将对象全部取出之后，将这个WeakOrderQueue从当前Stack的链接中删除。
					}
				} else {
					prev = cursor;
				}

				cursor = next;

			} while (cursor != null && !success);

			this.prev = prev;
			this.cursor = cursor;
			return success;
		}

		void push(DefaultHandle<?> item) {
			if ((item.recycleId | item.lastRecycledId) != 0) {//pop出去之后这俩值都是0，这个时候如果不是0，肯定时多次回收里
				throw new IllegalStateException("recycled already");
			}
			item.recycleId = item.lastRecycledId = OWN_THREAD_ID;//这个栈的唯一id

			int size = this.size;
			if (size >= maxCapacity) {
				// Hit the maximum capacity - drop the possibly youngest object.
				return;
			}
			if (size == elements.length) {
				elements = Arrays.copyOf(elements, Math.min(size << 1, maxCapacity));
			}

			elements[size] = item;
			this.size = size + 1;
		}

		DefaultHandle<T> newHandle() {
			return new DefaultHandle<T>(this);
		}
	}
}
