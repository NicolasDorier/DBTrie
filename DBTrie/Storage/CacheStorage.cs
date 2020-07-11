using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Storage
{
	public class CacheSettings
	{
		public CacheSettings()
		{
			PageSize = Sizes.DefaultPageSize;
		}
		public int PageSize { get; set; }
		/// <summary>
		/// If set, will evict page from the case as soon as we reach this count
		/// </summary>
		public int? MaxPageCount { get; set; }

		/// <summary>
		/// If true, and a page get evicted with uncommitted data, this will automatically write it down the inner storage.
		/// </summary>
		public bool AutoCommitEvictedPages { get; set; }

		public CacheSettings Clone()
		{
			return new CacheSettings()
			{
				PageSize = PageSize,
				MaxPageCount = MaxPageCount,
				AutoCommitEvictedPages = AutoCommitEvictedPages
			};
		}
	}
	public class CacheStorage : IStorage, IAsyncDisposable
	{
		internal class LRU
		{
			public int Count => hashmap.Count;
			LinkedList<long> list = new LinkedList<long>();
			Dictionary<long, LinkedListNode<long>> hashmap = new Dictionary<long, LinkedListNode<long>>();
			public void Accessed(long page)
			{
				if (hashmap.TryGetValue(page, out var node))
				{
					list.Remove(node);
					list.AddLast(node);
				}
				else
				{
					node = list.AddLast(page);
					hashmap.Add(page, node);
				}
				Debug.Assert(hashmap.Count == list.Count);
			}
			public bool TryPop(out long page)
			{
				if (list.Count == 0)
				{
					page = -1;
					return false;
				}
				page = list.First.Value;
				hashmap.Remove(page);
				list.RemoveFirst();
				Debug.Assert(hashmap.Count == list.Count);
				return true;
			}

			public void Push(long page)
			{
				var node = list.AddFirst(page);
				hashmap.Add(page, node);
				Debug.Assert(hashmap.Count == list.Count);
			}

			public void Remove(long page)
			{
				if (hashmap.TryGetValue(page, out var node))
				{
					list.Remove(node);
					hashmap.Remove(page);
				}
				Debug.Assert(hashmap.Count == list.Count);
			}
		}
		internal class CachePage : IDisposable
		{
			public CachePage(int page, IMemoryOwner<byte> memory)
			{
				PageNumber = page;
				_owner = memory;
			}
			public int PageNumber { get; }
			public int WrittenStart { get; set; }
			public int WrittenLength { get; set; }
			IMemoryOwner<byte> _owner;
			public Memory<byte> Content => _owner.Memory;

			public bool Dirty => WrittenLength != 0;

			public async ValueTask Flush(IStorage storage, int pageSize)
			{
				await storage.Write(PageNumber * pageSize, Content.Slice(WrittenStart, WrittenLength));
				WrittenStart = 0;
				WrittenLength = 0;
			}
			public void Dispose()
			{
				_owner.Dispose();
			}
		}
		public IStorage InnerStorage { get; }
		internal Dictionary<long, CachePage> pages = new Dictionary<long, CachePage>();



		private MemoryPool<byte> MemoryPool = MemoryPool<byte>.Shared;
		bool own;
		LRU? lru = null;

		CacheSettings Settings;
		public CacheStorage(IStorage inner, bool ownInner = true, CacheSettings? settings = null)
		{
			Settings = settings ?? new CacheSettings();
			InnerStorage = inner;
			Length = inner.Length;
			own = ownInner;
			if (Settings.MaxPageCount is int)
				lru = new LRU();
		}
		public int PageSize => Settings.PageSize;

		public async ValueTask Read(long offset, Memory<byte> output)
		{
			var p = Math.DivRem(offset, PageSize, out var pageOffset);
			if (p > _LastPage)
			{
				output.Span.Fill(0);
				return;
			}
			CachePage? page = null;
			while (!output.IsEmpty)
			{
				if (!pages.TryGetValue(p, out page))
				{
					page = await FetchPage(p);
				}
				Accessed(p);
				var chunkLength = Math.Min(output.Length, PageSize - pageOffset);
				if (_LastPage == p)
				{
					chunkLength = Math.Min(chunkLength, _LastPageLength);
					page.Content.Span.Slice((int)pageOffset, (int)chunkLength).CopyTo(output.Span);
					output.Span.Slice((int)chunkLength).Fill(0);
					return;
				}
				else
				{
					page.Content.Span.Slice((int)pageOffset, (int)chunkLength).CopyTo(output.Span);
				}
				output = output.Slice((int)chunkLength);
				pageOffset = 0;
				p++;				
			}
		}

		private async Task<CachePage> FetchPage(long p)
		{
			if (lru is LRU && Settings.MaxPageCount is int max && pages.Count >= max)
			{
				bool noPageToEvict = true;
				Stack<long>? backToLRU = null;
				while (pages.Count >= max && lru.Count > 0)
				{
					if (lru.TryPop(out var leastUsedPage) && 
						pages.TryGetValue(leastUsedPage, out var lruPage))
					{
						bool evict = true;
						if (lruPage.Dirty)
						{
							if (Settings.AutoCommitEvictedPages)
							{
								await lruPage.Flush(InnerStorage, PageSize);
							}
							else
							{
								evict = false;
								backToLRU ??= new Stack<long>();
								backToLRU.Push(leastUsedPage);
							}
						}
						if (evict)
						{
							noPageToEvict = false;
							lruPage.Dispose();
							pages.Remove(lruPage.PageNumber);
						}
					}
				}
				while (backToLRU is Stack<long> s && s.TryPop(out var dirtyPop))
				{
					lru.Push(dirtyPop);
				}
				if (noPageToEvict)
					throw new InvalidOperationException("No page available to evict from cache");
			}
			var owner = MemoryPool.Rent(PageSize);
			await InnerStorage.Read(p * PageSize, owner.Memory);
			var page = new CachePage((int)p, owner);
			pages.Add(p, page);
			return page;
		}

		public async ValueTask Write(long offset, ReadOnlyMemory<byte> input)
		{
			var lastByte = offset + input.Length;
			var p = Math.DivRem(offset, PageSize, out var pageOffset);
			while (!input.IsEmpty)
			{
				if (!pages.TryGetValue(p, out var page))
				{
					page = await FetchPage(p);
				}
				Accessed(p);
				input = WriteOnPage(page, pageOffset, input);
				pageOffset = 0;
				p++;
			}
			Length = Math.Max(Length, lastByte);			
		}

		private void Accessed(long p)
		{
			if (lru is LRU)
			{
				lru.Accessed(p);
				Debug.Assert(lru.Count == pages.Count);
			}
		}

		private ReadOnlyMemory<byte> WriteOnPage(CachePage page, long pageOffset, ReadOnlyMemory<byte> input)
		{
			var chunkLength = Math.Min(input.Length, PageSize - pageOffset);
			input.Span.Slice(0, (int)chunkLength).CopyTo(page.Content.Span.Slice((int)pageOffset));
			input = input.Slice((int)chunkLength);
			page.WrittenLength = (int)Math.Max(page.WrittenLength, pageOffset + chunkLength);
			page.WrittenStart = (int)Math.Min(page.WrittenStart, pageOffset);
			return input;
		}

		long _Length;
		private long _LastPage;
		private long _LastPageLength;

		public long Length
		{
			get
			{
				return _Length;
			}
			internal set
			{
				if (value < 0)
					throw new ArgumentOutOfRangeException();
				if (_Length != value)
				{
					var oldSize = _Length;
					_Length = value;
					_LastPage = Math.DivRem((int)value, PageSize, out _LastPageLength);
					if (_LastPageLength == 0)
					{
						_LastPage--;
						_LastPageLength = PageSize;
					}
					if (oldSize > _Length)
					{
						List<long>? toRemove = null;
						foreach (var page in pages)
						{
							if (page.Value.PageNumber > _LastPage)
							{
								if (toRemove is null)
									toRemove = new List<long>();
								toRemove.Add(page.Key);
								page.Value.Dispose();
							}
						}
						if (pages.TryGetValue(_LastPage, out var lastPage))
						{
							var oldWrittenLength = lastPage.WrittenLength;
							lastPage.WrittenLength = (int)Math.Min(lastPage.WrittenLength, _LastPageLength);
							lastPage.Content.Span.Slice(lastPage.WrittenLength, oldWrittenLength - lastPage.WrittenLength).Fill(0);
						}
						if (toRemove != null)
						{
							foreach (var page in toRemove)
							{
								lru?.Remove(page);
								pages.Remove(page);
							}
						}
					}
				}
			}
		}

		public int MappedPageCount => pages.Count;

		public async ValueTask Flush()
		{
			await InnerStorage.Resize(Length);
			foreach(var page in pages.Where(p => p.Value.Dirty))
			{
				await page.Value.Flush(InnerStorage, PageSize);
			}
			await InnerStorage.Flush();
		}

		/// <summary>
		/// Make sure the underlying file can grow to write the data to commit
		/// </summary>
		/// <returns></returns>
		public async ValueTask ResizeInner()
		{
			await InnerStorage.Resize(Length);
		}

		public bool Clear(bool clearOnlyWrittenPages)
		{
			List<long> toRemove = new List<long>();
			foreach (var page in pages.Where(p => !clearOnlyWrittenPages || p.Value.Dirty))
			{
				toRemove.Add(page.Key);
				page.Value.Dispose();
			}
			foreach (var page in toRemove)
			{
				lru?.Remove(page);
				pages.Remove(page);
			}
			return toRemove.Count > 0;
		}

		public async ValueTask DisposeAsync()
		{
			await Flush();
			if (own)
				await InnerStorage.DisposeAsync();
			Clear(false);
			pages.Clear();
		}

		public ValueTask Resize(long newLength)
		{
			Length = newLength;
			return default;
		}

		public bool TryDirectRead(long offset, long length, out ReadOnlyMemory<byte> output)
		{
			output = default;
			if (length > PageSize)
				return false;
			var p = Math.DivRem(offset, PageSize, out var pageOffset);
			if (p > _LastPage)
				return false;
			var pageLength = p == _LastPage ? _LastPageLength : PageSize;
			if (pageOffset + length > pageLength)
				return false;
			if (!pages.TryGetValue(p, out var page))
				return false;
			Accessed(p);
			output = page.Content.Slice((int)pageOffset, (int)length);
			return true;
		}
	}
}
