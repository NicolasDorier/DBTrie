using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Storage.Cache
{
	public class PagePool
	{
		internal MemoryPool<byte> MemoryPool = MemoryPool<byte>.Shared;
		public PagePool(int pageSize = Sizes.DefaultPageSize, int maxPageCount = int.MaxValue)
		{
			PageSize = pageSize;
			MaxPageCount = maxPageCount;
			if (maxPageCount != int.MaxValue)
				lru = new LRU<Page>();
		}
		internal LRU<Page>? lru;
		public int PageSize { get; }
		public int MaxPageCount { get; }
		public int PageCount { get; internal set; }
		internal async ValueTask<Page> NewPage(int pageNumber)
		{
			if (lru is LRU<Page>)
			{
				Debug.Assert(lru.Count <= MaxPageCount);
				if (lru.Count == MaxPageCount)
				{
					bool noPageToEvict = true;
					Stack<Page>? backToLRU = null;
					while (lru.Count > 0)
					{
						if (lru.TryPop(out var leastUsedPage))
						{
							if (leastUsedPage.CanEvict)
							{
								if (leastUsedPage.EvictedCallback is Func<Page, ValueTask> evictCallback)
									await evictCallback(leastUsedPage);
								noPageToEvict = false;
								leastUsedPage.Dispose(true);
								break;
							}
							else
							{
								backToLRU ??= new Stack<Page>();
								backToLRU.Push(leastUsedPage);
							}
						}
					}
					while (backToLRU is Stack<Page> s && s.TryPop(out var dirtyPop))
					{
						lru.Push(dirtyPop);
					}
					if (noPageToEvict)
						throw new InvalidOperationException("No page available to evict from cache");
				}
			}
			var page = new Page(pageNumber, this);
			page.Accessed();
			PageCount++;
			Debug.Assert(lru is LRU<Page> l ? PageCount == l.Count : true);
			return page;
		}
	}
}
