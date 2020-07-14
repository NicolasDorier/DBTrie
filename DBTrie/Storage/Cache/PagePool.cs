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
		internal HashSet<Page> pages = new HashSet<Page>();
		internal LRU<Page>? lru;
		public int PageSize { get; }
		public int MaxPageCount { get; set; }
		public int PageCount => pages.Count;
		public int FreePageCount => MaxPageCount - pages.Count;
		public int EvictablePageCount
		{
			get
			{
				if (this.lru is LRU<Page> lru)
				{
					return lru.Count;
				}
				return 0;
			}
		}
		internal async ValueTask<Page> NewPage(int pageNumber)
		{
			if (lru is LRU<Page> && FreePageCount == 0)
			{
				if (lru.TryPop(out var leastUsedPage))
				{
					Debug.Assert(leastUsedPage.CanEvict);
					if (leastUsedPage.EvictedCallback is Func<Page, ValueTask> evictCallback)
						await evictCallback(leastUsedPage);
					leastUsedPage.Dispose();
				}
				else
				{
					throw new NoMorePageAvailableException();
				}
			}
			var page = new Page(pageNumber, this);
			page.Accessed();
			pages.Add(page);
			return page;
		}
	}
}
