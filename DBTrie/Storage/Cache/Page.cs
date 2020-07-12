using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Storage.Cache
{
	internal class Page
	{
		public Page(int pageNumber, PagePool pool)
		{
			this.pool = pool;
			_owner = pool.MemoryPool.Rent(pool.PageSize);
			this.PageNumber = pageNumber;
		}
		public int PageNumber { get; }
		public int WrittenStart { get; set; }
		public int WrittenLength { get; set; }
		bool disposed;
		IMemoryOwner<byte> _owner;
		private readonly PagePool pool;

		public Memory<byte> Content => _owner.Memory;
		public bool CanEvict { get; set; }
		public Func<Page, ValueTask>? EvictedCallback { get; set; }

		public bool Dirty => WrittenLength != 0;

		public void Accessed()
		{
			Debug.Assert(!disposed);
			if (this.pool.lru is LRU<Page> lru)
				lru.Accessed(this);
		}
		public void Dispose(bool evicted = false)
		{
			if (!disposed)
			{
				disposed = true;
				_owner.Dispose();
				this.pool.PageCount--;
				if (!evicted && this.pool.lru is LRU<Page> lru)
					lru.Remove(this);
			}
		}
	}
}
