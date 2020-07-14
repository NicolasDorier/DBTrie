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
		public bool CanEvict { get; private set; }

		public void EnableEviction()
		{
			if (CanEvict)
				return;
			Debug.Assert(!(this.pool.lru is LRU<Page> lru) || !lru.Contains(this));
			CanEvict = true;
			Accessed();
		}
		public void DisableEviction()
		{
			if (!CanEvict)
				return;
			Debug.Assert(!(this.pool.lru is LRU<Page> lru) || lru.Contains(this));
			CanEvict = false;
			this.pool.lru?.Remove(this);
		}

		public Func<Page, ValueTask>? EvictedCallback { get; set; }

		public bool Dirty => WrittenLength != 0;

		public void Accessed()
		{
			Debug.Assert(!disposed);
			if (CanEvict && this.pool.lru is LRU<Page> lru)
				lru.Accessed(this);
		}
		public void Dispose()
		{
			if (!disposed)
			{
				disposed = true;
				_owner.Dispose();
				this.pool.pages.Remove(this);
				this.pool.lru?.Remove(this);
			}
		}
	}
}
