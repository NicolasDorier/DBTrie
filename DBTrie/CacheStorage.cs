using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie
{
	
	public class CacheStorage : IStorage, IAsyncDisposable
	{
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

			public void Dispose()
			{
				_owner.Dispose();
			}
		}
		private readonly IStorage inner;
		internal SortedList<long, CachePage> pages = new SortedList<long, CachePage>();
		private MemoryPool<byte> MemoryPool = MemoryPool<byte>.Shared;
		bool own;
		public CacheStorage(IStorage inner, bool ownInner = true, int pageSize = 4096)
		{
			this.inner = inner;
			PageSize = pageSize;
			_Length = inner.Length;
			own = ownInner;
		}
		int LastPage = -1;
		public int PageSize { get; }

		public async ValueTask Read(long offset, Memory<byte> output)
		{
			var p = Math.DivRem(offset, PageSize, out var pageOffset);
			while (!output.IsEmpty)
			{
				if (!pages.TryGetValue(p, out var page))
				{
					page = await FetchPage(p);
				}
				var chunkLength = Math.Min(output.Length, PageSize - pageOffset);
				page.Content.Span.Slice((int)pageOffset, (int)chunkLength).CopyTo(output.Span);
				output = output.Slice((int)chunkLength);
				pageOffset = 0;
				p++;
			}
		}

		private async Task<CachePage> FetchPage(long p)
		{
			var owner = MemoryPool.Rent(PageSize);
			await inner.Read(p * PageSize, owner.Memory);
			var page = new CachePage((int)p, owner);
			pages.Add(p, page);
			LastPage = (int)Math.Max(p, LastPage);
			return page;
		}

		public async ValueTask Write(long offset, ReadOnlyMemory<byte> input)
		{
			var p = Math.DivRem(offset, PageSize, out var pageOffset);
			while (!input.IsEmpty)
			{
				if (!pages.TryGetValue(p, out var page))
				{
					page = await FetchPage(p);
				}
				input = WriteOnPage(page, pageOffset, input);
				pageOffset = 0;
				p++;
			}
		}

		public async ValueTask<long> WriteToEnd(ReadOnlyMemory<byte> input)
		{
			var len = input.Length;
			var p = Math.DivRem(Length, PageSize, out var pageOffset);
			while (!input.IsEmpty)
			{
				if (!pages.TryGetValue(p, out var page))
				{
					page = await FetchPage(p);
				}
				input = WriteOnPage(page, pageOffset, input);
				pageOffset = 0;
				p++;
			}
			var position = _Length;
			_Length += len;
			return position;
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
		public long Length => _Length;

		public async ValueTask Flush()
		{
			foreach(var page in pages.Where(p => p.Value.Dirty))
			{
				await inner.Write(page.Key * PageSize, page.Value.Content.Slice(page.Value.WrittenStart, page.Value.WrittenLength));
				page.Value.WrittenStart = 0;
				page.Value.WrittenLength = 0;
			}
			await inner.Flush();
		}

		public async ValueTask DisposeAsync()
		{
			await Flush();
			if (own)
				await inner.DisposeAsync();
			foreach (var page in pages)
				page.Value.Dispose();
			pages.Clear();
		}
	}
}
