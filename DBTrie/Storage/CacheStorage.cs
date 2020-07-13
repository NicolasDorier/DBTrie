using DBTrie.Storage.Cache;
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
		public IStorage InnerStorage { get; }
		internal Dictionary<int, Page> pages = new Dictionary<int, Page>();
		bool own;
		bool canCommitAutomatically;
		PagePool _PagePool;
		Func<Page, ValueTask> _EvictedCallback;

		public int PageSize => _PagePool.PageSize;

		public CacheStorage(IStorage inner, bool ownInner = true, CacheSettings? settings = null):
			this(inner, ownInner, CreatePagePool(settings), (settings ?? new CacheSettings()).AutoCommitEvictedPages)
		{
			
		}

		private static PagePool CreatePagePool(CacheSettings? settings)
		{
			settings ??= new CacheSettings();
			return new PagePool(settings.PageSize, settings.MaxPageCount ?? int.MaxValue);
		}

		internal CacheStorage(IStorage inner, bool ownInner, PagePool pagePool, bool autoCommitEvictedPages)
		{
			if (pagePool == null)
				throw new ArgumentNullException(nameof(pagePool));
			InnerStorage = inner;
			own = ownInner;
			this.canCommitAutomatically = autoCommitEvictedPages;
			_PagePool = pagePool;
			_EvictedCallback = new Func<Page, ValueTask>(EvictPage);
			Length = inner.Length;
			_LengthChanged = false;
		}

		async ValueTask EvictPage(Page p)
		{
			await FlushPage(p);
			pages.Remove(p.PageNumber);
		}

		public async ValueTask Read(long offset, Memory<byte> output)
		{
			var p = (int)Math.DivRem(offset, _PagePool.PageSize, out var pageOffset);
			if (p > _LastPage)
			{
				output.Span.Fill(0);
				return;
			}
			Page? page = null;
			while (!output.IsEmpty)
			{
				if (!pages.TryGetValue(p, out page))
				{
					page = await FetchPage(p);
				}
				page.Accessed();
				var chunkLength = Math.Min(output.Length, _PagePool.PageSize - pageOffset);
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

		private async ValueTask<Page> FetchPage(int p)
		{
			var page = await _PagePool.NewPage((int)p);
			await InnerStorage.Read(p * _PagePool.PageSize, page.Content);
			pages.Add(p, page);
			page.CanEvict = true;
			page.EvictedCallback = _EvictedCallback;
			return page;
		}

		public async ValueTask Write(long offset, ReadOnlyMemory<byte> input)
		{
			var lastByte = offset + input.Length;
			var p = (int)Math.DivRem(offset, _PagePool.PageSize, out var pageOffset);
			while (!input.IsEmpty)
			{
				if (!pages.TryGetValue(p, out var page))
				{
					page = await FetchPage(p);
				}
				page.Accessed();
				input = WriteOnPage(page, pageOffset, input);
				pageOffset = 0;
				p++;
			}
			Length = Math.Max(Length, lastByte);			
		}

		private ReadOnlyMemory<byte> WriteOnPage(Page page, long pageOffset, ReadOnlyMemory<byte> input)
		{
			var chunkLength = Math.Min(input.Length, _PagePool.PageSize - pageOffset);
			input.Span.Slice(0, (int)chunkLength).CopyTo(page.Content.Span.Slice((int)pageOffset));
			input = input.Slice((int)chunkLength);
			page.WrittenLength = (int)Math.Max(page.WrittenLength, pageOffset + chunkLength);
			page.WrittenStart = (int)Math.Min(page.WrittenStart, pageOffset);
			page.CanEvict = canCommitAutomatically;
			return input;
		}

		long _Length;
		private int _LastPage;
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
					_LengthChanged = true;
					var oldSize = _Length;
					_Length = value;
					_LastPage = (int)Math.DivRem((int)value, _PagePool.PageSize, out _LastPageLength);
					if (_LastPageLength == 0)
					{
						_LastPage--;
						_LastPageLength = _PagePool.PageSize;
					}
					if (oldSize > _Length)
					{
						List<int>? toRemove = null;
						foreach (var page in pages)
						{
							if (page.Key > _LastPage)
							{
								if (toRemove is null)
									toRemove = new List<int>();
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
								pages.Remove(page);
							}
						}
					}
				}
			}
		}

		public int MappedPageCount => pages.Count;

		bool _LengthChanged;
		public async ValueTask Flush()
		{
			if (_LengthChanged)
				await InnerStorage.Resize(Length);
			foreach(var page in pages.Where(p => p.Value.Dirty))
			{
				await FlushPage(page.Value);
			}
			await InnerStorage.Flush();
			_LengthChanged = false;
		}

		async ValueTask FlushPage(Page page)
		{
			await InnerStorage.Write(page.PageNumber * _PagePool.PageSize, page.Content.Slice(page.WrittenStart, page.WrittenLength));
			page.WrittenStart = 0;
			page.WrittenLength = 0;
			page.CanEvict = true;
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
			List<int> toRemove = new List<int>();
			foreach (var page in pages.Where(p => !clearOnlyWrittenPages || p.Value.Dirty))
			{
				toRemove.Add(page.Key);
				page.Value.Dispose();
			}
			foreach (var page in toRemove)
			{
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
			if (length > _PagePool.PageSize)
				return false;
			var p = (int)Math.DivRem(offset, _PagePool.PageSize, out var pageOffset);
			if (p > _LastPage)
				return false;
			var pageLength = p == _LastPage ? _LastPageLength : _PagePool.PageSize;
			if (pageOffset + length > pageLength)
				return false;
			if (!pages.TryGetValue(p, out var page))
				return false;
			page.Accessed();
			output = page.Content.Slice((int)pageOffset, (int)length);
			return true;
		}
	}
}
