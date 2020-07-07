using DBTrie.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Drawing;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.TrieModel
{
	internal class LTrie
	{
		static readonly byte[] DBreezeMagic;
		static LTrie()
		{
			DBreezeMagic = UTF8Encoding.UTF8.GetBytes("dbreeze.tiesky.com");
		}
		public static async ValueTask<LTrie> OpenFromStorage(IStorage storage, MemoryPool<byte>? memoryPool = null)
		{
			if (storage == null)
				throw new ArgumentNullException(nameof(storage));
			memoryPool ??= MemoryPool<byte>.Shared;
			using var owner = memoryPool.Rent(Sizes.RootSize);
			var mem = owner.Memory.Slice(0, Sizes.RootSize);
			await storage.Read(0, mem);
			return OpenFromSpan(storage, mem.Span, memoryPool);
		}
		public static LTrie OpenFromSpan(IStorage storage, ReadOnlySpan<byte> span, MemoryPool<byte>? memoryPool = null)
		{
			if (storage == null)
				throw new ArgumentNullException(nameof(storage));
			if (span[0] != 1)
				throw new FormatException("Impossible to parse the trie");
			if (span[1] != 1)
				throw new FormatException("Impossible to parse the trie");
			var link = (long)span.Slice(2, Sizes.DefaultPointerLen).BigEndianToLongDynamic();
			var count = (long)span.Slice(2 + Sizes.DefaultPointerLen, 8).BigEndianToLongDynamic();
			if (DBreezeMagic.AsSpan().SequenceCompareTo(span.Slice(2 + Sizes.DefaultPointerLen + 8, DBreezeMagic.Length)) != 0)
				throw new FormatException("Impossible to parse the trie");
			return new LTrie(storage, link, count, memoryPool);
		}
		public static async ValueTask<LTrie> InitTrie(IStorage storage, MemoryPool<byte>? memoryPool = null)
		{
			memoryPool ??= MemoryPool<byte>.Shared;
			using var owner = memoryPool.Rent(Sizes.RootSize);
			var memory = owner.Memory.Slice(0, WriteNew(owner.Memory.Span));
			await storage.Write(0, memory);
			return await OpenFromStorage(storage);
		}

		internal MemoryPool<byte> MemoryPool { get; }
		internal IStorage Storage { get; }
		public StorageHelper StorageHelper { get; }
		public bool ConsistencyCheck { get; set; }

		public void ActivateCache()
		{
			NodeCache = new NodeCache();
		}

		public NodeCache? NodeCache { get; private set; }

		internal async ValueTask<LTrieValue> ReadValue(long pointer)
		{
			//1byte - protocol, FullKeyLen (2 bytes), FullValueLen (4 bytes),[Reserved Space For Update- 4 bytes],FullKey,FullValue
			//1 + 2 + 4 + 4 + 100 = 111
			int readLen = 256;
			var owner = MemoryPool.Rent(readLen);
			var memory = owner.Memory.Slice(0, readLen);
			await Storage.Read(pointer, memory);
			var protocol = memory.Span[0];
			var headerSize = protocol == 0 ? 7 : 11;
			ushort keySize = memory.ToReadOnly().Span.Slice(1, 2).ReadUInt16BigEndian();
			if (keySize > (readLen - headerSize))
			{
				readLen = keySize + headerSize;
				owner.Dispose();
				owner = MemoryPool.Rent(readLen);
				memory = owner.Memory.Slice(0, readLen);
				await Storage.Read(pointer, memory);
			}
			bool nullValue = (memory.Span[3] & 0x80) != 0;
			int valueSize = (int)(nullValue ? 0U : memory.ToReadOnly().Span.Slice(3).ReadUInt32BigEndian());
			return new LTrieValue(owner.Slice(headerSize, keySize))
			{
				Protocol = protocol,
				Pointer = pointer,
				ValueLength = valueSize,
				ValuePointer = pointer + headerSize + keySize,
				ValueMaxLength = protocol == 0 ? valueSize : (int)memory.Slice(7).ToReadOnly().Span.ReadUInt32BigEndian()
			};
		}

		internal async ValueTask<bool> TryOverwriteValue(Link link, ReadOnlyMemory<byte> value)
		{
			if (link.LinkToNode)
				return false;
			using var record = await ReadValue(link.Pointer);
			if (record.ValueMaxLength >= value.Length)
			{
				using var owner = this.MemoryPool.Rent(LTrieValue.GetSize(record.Key, value));
				var len = record.WriteToSpan(owner.Memory.Span, value.Span);
				await Storage.Write(link.Pointer, owner.Memory.Slice(0, len));
				return true;
			}
			return false;
		}

		internal IMemoryOwner<byte> GetNameAsBytes(string str)
		{
			var bytes = Encoding.UTF8.GetByteCount(str, 0, str.Length);
			var owner = MemoryPool.Rent(bytes);
			Encoding.UTF8.GetBytes(str.AsSpan(), owner.Memory.Span);
			return owner.Slice(0, bytes);
		}

		internal LTrie(IStorage storage, long rootPointer, long recordCount, MemoryPool<byte>? memoryPool = null)
		{
			if (storage == null)
				throw new ArgumentNullException(nameof(storage));
			memoryPool ??= MemoryPool<byte>.Shared;
			MemoryPool = memoryPool;
			Storage = storage;
			StorageHelper = new StorageHelper(memoryPool, storage);
			RootPointer = rootPointer;
			RecordCount = recordCount;
		}

		public long RootPointer { get; private set; }
		public long RecordCount { get; private set; }


		internal static int WriteNew(Span<byte> output)
		{
			int i = 0;
			output[i++] = 1;
			output[i++] = 1;
			int linkToNode = i;
			i += Sizes.DefaultPointerLen;
			i += 8;
			DBreezeMagic.CopyTo(output.Slice(i));
			i += DBreezeMagic.Length;
			output.Slice(linkToNode).ToBigEndianDynamic((ulong)i);
			i += LTrieNode.WriteNew(output.Slice(i), 1);
			return i;
		}

		public ValueTask<LTrieNode> ReadNode()
		{
			return ReadNode(RootPointer, 0);
		}
		public async ValueTask<LTrieNode> ReadNode(long pointer, int minKeyLength, bool useCache = true)
		{
			LTrieNode? cached = null;
			NodeCache?.TryGetValue(pointer, out cached);
			if (cached is LTrieNode && useCache)
			{
				if (cached.MinKeyLength != minKeyLength)
					throw new InvalidOperationException("Inconsistent depth, bug in DBTrie");
				if (cached.OwnPointer != pointer)
					throw new InvalidOperationException("Inconsistent pointer in cache, bug in DBTrie");
				await cached.AssertConsistency();
				return cached;
			}
			using var owner = MemoryPool.Rent(Sizes.MaximumNodeSize);
			var memory = owner.Memory.Slice(Sizes.MaximumNodeSize);
			await Storage.Read(pointer, memory);
			var node = new LTrieNode(this, minKeyLength, pointer, memory);
			if (useCache)
				NodeCache?.Add(pointer, node);
			return node;
		}

		public async ValueTask<LTrieRow?> GetKey(string key)
		{
			using var owner = GetNameAsBytes(key);
			return await GetRow(owner.Memory);
		}

		public async ValueTask SetKey(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			var res = await FindBestMatch(key);
			bool increaseRecord = false;
			if (res.ValueLink is null)
			{
				increaseRecord = true;
				var oldPointer = res.BestNode.OwnPointer;
				var relocated = await res.BestNode.SetValue(res.MissingValue, key, value);
				if (relocated)
				{
					if (res.BestNodeParent is LTrieNode parent)
					{
						var incomingLink = parent.GetLinkFromPointer(oldPointer);
						await this.StorageHelper.WritePointer(incomingLink.OwnPointer + 2, res.BestNode.OwnPointer);
						// Update in-memory
						incomingLink.Pointer = res.BestNode.OwnPointer;
						await parent.AssertConsistency();
					}
					else
					{
						await this.StorageHelper.WritePointer(2, res.BestNode.OwnPointer);
						// Update in-memory
						RootPointer = res.BestNode.OwnPointer;
					}
				}
				await res.BestNode.AssertConsistency();
			}
			else
			{
				// we replace the internal value
				if (res.BestNode.MinKeyLength == key.Length)
				{
					await res.BestNode.SetValue(key, value);
					return;
				}
				var record = await ReadValue(res.ValueLink.Pointer);
				// We are replacing a child with the same key
				if (record.Key.Span.SequenceEqual(key.Span))
				{
					await res.BestNode.SetValue(res.ValueLink.Label, key, value);
					return;
				}

				var prev = res.BestNodeParent;
				var gn = res.BestNode;
				var maxCommonKeyLength = Math.Min(key.Length, record.Key.Length);
				// We need to convert the value child to a node
				// For this, we expand nodes until there is divergence of key between record key and the search key
				for (int i = res.KeyIndex; i < maxCommonKeyLength; i++)
				{
					if (key.Span[i] != record.Key.Span[i])
						break;
					var oldPointer = gn.OwnPointer;
					var result = await gn.SetLinkToNode(key.Span[i]);
					if (result.Relocated)
					{
						if (prev is LTrieNode)
						{
							var incomingLink = prev.GetLinkFromPointer(oldPointer);
							await StorageHelper.WritePointer(incomingLink.OwnPointer + 2, gn.OwnPointer);

							// Update in-memory
							incomingLink.Pointer = gn.OwnPointer;
						}
						else
						{
							await this.StorageHelper.WritePointer(2, res.BestNode.OwnPointer);
							// Update in-memory
							RootPointer = gn.OwnPointer;
						}
					}
					prev = gn;
					await gn.AssertConsistency();
					gn = await ReadNode(result.Link.Pointer, prev.MinKeyLength + 1);
				}
				increaseRecord = true;
				if (maxCommonKeyLength == key.Length)
				{
					await gn.SetValue(key, value);
				}
				else
				{
					await gn.SetValue(key.Span[maxCommonKeyLength], key, value);
				}
				await gn.AssertConsistency();
			}
			if (increaseRecord)
			{
				// Update the record count
				await StorageHelper.WriteLong(2 + Sizes.DefaultPointerLen, RecordCount + 1);
				// Update in-memory
				RecordCount++;
			}
		}

		internal async IAsyncEnumerable<LTrieValue> EnumerateStartWith(string startWithKey)
		{
			var c = Encoding.UTF8.GetByteCount(startWithKey, 0, startWithKey.Length);
			using var owner = MemoryPool.Rent(c);
			Encoding.UTF8.GetBytes(startWithKey, owner.Memory.Span.Slice(0, c));
			await foreach (var item in EnumerateStartWith(owner.Memory.Slice(0, c)))
			{
				yield return item;
			}
		}

		internal async IAsyncEnumerable<LTrieValue> EnumerateStartWith(ReadOnlyMemory<byte> startWithKey)
		{
			var res = await FindBestMatch(startWithKey);

			// In this case, we don't have an exact match, and no children either
			if (res.ValueLink is null && res.MissingValue is byte)
				yield break;
			if (res.ValueLink is Link)
			{
				var record = await ReadValue(res.ValueLink.Pointer);
				if (record.Key.Span.StartsWith(startWithKey.Span))
					yield return record;
			}
			var nextNodes = new Stack<IEnumerator<Link>>();
			nextNodes.Push(res.BestNode.ExternalLinks.GetEnumerator());
			while (nextNodes.TryPop(out var externalLinks))
			{
				while (externalLinks.MoveNext())
				{
					var link = externalLinks.Current;
					if (!link.LinkToNode)
					{
						var record = await ReadValue(link.Pointer);
						if (record.Key.Span.StartsWith(startWithKey.Span))
							yield return record;
					}
					else
					{
						var childNode = await ReadNode(link.Pointer, res.BestNode.MinKeyLength + nextNodes.Count + 1);
						if (childNode.InternalLink is Link ll)
						{
							var record = await ReadValue(ll.Pointer);
							if (record.Key.Span.StartsWith(startWithKey.Span))
								yield return record;
						}
						nextNodes.Push(externalLinks);
						nextNodes.Push(childNode.ExternalLinks.GetEnumerator());
						break;
					}
				}
			}
		}
		public async ValueTask<LTrieRow?> GetRow(string key)
		{
			var c = Encoding.UTF8.GetByteCount(key, 0, key.Length);
			using var owner = MemoryPool.Rent(c);
			Encoding.UTF8.GetBytes(key, owner.Memory.Span.Slice(0, c));
			return await GetRow(owner.Memory.Slice(0, c));
		}
		public async ValueTask<LTrieRow?> GetRow(ReadOnlyMemory<byte> key)
		{
			var res = await FindBestMatch(key);
			if (res.ValueLink is null)
				return null;
			using var r = await ReadValue(res.ValueLink.Pointer);
			if (r.Key.Span.SequenceCompareTo(key.Span) != 0)
				return null;
			return new LTrieRow(Storage, key)
			{
				Pointer = res.ValueLink.Pointer,
				ValueLength = r.ValueLength,
				ValuePointer = r.ValuePointer
			};
		}

		internal async ValueTask<MatchResult> FindBestMatch(ReadOnlyMemory<byte> key)
		{
			LTrieNode gn = await ReadNode();
			LTrieNode? prev = null;
			if (key.Length == 0)
			{
				if (gn.InternalLink is Link k)
				{
					return new MatchResult(k, gn, prev);
				}
			}

			for (int i = 0; i < key.Length; i++)
			{
				var externalLink = gn.GetLink(key.Span[i]);
				if (externalLink is null)
				{
					return new MatchResult(null, gn, prev)
					{
						MissingValue = key.Span[i],
						KeyIndex = i
					};
				}
				if (!externalLink.LinkToNode)
				{
					return new MatchResult(externalLink, gn, prev)
					{
						KeyIndex = i
					};
				}
				prev = gn;
				gn = await ReadNode(externalLink.Pointer, i + 1);
			}
			return new MatchResult(gn.InternalLink, gn, prev);
		}

		internal class MatchResult
		{
			public MatchResult(Link? valueLink,
							   LTrieNode node,
							   LTrieNode? nodeParent)
			{
				BestNodeParent = nodeParent;
				BestNode = node;
				ValueLink = valueLink;
			}
			public LTrieNode? BestNodeParent { get; }
			public LTrieNode BestNode { get; }
			public Link? ValueLink { get; set; }
			public byte? MissingValue { get; set; }
			public int KeyIndex { get; internal set; } = -1;
		}
	}
}
