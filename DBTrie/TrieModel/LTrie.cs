using DBTrie.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
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

		public static async ValueTask<LTrie> OpenOrInitFromStorage(IStorage storage, MemoryPool<byte>? memoryPool = null)
		{
			try
			{
				return await OpenFromStorage(storage, memoryPool);
			}
			catch (FormatException)
			{
				return await InitTrie(storage, memoryPool);
			}
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

		public async ValueTask<bool> SetKey(string key, string value)
		{
			var keyCount = Encoding.UTF8.GetByteCount(key);
			var valueCount = Encoding.UTF8.GetByteCount(value);
			using var owner = MemoryPool.Rent(keyCount + valueCount);
			Encoding.UTF8.GetBytes(key, owner.Memory.Span);
			Encoding.UTF8.GetBytes(value, owner.Memory.Span.Slice(keyCount));
			return await SetValue(owner.Memory.Slice(0, keyCount), owner.Memory.Slice(keyCount, valueCount));
		}
		public async ValueTask<bool> SetValue(string key, ReadOnlyMemory<byte> value)
		{
			var keyCount = Encoding.UTF8.GetByteCount(key);
			using var owner = MemoryPool.Rent(keyCount);
			Encoding.UTF8.GetBytes(key, owner.Memory.Span);
			return await SetValue(owner.Memory.Slice(0, keyCount), value);
		}
		internal async ValueTask<bool> SetValue(ReadOnlyMemory<byte> key, ulong value)
		{
			using var owner = MemoryPool.Rent(8);
			owner.Memory.Span.ToBigEndian(value);
			return await SetValue(key, owner.Memory.Slice(0, 8));
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
			var rootAndNodeSize = Sizes.RootSize + LTrieNode.GetSize(1);
			using var owner = memoryPool.Rent(rootAndNodeSize);
			WriteRoot(owner.Memory.Span);
			LTrieNode.WriteNew(owner.Memory.Span.Slice(Sizes.RootSize), 1);
			await storage.Write(0, owner.Memory.Slice(0, rootAndNodeSize));
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
			return new LTrieValue(this, owner.Slice(headerSize, keySize), valueSize)
			{
				Protocol = protocol,
				Pointer = pointer,
				ValuePointer = pointer + headerSize + keySize,
				ValueMaxLength = protocol == 0 ? valueSize : (int)memory.Slice(7).ToReadOnly().Span.ReadUInt32BigEndian()
			};
		}

		public async ValueTask<int> Defragment(CancellationToken cancellationToken = default)
		{
			var usedMemories = new List<UsedMemory>();
			usedMemories.Add(new UsedMemory() { Pointer = 0, Size = Sizes.RootSize, PointingTo = new List<UsedMemory>() });
			var nodesToVisit = new Stack<(long NodePointer, long PointedFrom, UsedMemory ParentNode)>();
			nodesToVisit.Push((RootPointer, 2, usedMemories[0]));
			while(nodesToVisit.Count > 0)
			{
				cancellationToken.ThrowIfCancellationRequested();
				var current = nodesToVisit.Pop();
				var node = await ReadNode(current.NodePointer, -1, false);
				var nodeMemory = new UsedMemory()
				{
					Pointer = node.OwnPointer,
					Size = node.Size,
					PointedBy = current.PointedFrom,
					PointingTo = new List<UsedMemory>(node.ExternalLinks.Count + (node.InternalLink is null ? 0 : 1))
				};
				current.ParentNode.PointingTo!.Add(nodeMemory);
				usedMemories.Add(nodeMemory);
				if (node.InternalLink is Link internalLink)
				{
					using var v = await ReadValue(internalLink.Pointer);
					var valueMem = new UsedMemory()
					{
						Pointer = v.Pointer,
						Size = v.Size,
						PointedBy = internalLink.OwnPointer
					};
					nodeMemory.PointingTo.Add(valueMem);
					usedMemories.Add(valueMem);
				}
				foreach (var externalLink in node.ExternalLinks)
				{
					if (externalLink.LinkToNode)
					{
						nodesToVisit.Push((externalLink.Pointer, externalLink.OwnPointer + 2, nodeMemory));
					}
					else
					{
						using var v = await ReadValue(externalLink.Pointer);
						var valueMem = new UsedMemory()
						{
							Pointer = v.Pointer,
							Size = v.Size,
							PointedBy = externalLink.OwnPointer + 2
						};
						nodeMemory.PointingTo.Add(valueMem);
						usedMemories.Add(valueMem);
					}
				}
			}
			NodeCache?.Clear();
			int totalSaved = 0;
			int nextOffset = Sizes.RootSize;
			// We have a list of all memory region in use (skipping root)
			var owner = MemoryPool.Rent(256);
			foreach (var region in usedMemories.OrderBy(u => u.Pointer).Skip(1))
			{
				// We can't cancel mid-way
				var gap = (int)(region.Pointer - nextOffset);
				if (gap > 0)
				{
					if (owner.Memory.Span.Length < region.Size)
					{
						owner.Dispose();
						owner = MemoryPool.Rent(region.Size);
					}
					var mem = owner.Memory.Slice(0, region.Size);
					await Storage.Read(region.Pointer, mem);
					await Storage.Write(nextOffset, mem);
					await StorageHelper.WritePointer(region.PointedBy, nextOffset);
					region.Pointer -= gap;
					if (region.PointingTo is List<UsedMemory> pointingTo)
					{
						foreach (var childmem in pointingTo)
						{
							childmem.PointedBy -= gap;
						}
					}
					totalSaved = gap - totalSaved;
				}
				else if (gap != 0)
					throw new InvalidOperationException("Bug in DBTrie during garbage collection");
				nextOffset += region.Size;
			}
			owner.Dispose();
			RootPointer = await StorageHelper.ReadPointer(2);
			await Storage.Resize(Storage.Length - totalSaved);
			return totalSaved;
		}

		internal async ValueTask<bool> TryOverwriteValue(Link link, ReadOnlyMemory<byte> value)
		{
			if (link.LinkToNode)
				return false;
			using var record = await ReadValue(link.Pointer);
			if (record.ValueMaxLength >= value.Length)
			{
				using var owner = this.MemoryPool.Rent(LTrieValue.GetSize(record.Key.Length, value.Length, record.ValueMaxLength));
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


		internal static void WriteRoot(Span<byte> output)
		{
			int i = 0;
			output[i++] = 1;
			output[i++] = 1;
			output.Slice(i).ToBigEndianDynamic(64);
			i += Sizes.DefaultPointerLen;
			output.Slice(i, 8).Fill(0);
			i += 8;
			DBreezeMagic.CopyTo(output.Slice(i));
			i += DBreezeMagic.Length;
			output.Slice(i, Sizes.RootSize - i).Fill(0);
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
			var memory = owner.Memory.Slice(0, Sizes.MaximumNodeSize);
			await Storage.Read(pointer, memory);
			var node = new LTrieNode(this, minKeyLength, pointer, memory);
			if (useCache)
				NodeCache?.Add(pointer, node);
			return node;
		}

		public async ValueTask<LTrieValue?> GetValue(string key)
		{
			using var owner = GetNameAsBytes(key);
			return await GetValue(owner.Memory);
		}
		public async ValueTask<string?> GetValueString(string key)
		{
			using var owner = GetNameAsBytes(key);
			var val = await GetValue(owner.Memory);
			if (val is null)
				return null;
			return await val.ReadValueString();
		}

		public async ValueTask<bool> SetValue(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
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
					increaseRecord = false;
					await res.BestNode.SetInternalValue(key, value);
				}
				else
				{
					using var record = await ReadValue(res.ValueLink.Pointer);
					// We are replacing a child with the same key
					if (record.Key.Span.SequenceEqual(key.Span))
					{
						increaseRecord = false;
						await res.BestNode.SetValue(res.ValueLink.Label, key, value);
					}
					else
					{
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
							var link = await gn.SetValueLinkToNode(key.Span[i]);
							prev = gn;
							await gn.AssertConsistency();
							gn = await ReadNode(link.Pointer, prev.MinKeyLength + 1);
						}
						increaseRecord = true;
						if (gn.MinKeyLength == key.Length)
						{
							await gn.SetInternalValue(key, value);
						}
						else
						{
							await gn.SetExternalValue(key.Span[gn.MinKeyLength], key, value);
						}
						await gn.AssertConsistency();
					}
				}
			}
			if (increaseRecord)
			{
				// Update the record count
				await StorageHelper.WriteLong(2 + Sizes.DefaultPointerLen, RecordCount + 1);
				// Update in-memory
				RecordCount++;
			}
			return increaseRecord;
		}

		internal async IAsyncEnumerable<LTrieValue> EnumerateStartsWith(string startsWith)
		{
			var c = Encoding.UTF8.GetByteCount(startsWith, 0, startsWith.Length);
			using var owner = MemoryPool.Rent(c);
			Encoding.UTF8.GetBytes(startsWith, owner.Memory.Span.Slice(0, c));
			await foreach (var item in EnumerateStartsWith(owner.Memory.Slice(0, c)))
			{
				yield return item;
			}
		}

		internal async IAsyncEnumerable<LTrieValue> EnumerateStartsWith(ReadOnlyMemory<byte> startWithKey)
		{
			var res = await FindBestMatch(startWithKey);

			// In this case, we don't have an exact match, and no children either
			if (res.ValueLink is null && res.MissingValue is byte)
				yield break;
			if (res.ValueLink is Link l && l.Label is null)
			{
				using var record = await ReadValue(res.ValueLink.Pointer);
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
						using var record = await ReadValue(link.Pointer);
						if (record.Key.Span.StartsWith(startWithKey.Span))
							yield return record;
					}
					else
					{
						var childNode = await ReadNode(link.Pointer, res.BestNode.MinKeyLength + nextNodes.Count + 1);
						if (childNode.InternalLink is Link ll)
						{
							using var record = await ReadValue(ll.Pointer);
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
		public async ValueTask<LTrieValue?> GetValue(ReadOnlyMemory<byte> key)
		{
			var res = await FindBestMatch(key);
			if (res.ValueLink is null)
				return null;
			using var r = await ReadValue(res.ValueLink.Pointer);
			if (r.Key.Span.SequenceCompareTo(key.Span) != 0)
				return null;
			return r;
		}
		public async Task<bool> DeleteRow(string key)
		{
			using var owner = GetNameAsBytes(key);
			return await this.DeleteRow(owner.Memory);
		}
		public async ValueTask<bool> DeleteRow(ReadOnlyMemory<byte> key)
		{
			var res = await FindBestMatch(key);
			if (res.ValueLink is null)
				return false;
			bool removedValue = false;
			bool removedNode = false;
			if (res.ValueLink.Label is byte label)
			{
				using var value = await ReadValue(res.ValueLink.Pointer);
				if (value.Key.Span.SequenceCompareTo(key.Span) == 0)
				{
					removedValue = await res.BestNode.RemoveExternalLink(label);
				}
			}
			else
			{
				removedValue = await res.BestNode.RemoveInternalLink();
			}

			if (res.BestNodeParent is LTrieNode parent)
			{
				// If there is a single value link and no internal link, we can delete this node
				if (res.BestNode.GetRemainingValueLink() is Link remainingLink)
				{
					var incomingLink = parent.GetLinkFromPointer(res.BestNode.OwnPointer);
					// Update storage so incoming link now point to the remaininglink's value
					await StorageHelper.WriteExternalLink(incomingLink.OwnPointer, incomingLink.Label!.Value, false, remainingLink.Pointer);

					// Update in-memory
					incomingLink.LinkToNode = false;
					incomingLink.Pointer = remainingLink.Pointer;
					removedNode = true;
				}
				// If we find out that there is
				// no more external links, then we can delete the node
				if (!removedNode && res.BestNode.ExternalLinks.Count == 0)
				{
					var incomingLink = parent.GetLinkFromPointer(res.BestNode.OwnPointer);
					var internalLink = res.BestNode.InternalLink;
					// Update storage, if there is no internal link, the incoming link should be removed
					if (internalLink is null)
					{
						await parent.RemoveExternalLink(incomingLink.Label!.Value);
					}
					// Else, it should be redirected to the value of the internal link
					else
					{
						await StorageHelper.WriteExternalLink(incomingLink.OwnPointer, incomingLink.Label!.Value, false, internalLink.Pointer);
						incomingLink.LinkToNode = false;
						incomingLink.Pointer = internalLink.Pointer;
					}
					removedNode = true;
				}
			}
			
			if (removedValue)
			{
				await StorageHelper.WriteLong(2 + Sizes.DefaultPointerLen, RecordCount - 1);
				RecordCount--;
			}
			if (removedNode)
			{
				NodeCache?.Remove(res.BestNode.OwnPointer);
				if (res.BestNodeParent is LTrieNode n)
					await n.AssertConsistency();
			}
			if (removedValue)
			{
				if (res.BestNode is LTrieNode n)
					await n.AssertConsistency();
			}
			return removedValue;
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
