using DBTrie.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Diagnostics;
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

		public async ValueTask<bool> SetValue(string key, string value)
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
			var link = (long)span.Slice(2).BigEndianToLongDynamic();
			var count = (long)span.Slice(2 + Sizes.DefaultPointerLen).ReadUInt64BigEndian();
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
			await storage.Flush();
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
			ushort keySize = memory.ToReadOnly().Span.Slice(1).ReadUInt16BigEndian();
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
			while (nodesToVisit.Count > 0)
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
		public ValueTask<LTrieNodeStruct> ReadNodeStruct()
		{
			return ReadNodeStruct(RootPointer);
		}

		public async ValueTask<LTrieNodeStruct> ReadNodeStruct(long pointer)
		{
			// We try reading directly from the buffer without copy
			if (Storage.TryDirectRead(pointer, Sizes.NodeMinSize, out var memory))
			{
				return new LTrieNodeStruct(pointer, memory.Span);
			}

			// If that fail, fallback
			using var owner = MemoryPool.Rent(Sizes.NodeMinSize);
			await Storage.Read(pointer, owner.Memory);
			return new LTrieNodeStruct(pointer, owner.Memory.Span);
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

			var node = await FetchNode(pointer, minKeyLength);
			if (useCache)
				NodeCache?.Add(pointer, node);
			return node;
		}

		private async ValueTask<LTrieNode> ToLTrieNodeObject(LTrieNodeStruct nodeStruct, int minKeyLength)
		{
			LTrieNode? cached = null;
			NodeCache?.TryGetValue(nodeStruct.OwnPointer, out cached);
			if (cached is LTrieNode)
			{
				if (cached.MinKeyLength != minKeyLength)
					throw new InvalidOperationException("Inconsistent depth, bug in DBTrie");
				if (cached.OwnPointer != nodeStruct.OwnPointer)
					throw new InvalidOperationException("Inconsistent pointer in cache, bug in DBTrie");
				await cached.AssertConsistency();
				return cached;
			}
			var node = await FetchNodeFromStruct(nodeStruct, minKeyLength);
			NodeCache?.Add(nodeStruct.OwnPointer, node);
			return node;
		}

		private async ValueTask<LTrieNode> FetchNode(long pointer, int minKeyLength)
		{
			int lineLength = -1;
			// We try reading directly from the buffer without copy
			if (Storage.TryDirectRead(pointer, Sizes.NodeMinSize, out var memory))
			{
				lineLength = memory.Span.ReadUInt16BigEndian();
				var nodeSize = 2 + lineLength;
				if (nodeSize == Sizes.NodeMinSize)
					return new LTrieNode(this, minKeyLength, pointer, memory);
				if (nodeSize < Sizes.NodeMinSize)
					throw new FormatException("Invalid node size");
				if (Storage.TryDirectRead(pointer, nodeSize, out memory))
					return new LTrieNode(this, minKeyLength, pointer, memory);
			}

			// If that fail, fallback
			{
				var nodeSize = lineLength == -1 ? Sizes.MaximumNodeSize : 2 + lineLength;
				using var owner = MemoryPool.Rent(nodeSize);
				var outputMemory = owner.Memory.Slice(0, nodeSize);
				await Storage.Read(pointer, outputMemory);
				if (lineLength == -1)
				{
					lineLength = ((ReadOnlySpan<byte>)outputMemory.Span).ReadUInt16BigEndian();
					outputMemory = outputMemory.Slice(0, 2 + lineLength);
				}
				return new LTrieNode(this, minKeyLength, pointer, outputMemory);
			}
		}

		public async ValueTask<LTrieValue?> GetValue(string key)
		{
			using var owner = GetNameAsBytes(key);
			return await GetValue(owner.Memory);
		}
		public async ValueTask<string?> GetValueString(string key)
		{
			using var owner = GetNameAsBytes(key);
			using var val = await GetValue(owner.Memory);
			if (val is null)
				return null;
			return await val.ReadValueString();
		}

		public async ValueTask<bool> SetValue(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			AssertNotEnumerating();
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
					var valueKey = record.Key;
					// We are replacing a child with the same key
					if (valueKey.Span.SequenceEqual(key.Span))
					{
						increaseRecord = false;
						await res.BestNode.SetValue(res.ValueLink.Label, key, value);
					}
					else
					{
						var prev = res.BestNodeParent;
						var gn = res.BestNode;
						var maxCommonKeyLength = Math.Min(key.Length, valueKey.Length);
						// We need to convert the value child to a node
						// For this, we expand nodes until there is divergence of key between record key and the search key
						for (int i = res.KeyIndex; i < maxCommonKeyLength; i++)
						{
							if (key.Span[i] != valueKey.Span[i])
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

		internal IAsyncEnumerable<LTrieValue> EnumerateStartsWith(string startsWith)
		{
			return EnumerateStartsWith(startsWith, EnumerationOrder.Ordered);
		}
		internal async IAsyncEnumerable<LTrieValue> EnumerateStartsWith(string startsWith, EnumerationOrder order)
		{
			var c = Encoding.UTF8.GetByteCount(startsWith, 0, startsWith.Length);
			using var owner = MemoryPool.Rent(c);
			Encoding.UTF8.GetBytes(startsWith, owner.Memory.Span.Slice(0, c));
			await foreach (var item in EnumerateStartsWith(owner.Memory.Slice(0, c), order))
			{
				yield return item;
			}
		}

		async ValueTask<LTrieValue?> GetValueIfStartWith(ReadOnlyMemory<byte> startWithKey, long pointer)
		{
			var record = await ReadValue(pointer);
			if (record.Key.Span.StartsWith(startWithKey.Span))
			{
				return record;
			}
			else
			{
				record.Dispose();
				return null;
			}
		}
		bool enumerating = false;
		void AssertNotEnumerating()
		{
			if (enumerating)
				throw new InvalidOperationException("Impossible to do this operation while enumerating");
		}
		internal IAsyncEnumerable<LTrieValue> EnumerateStartsWith(ReadOnlyMemory<byte> startWithKey)
		{
			return EnumerateStartsWith(startWithKey, EnumerationOrder.Ordered);
		}
		internal async IAsyncEnumerable<LTrieValue> EnumerateStartsWith(ReadOnlyMemory<byte> startWithKey, EnumerationOrder order)
		{
			try
			{
				enumerating = true;
				var res = await FindBestMatch(startWithKey);
				// In this case, we don't have an exact match, and no children either
				if (res.ValueLink is null && res.MissingValue is byte)
					yield break;
				if (res.ValueLink is Link l && l.Label is null)
				{
					var record = await GetValueIfStartWith(startWithKey, res.ValueLink.Pointer);
					if (record is LTrieValue)
						yield return record;
				}
				var nextNodes = new Stack<(int Depth, LTrieNodeStruct Node, int NextLinkIndex)>();

				(int Depth, LTrieNodeStruct Node, int NextLinkIndex) current =
					(res.BestNode.MinKeyLength, await ReadNodeStruct(res.BestNode.OwnPointer), 0);
				while (current.Depth > -1 || nextNodes.TryPop(out current))
				{
					LTrieNodeExternalLinkStruct link = default;
					if (current.NextLinkIndex == 0 && current.Node.ExternalLinkSlotCount == 1)
					{
						link = current.Node.FirstExternalLink;
					}
					else
					{
						link = await GetOrderedExternalLink(current.Node, current.NextLinkIndex, order);
					}
					if (link.Pointer == 0)
					{
						current.Depth = -1;
						continue;
					}
					if (!link.LinkToNode)
					{
						var record = await GetValueIfStartWith(startWithKey, link.Pointer);
						if (record is LTrieValue)
							yield return record;
						current.NextLinkIndex++;
						if (current.NextLinkIndex >= current.Node.ExternalLinkSlotCount)
							current.Depth = -1;
					}
					else
					{
						var childNode = await ReadNodeStruct(link.Pointer);
						if (childNode.InternalLinkPointer != 0)
						{
							var record = await GetValueIfStartWith(startWithKey, childNode.InternalLinkPointer);
							if (record is LTrieValue)
								yield return record;
						}
						current.NextLinkIndex++;
						if (current.NextLinkIndex < current.Node.ExternalLinkSlotCount)
							nextNodes.Push((current.Depth, current.Node, current.NextLinkIndex));
						current = (current.Depth + 1, childNode, 0);
						if (current.NextLinkIndex >= childNode.ExternalLinkSlotCount)
							current.Depth = -1;
					}
				}
			}
			finally
			{
				enumerating = false;
			}
		}
		public async ValueTask<LTrieValue?> GetValue(ReadOnlyMemory<byte> key)
		{
			var res = await FindBestMatch(key);
			if (res.ValueLink is null)
				return null;
			var r = await ReadValue(res.ValueLink.Pointer);
			if (r.Key.Span.SequenceCompareTo(key.Span) != 0)
			{
				r.Dispose();
				return null;
			}
			return r;
		}
		public async Task<bool> DeleteRow(string key)
		{
			using var owner = GetNameAsBytes(key);
			return await this.DeleteRow(owner.Memory);
		}
		public async ValueTask<bool> DeleteRow(ReadOnlyMemory<byte> key)
		{
			AssertNotEnumerating();
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
			LTrieNodeStruct gn = await ReadNodeStruct();
			LTrieNodeStruct prev = default;
			if (key.Length == 0)
			{
				return await CreateMatchResult(0, gn.GetInternalLinkObject(), gn, prev);
			}
			for (int i = 0; i < key.Length; i++)
			{
				LTrieNodeExternalLinkStruct externalLink = default;
				if (key.Span[i] == gn.FirstExternalLink.Value)
				{
					externalLink = gn.FirstExternalLink;
				}
				else if (gn.ExternalLinkSlotCount > 1)
				{
					externalLink = await GetRemainingExternalLink(gn, key.Span[i]);
				}
				if (externalLink.Pointer == 0)
				{
					var result = await CreateMatchResult(i, null, gn, prev);
					result.MissingValue = key.Span[i];
					result.KeyIndex = i;
					return result;
				}
				if (!externalLink.LinkToNode)
				{
					var result = await CreateMatchResult(i, externalLink.ToLinkObject(), gn, prev);
					result.KeyIndex = i;
					return result;
				}
				prev = gn;
				gn = await ReadNodeStruct(externalLink.Pointer);
			}
			return await CreateMatchResult(key.Length, gn.GetInternalLinkObject(), gn, prev);
		}

		private async ValueTask<LTrieNodeExternalLinkStruct> GetRemainingExternalLink(LTrieNodeStruct gn, byte value)
		{
			IMemoryOwner<byte>? owner = null;
			var restLength = gn.GetRestLength();
			var secondLinkPointer = gn.GetSecondExternalLinkOwnPointer();
			if (!Storage.TryDirectRead(secondLinkPointer, restLength, out var memory))
			{
				owner = MemoryPool.Rent(restLength);
				var outputMemory = owner.Memory.Slice(0, restLength);
				await Storage.Read(secondLinkPointer, outputMemory);
				memory = outputMemory;
			}
			for (int linkIndex = 0; linkIndex < gn.ExternalLinkSlotCount - 1; linkIndex++)
			{
				var linkOwnPointer = secondLinkPointer + linkIndex * Sizes.ExternalLinkLength;
				var l = new LTrieNodeExternalLinkStruct(linkOwnPointer, memory.Span.Slice(linkIndex * Sizes.ExternalLinkLength, Sizes.ExternalLinkLength));
				if (l.Pointer != 0 && l.Value == value)
				{
					owner?.Dispose();
					return l;
				}
			}
			owner?.Dispose();
			return default;
		}
		private async ValueTask<LTrieNodeExternalLinkStruct> GetOrderedExternalLink(LTrieNodeStruct gn, int orderedIndex, EnumerationOrder order)
		{
			// We don't have to call this method, we should just pick the first link in this case
			Debug.Assert(orderedIndex > 0 || (orderedIndex == 0 && gn.ExternalLinkSlotCount > 1));
			IMemoryOwner<byte>? owner = null;
			var restLength = gn.GetRestLength();
			var secondLinkPointer = gn.GetSecondExternalLinkOwnPointer();
			if (!Storage.TryDirectRead(secondLinkPointer, restLength, out var memory))
			{
				owner = MemoryPool.Rent(restLength);
				var outputMemory = owner.Memory.Slice(0, restLength);
				await Storage.Read(secondLinkPointer, outputMemory);
				memory = outputMemory;
			}
			if (order == EnumerationOrder.Unordered)
			{
				var currentIndex = 0;
				if (gn.FirstExternalLink.Pointer != 0)
				{
					if (currentIndex == orderedIndex)
					{
						owner?.Dispose();
						return gn.FirstExternalLink;
					}
					currentIndex++;
				}
				for (int linkIndex = 0; linkIndex < gn.ExternalLinkSlotCount - 1; linkIndex++)
				{
					var linkOwnPointer = secondLinkPointer + linkIndex * Sizes.ExternalLinkLength;
					var l = new LTrieNodeExternalLinkStruct(linkOwnPointer, memory.Span.Slice(linkIndex * Sizes.ExternalLinkLength, Sizes.ExternalLinkLength));
					if (l.Pointer != 0)
					{
						if (currentIndex == orderedIndex)
						{
							owner?.Dispose();
							return l;
						}
						currentIndex++;
					}
				}
				owner?.Dispose();
				return default;
			}
			else // order == EnumerationOrder.Ordered
			{
				var links = new SortedList<byte, LTrieNodeExternalLinkStruct>(gn.ExternalLinkSlotCount);
				if (gn.FirstExternalLink.Pointer != 0)
					links.Add(gn.FirstExternalLink.Value, gn.FirstExternalLink);
				for (int linkIndex = 0; linkIndex < gn.ExternalLinkSlotCount - 1; linkIndex++)
				{
					var linkOwnPointer = secondLinkPointer + linkIndex * Sizes.ExternalLinkLength;
					var l = new LTrieNodeExternalLinkStruct(linkOwnPointer, memory.Span.Slice(linkIndex * Sizes.ExternalLinkLength, Sizes.ExternalLinkLength));
					if (l.Pointer != 0)
					{
						links.Add(l.Value, l);
					}
				}
				owner?.Dispose();
				return links.Skip(orderedIndex).Select(c => c.Value).FirstOrDefault();
			}
		}

		private async ValueTask<MatchResult> CreateMatchResult(int minKeyLength, Link? valueLink, LTrieNodeStruct gn, LTrieNodeStruct prev)
		{
			return new MatchResult(valueLink,
				await ToLTrieNodeObject(gn, minKeyLength),
				prev.OwnPointer == 0 ? null : await ToLTrieNodeObject(prev, minKeyLength - 1));
		}

		
		private async ValueTask<LTrieNode> FetchNodeFromStruct(LTrieNodeStruct nodeStruct, int minKeyLength)
		{
			if (nodeStruct.ExternalLinkSlotCount == 1)
			{
				return new LTrieNode(this, minKeyLength, nodeStruct);
			}
			var restLength = nodeStruct.GetRestLength();
			var secondLinkPointer = nodeStruct.GetSecondExternalLinkOwnPointer();
			if (Storage.TryDirectRead(secondLinkPointer, restLength, out var memory))
			{
				return new LTrieNode(this, minKeyLength, nodeStruct, memory.Span.Slice(0, restLength));
			}
			using var owner = MemoryPool.Rent(restLength);
			var outputMemory = owner.Memory.Slice(0, restLength);
			await Storage.Read(secondLinkPointer, outputMemory);
			return new LTrieNode(this, minKeyLength, nodeStruct, outputMemory.Span);
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
