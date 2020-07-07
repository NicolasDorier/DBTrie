using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.TrieModel
{
	public class LTrieRootNode
	{
		static readonly byte[] DBreezeMagic;
		static LTrieRootNode()
		{
			DBreezeMagic = UTF8Encoding.UTF8.GetBytes("dbreeze.tiesky.com");
		}
		public LTrie Trie { get; }
		public long LinkToZero { get; set; }
		public long RecordCount { get; set; }

		internal LTrieRootNode(LTrie trie, long linkToZero, long recordCount)
		{
			Trie = trie;
			LinkToZero = linkToZero;
			RecordCount = recordCount;
		}

		public static LTrieRootNode ReadFromMemory(LTrie trie, ReadOnlyMemory<byte> memory)
		{
			if (trie == null)
				throw new ArgumentNullException(nameof(trie));
			if (memory.Span[0] != 1)
				throw new FormatException("Impossible to parse the trie");
			if (memory.Span[1] != 1)
				throw new FormatException("Impossible to parse the trie");
			var link = (long)memory.Span.Slice(2, Sizes.DefaultPointerLen).BigEndianToLongDynamic();
			var count = (long)memory.Span.Slice(2 + Sizes.DefaultPointerLen, 8).BigEndianToLongDynamic();
			if (DBreezeMagic.AsSpan().SequenceCompareTo(memory.Span.Slice(2 + Sizes.DefaultPointerLen + 8, DBreezeMagic.Length)) != 0)
				throw new FormatException("Impossible to parse the trie");
			return new LTrieRootNode(trie, link, count);
		}
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
			i += LTrieGenerationNode.WriteNew(output.Slice(i), 1);
			return i;
		}

		public ValueTask<LTrieGenerationNode> ReadGenerationNode()
		{
			return ReadGenerationNode(LinkToZero, 0);
		}
		public async ValueTask<LTrieGenerationNode> ReadGenerationNode(long pointer, int minKeyLength, bool useCache = true)
		{
			LTrieGenerationNode? cached = null;
			Trie.GenerationNodeCache?.TryGetValue(pointer, out cached);
			if (cached is LTrieGenerationNode && useCache)
			{
				if (cached.MinKeyLength != minKeyLength)
					throw new InvalidOperationException("Inconsistent depth, bug in DBTrie");
				if (cached.Pointer != pointer)
					throw new InvalidOperationException("Inconsistent pointer in cache, bug in DBTrie");
				await cached.AssertConsistency();
				return cached;
			}
			using var owner = Trie.MemoryPool.Rent(Sizes.MaximumKidLineLength);
			var memory = owner.Memory.Slice(Sizes.MaximumKidLineLength);
			await Trie.Storage.Read(pointer, memory);
			var node = new LTrieGenerationNode(this, minKeyLength, pointer, memory);
			if (useCache)
				Trie.GenerationNodeCache?.Add(pointer, node);
			return node;
		}
		public async ValueTask<LTrieRow?> GetKey(string key)
		{
			using var owner = Trie.GetNameAsBytes(key);
			return await GetRow(owner.Memory);
		}

		public async ValueTask SetKey(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			var res = await FindBestMatch(key);
			bool increaseRecord = false;
			if (res.ValueKid is null)
			{
				increaseRecord = true;
				var oldPointer = res.BestGenerationNode.Pointer;
				var relocated = await res.BestGenerationNode.SetValueKid(res.MissingKid, key, value);
				if (relocated)
				{
					if (res.BestGenerationNodeParent is LTrieGenerationNode parent)
					{
						var parentKid = parent.Kids.GetFromRecordPointer(oldPointer);
						await this.Trie.StorageWriter.WritePointer(parentKid.SlotPointer + 2, res.BestGenerationNode.Pointer);
						// Update in-memory
						parentKid.RecordPointer = res.BestGenerationNode.Pointer;
						await parent.AssertConsistency();
					}
					else
					{
						await this.Trie.StorageWriter.WritePointer(2, res.BestGenerationNode.Pointer);
						// Update in-memory
						LinkToZero = res.BestGenerationNode.Pointer;
					}
				}
				await res.BestGenerationNode.AssertConsistency();
			}
			else
			{
				// we replace the value kid at this node
				if (res.BestGenerationNode.MinKeyLength == key.Length)
				{
					await res.BestGenerationNode.SetValueKid(key, value);
					return;
				}
				var record = await Trie.ReadRecord(res.ValueKid.RecordPointer);
				// We are replacing a child with the same key
				if (record.Key.Span.SequenceEqual(key.Span))
				{
					await res.BestGenerationNode.SetValueKid(res.ValueKid.Value, key, value);
					return;
				}

				var prev = res.BestGenerationNodeParent;
				var gn = res.BestGenerationNode;
				var maxCommonKeyLength = Math.Min(key.Length, record.Key.Length);
				// We need to convert the value child to a node
				// For this, we expand nodes until there is divergence of key between record key and the search key
				for (int i = res.KeyIndex; i < maxCommonKeyLength; i++)
				{
					if (key.Span[i] != record.Key.Span[i])
						break;
					var oldPointer = gn.Pointer;
					var makeLinkResult = await gn.MakeLinkKid(key.Span[i]);
					if (makeLinkResult.Relocated)
					{
						if (prev is LTrieGenerationNode)
						{
							var parentKid = prev.Kids.GetFromRecordPointer(oldPointer);
							await Trie.StorageWriter.WritePointer(parentKid.SlotPointer + 2, gn.Pointer);

							// Update in-memory
							parentKid.RecordPointer = gn.Pointer;
						}
						else
						{
							await this.Trie.StorageWriter.WritePointer(2, res.BestGenerationNode.Pointer);
							// Update in-memory
							LinkToZero = gn.Pointer;
						}
					}
					prev = gn;
					await gn.AssertConsistency();
					gn = await ReadGenerationNode(makeLinkResult.Kid.RecordPointer, prev.MinKeyLength + 1);
				}
				increaseRecord = true;
				if (maxCommonKeyLength == key.Length)
				{
					await gn.SetValueKid(key, value);
				}
				else
				{
					await gn.SetValueKid(key.Span[maxCommonKeyLength], key, value);
				}
				await gn.AssertConsistency();
			}
			if (increaseRecord)
			{
				// Update the record count
				await Trie.StorageWriter.WriteLong(2 + Sizes.DefaultPointerLen, RecordCount + 1);
				// Update in-memory
				RecordCount++;
			}
		}

		internal async IAsyncEnumerable<LTrieKidRecord> EnumerateStartWith(string startWithKey)
		{
			var c = Encoding.UTF8.GetByteCount(startWithKey, 0, startWithKey.Length);
			using var owner = Trie.MemoryPool.Rent(c);
			Encoding.UTF8.GetBytes(startWithKey, owner.Memory.Span.Slice(0, c));
			await foreach (var item in EnumerateStartWith(owner.Memory.Slice(0, c)))
			{
				yield return item;
			}
		}

		internal async IAsyncEnumerable<LTrieKidRecord> EnumerateStartWith(ReadOnlyMemory<byte> startWithKey)
		{
			var res = await FindBestMatch(startWithKey);

			// In this case, we don't have an exact match, and no children either
			if (res.ValueKid is null && res.MissingKid is byte)
				yield break;
			if (res.ValueKid is LTrieKid)
			{
				var record = await Trie.ReadRecord(res.ValueKid.RecordPointer);
				if (record.Key.Span.StartsWith(startWithKey.Span))
					yield return record;
			}
			var nextNodes = new Stack<IEnumerator<LTrieKid>>();
			nextNodes.Push(res.BestGenerationNode.Kids.Enumerate().GetEnumerator());
			while (nextNodes.TryPop(out var kids))
			{
				while (kids.MoveNext())
				{
					var kid = kids.Current;
					if (!kid.LinkToNode)
					{
						var record = await Trie.ReadRecord(kid.RecordPointer);
						if (record.Key.Span.StartsWith(startWithKey.Span))
							yield return record;
					}
					else
					{
						var kidgn = await ReadGenerationNode(kid.RecordPointer, res.BestGenerationNode.MinKeyLength + nextNodes.Count + 1);
						if (kidgn.ValueKid is LTrieKid kk)
						{
							var record = await Trie.ReadRecord(kk.RecordPointer);
							if (record.Key.Span.StartsWith(startWithKey.Span))
								yield return record;
						}
						nextNodes.Push(kids);
						nextNodes.Push(kidgn.Kids.Enumerate().GetEnumerator());
						break;
					}
				}
			}
		}
		public async ValueTask<LTrieRow?> GetRow(string key)
		{
			var c = Encoding.UTF8.GetByteCount(key, 0, key.Length);
			using var owner = Trie.MemoryPool.Rent(c);
			Encoding.UTF8.GetBytes(key, owner.Memory.Span.Slice(0, c));
			return await GetRow(owner.Memory.Slice(0, c));
		}
		public async ValueTask<LTrieRow?> GetRow(ReadOnlyMemory<byte> key)
		{
			var res = await FindBestMatch(key);
			if (res.ValueKid is null)
				return null;
			using var r = await Trie.ReadRecord(res.ValueKid.RecordPointer);
			if (r.Key.Span.SequenceCompareTo(key.Span) != 0)
				return null;
			return new LTrieRow(Trie.Storage, key)
			{
				Pointer = res.ValueKid.RecordPointer,
				ValueLength = r.ValueLength,
				ValuePointer = r.ValuePointer
			};
		}

		internal async ValueTask<MatchResult> FindBestMatch(ReadOnlyMemory<byte> key)
		{
			LTrieGenerationNode gn = await ReadGenerationNode();
			LTrieGenerationNode? prev = null;
			if (key.Length == 0)
			{
				if (gn.ValueKid is LTrieKid k)
				{
					return new MatchResult(k, gn, prev);
				}
			}

			for (int i = 0; i < key.Length; i++)
			{
				var kid = gn.Kids.GetKid(key.Span[i]);
				if (kid is null)
				{
					return new MatchResult(null, gn, prev)
					{
						MissingKid = key.Span[i],
						KeyIndex = i
					};
				}
				if (!kid.LinkToNode)
				{
					return new MatchResult(kid, gn, prev)
					{
						KeyIndex = i
					};
				}
				prev = gn;
				gn = await ReadGenerationNode(kid.RecordPointer, i + 1);
			}
			return new MatchResult(gn.ValueKid, gn, prev);
		}

		internal class MatchResult
		{
			public MatchResult(LTrieKid? valueKid,
							   LTrieGenerationNode gn,
							   LTrieGenerationNode? gnParent)
			{
				BestGenerationNodeParent = gnParent;
				BestGenerationNode = gn;
				ValueKid = valueKid;
			}
			public LTrieGenerationNode? BestGenerationNodeParent { get; }
			public LTrieGenerationNode BestGenerationNode { get; }
			public LTrieKid? ValueKid { get; set; }
			public byte? MissingKid { get; set; }
			public int KeyIndex { get; internal set; } = -1;
		}
	}
}
