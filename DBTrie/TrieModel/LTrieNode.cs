using System;
using DBTrie.Storage;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.TrieModel
{
	internal class LTrieNode
	{
		LTrie Trie { get; }
		public int MinKeyLength { get; }
		internal LTrieNode(LTrie trie, int minKeyLength, long pointer, ReadOnlyMemory<byte> memory)
		{
			Trie = trie;
			MinKeyLength = minKeyLength;
			OwnPointer = pointer;
			ushort lineLen = memory.Span.BigEndianToShort();
			LineLength = lineLen;
			var internalLinkPointer = (long)memory.Span.Slice(2, Sizes.DefaultPointerLen).BigEndianToLongDynamic();
			if (internalLinkPointer != 0)
			{
				InternalLink = new Link(null)
				{
					Pointer = internalLinkPointer,
					LinkToNode = false,
					OwnPointer = OwnPointer + 2
				};
			}

			memory = memory.Slice(2 + Sizes.DefaultPointerLen, lineLen - Sizes.DefaultPointerLen);
			links = new SortedList<byte, Link>(memory.Length / Sizes.ExternalLinkLength);
			for (int j = 0; j < memory.Length; j += Sizes.ExternalLinkLength)
			{
				var slotPointer = OwnPointer + 2 + Sizes.DefaultPointerLen + j;
				var i = memory.Span[j];
				var linkPointer = (long)memory.Span.Slice(j + 2, Sizes.DefaultPointerLen).BigEndianToLongDynamic();
				if (linkPointer == 0 || GetLink(i) is Link)
				{
					FreeSlotPointers.Enqueue(slotPointer);
					continue;
				}
				var l = new Link(i);
				l.Pointer = linkPointer;
				l.OwnPointer = slotPointer;
				l.LinkToNode = memory.Span[j + 1] == 0;
				links.Add(i, l);
			}
		}

		SortedList<byte, Link> links { get; } = new SortedList<byte, Link>();
		public Link? GetLink(byte value)
		{
			links.TryGetValue(value, out var k);
			return k;
		}
		public Queue<long> FreeSlotPointers { get; } = new Queue<long>();

		public Link UpdateInternalLink(bool linkToNode, long pointer)
		{
			if (InternalLink is Link l)
			{
				l.Pointer = pointer;
				l.LinkToNode = false;
				l.OwnPointer = OwnPointer + 2;
			}
			else
			{
				l = new Link(null)
				{
					Pointer = pointer,
					LinkToNode = false,
					OwnPointer = OwnPointer + 2
				};
				InternalLink = l;
			}
			return l;
		}
		public Link UpdateExternalLink(long ownPointer, byte label, bool linkToNode, long pointer)
		{
			if (GetLink(label) is Link l)
			{
				l.OwnPointer = ownPointer;
				l.LinkToNode = linkToNode;
				l.Pointer = pointer;
			}
			else
			{
				l = new Link(label)
				{
					OwnPointer = ownPointer,
					LinkToNode = linkToNode,
					Pointer = pointer
				};
				links.Add(label, l);
			}
			return l;
		}

		public Link GetLinkFromPointer(long pointer)
		{
			return this.ExternalLinks.First(l => l.Pointer == pointer);
		}

		public IEnumerable<Link> ExternalLinks => links.Values;

		public int LineLength { get; internal set; }

		public Link? InternalLink { get; private set; }
		public long OwnPointer { get; internal set; }
		
		internal async ValueTask SetValue(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			if (InternalLink is Link l && await Trie.TryOverwriteValue(l, value))
				return;
			// Write value in storage
			var linkPointer = await WriteNewValue(key, value);
			// Update internal link
			await Trie.StorageWriter.WritePointer(OwnPointer + 2, linkPointer);
			// Update in-memory representation
			UpdateInternalLink(false, linkPointer);
		}

		internal async ValueTask<(bool Relocated, Link Link)> SetLinkToNode(byte label)
		{
			if (this.GetLink(label) is Link l)
			{
				if (l.LinkToNode)
					throw new InvalidOperationException("Another link already exists");
				var record = await Trie.ReadValue(l.Pointer);
				var internalLink = record.Key.Length == MinKeyLength + 1;
				var newNodePointer = await WriteNewNode(internalLink ? 1 : 2);
				if (internalLink)
				{
					await Trie.StorageWriter.WritePointer(newNodePointer + 2, record.Pointer);
				}
				// Update the link in storage
				await Trie.StorageWriter.WriteExternalLink(l.OwnPointer, label, true, newNodePointer);
				if (!internalLink)
				{
					var nextLabel = record.Key.Span[this.MinKeyLength + 1];
					await Trie.StorageWriter.WriteExternalLink(newNodePointer + 2 + Sizes.DefaultPointerLen, nextLabel, false, record.Pointer);
				}

				// Update in-memory
				l.LinkToNode = true;
				l.Pointer = newNodePointer;
				// We don't need to update the in-memory representation of the new node
				// because it currently is not in memory
				return (false, l);
			}
			else
			{
				var relocated = false;
				if (!this.FreeSlotPointers.TryPeek(out var emptySlotPointer))
				{
					relocated = true;
					await Relocate(links.Count + 1);
					emptySlotPointer = FreeSlotPointers.Peek();
				}
				var newNodePointer = await WriteNewNode(1);
				await this.Trie.StorageWriter.WriteExternalLink(emptySlotPointer, label, false, newNodePointer);

				// Update in-memory
				l = UpdateExternalLink(emptySlotPointer, label, true, newNodePointer);
				return (relocated, l);
			}
		}

		internal static int WriteNew(Span<byte> output, int neededSlots)
		{
			var reservedSlots = LTrieNode.GetSlotReservationCount(neededSlots);
			var nodeSize = GetNodeSize(reservedSlots);
			output.ToBigEndian((ushort)(nodeSize - 2));
			output.Slice(2).ToBigEndianDynamic(0);
			output.Slice(2 + Sizes.DefaultPointerLen, reservedSlots * Sizes.ExternalLinkLength).Fill(0);
			return nodeSize;
		}

		private async ValueTask<long> WriteNewNode(int neededSlots)
		{
			var reservedSlots = LTrieNode.GetSlotReservationCount(neededSlots);
			var nodeSize = GetNodeSize(reservedSlots);
			using var owner = Trie.MemoryPool.Rent(nodeSize);
			WriteNew(owner.Memory.Span, neededSlots);
			return await Trie.Storage.WriteToEnd(owner.Memory.Slice(0, nodeSize));
		}

		private static int GetNodeSize(int reservedSlots)
		{
			return 2 + Sizes.DefaultPointerLen + (reservedSlots * Sizes.ExternalLinkLength);
		}

		internal async ValueTask<bool> SetValue(byte label, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			if (this.GetLink(label) is Link k)
			{
				if (await Trie.TryOverwriteValue(k, value))
					return false;
				// Write value in storage
				var valuePointer = await WriteNewValue(key, value);
				//Update the external link in storage
				await this.Trie.StorageWriter.WriteExternalLink(k.OwnPointer, label, false, valuePointer);
				// Update in-memory representation
				k.LinkToNode = false;
				k.Pointer = valuePointer;
				return false;
			}
			else
			{
				bool relocated = false;
				if (!this.FreeSlotPointers.TryPeek(out var emptySlotPointer))
				{
					// We need to relocate the current node somewhere else to
					// increase the number of slots
					relocated = true;
					await Relocate(links.Count + 1);
					emptySlotPointer = FreeSlotPointers.Peek();
				}

				// Let's add the new external link in one of the slot
				{
					// Write value in storage
					var valuePointer = await WriteNewValue(key, value);
					//Update the link in storage
					await Trie.StorageWriter.WriteExternalLink(emptySlotPointer, label, false, valuePointer);
					// Update in-memory representation
					this.FreeSlotPointers.Dequeue();
					this.UpdateExternalLink(emptySlotPointer, label, false, valuePointer);
				}
				return relocated;
			}
		}

		/// <summary>
		/// Used by tests to make sure there is no difference
		/// between storage and in-memory representation
		/// of the trie
		/// </summary>
		/// <returns></returns>
		internal async ValueTask AssertConsistency()
		{
			if (!Trie.ConsistencyCheck)
				return;
			var stored = await Trie.ReadNode(OwnPointer, MinKeyLength, false);
			if (stored.links.Count != links.Count)
				throw new Exception("stored.links.Count != links.Count");
			if (stored.InternalLink?.Pointer != InternalLink?.Pointer)
				throw new Exception("stored.InternalLink?.RecordPointer != InternalLink?.RecordPointer");
			for (int i = 0; i < 256; i++)
			{
				var k1 = stored.GetLink((byte)i);
				var k2 = GetLink((byte)i);
				if (k1?.OwnPointer != k2?.OwnPointer)
					throw new Exception("stored?.SlotPointer != this?.SlotPointer");
				if (k1?.Pointer != k2?.Pointer)
					throw new Exception("stored?.RecordPointer != this?.RecordPointer");
				if (k1?.LinkToNode != k2?.LinkToNode)
					throw new Exception("stored?.LinkToNode != this?.LinkToNode");
			}
		}

		private async Task Relocate(int neededSlots)
		{
			var newSlotCount = GetSlotReservationCount(neededSlots);
			var lineLen = Sizes.DefaultPointerLen + (newSlotCount * Sizes.ExternalLinkLength);
			using var owner = Trie.MemoryPool.Rent(2 + lineLen);
			int offset = 0;
			owner.Memory.Span.ToBigEndian((ushort)lineLen);
			offset += 2;
			if (InternalLink?.Pointer is long recordPointer)
				owner.Memory.Span.Slice(offset).ToBigEndianDynamic((ulong)recordPointer);
			else
				owner.Memory.Span.Slice(offset).ToBigEndianDynamic(0);
			offset += Sizes.DefaultPointerLen;
			foreach (var externalLink in ExternalLinks.OrderBy(k => k.OwnPointer))
			{
				owner.Memory.Span[offset] = externalLink.Label!.Value;
				owner.Memory.Span[offset + 1] = (byte)(externalLink.LinkToNode ? 0 : 1);
				offset += 2;
				owner.Memory.Span.Slice(offset).ToBigEndianDynamic((ulong)externalLink.Pointer);
				offset += Sizes.DefaultPointerLen;
			}
			owner.Memory.Span.Slice(offset).Fill(0);
			var newNodePointer = await Trie.Storage.WriteToEnd(owner.Memory.Slice(0, 2 + lineLen));
			// Update in-memory representation
			RelocateInMemory(newNodePointer);
			for (long location = OwnPointer + 2 + LineLength;
				location < OwnPointer + 2 + lineLen;
				location += Sizes.ExternalLinkLength)
			{
				FreeSlotPointers.Enqueue(location);
			}
			LineLength = lineLen;
		}

		internal async ValueTask<bool> SetValue(byte? linkLabel, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			if (linkLabel is byte b)
				return await SetValue(b, key, value);
			await SetValue(key, value);
			return false;
		}

		private void RelocateInMemory(long newPointer)
		{
			var oldPointer = OwnPointer;
			var offset = newPointer - OwnPointer;
			OwnPointer += offset;
			foreach (var k in ExternalLinks)
			{
				k.OwnPointer += offset;
			}
			Trie.GenerationNodeCache?.Relocate(oldPointer, newPointer);
		}

		public static int GetSlotReservationCount(int kl)
		{
			if (kl > 256)
				throw new ArgumentOutOfRangeException(nameof(kl), "Label count should be maximum 256");
			if (kl < 2) return 1;
			if (kl == 2) return 2;
			if (kl < 5) return 4;
			if (kl < 9) return 8;
			if (kl < 17) return 16;
			if (kl < 33) return 32;
			if (kl < 65) return 64;
			if (kl < 129) return 128;
			return 256;
		}

		internal async ValueTask<long> WriteNewValue(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			using var owner = Trie.MemoryPool.Rent(LTrieValue.GetSize(key, value));
			var len = LTrieValue.WriteToSpan(owner.Memory.Span, key.Span, value.Span);
			var memory = owner.Memory.Slice(0, len);
			return await Trie.Storage.WriteToEnd(memory);
		}
	}
}
