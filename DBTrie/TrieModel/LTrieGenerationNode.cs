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
	public class LTrieGenerationNode
	{
		LTrieRootNode root;
		LTrie Trie => root.Trie;
		public int MinKeyLength { get; }
		public LTrieGenerationNode(LTrieRootNode rootNode, int minKeyLength, long pointer, ReadOnlyMemory<byte> memory)
		{
			MinKeyLength = minKeyLength;
			this.root = rootNode;
			Pointer = pointer;
			ushort lineLen = memory.Span.BigEndianToShort();
			LineLength = lineLen;
			var valueKidPointer = (long)memory.Span.Slice(2, Sizes.DefaultPointerLen).BigEndianToLongDynamic();
			if (valueKidPointer != 0)
			{
				ValueKid = CreateValueKid(valueKidPointer);
			}
			Kids = new LTrieKids(this, rootNode.Trie, memory.Slice(2 + Sizes.DefaultPointerLen, lineLen - Sizes.DefaultPointerLen));
		}

		public int LineLength { get; internal set; }

		private LTrieKid CreateValueKid(long valueKidPointer)
		{
			return new LTrieKid(null)
			{
				RecordPointer = valueKidPointer,
				LinkToNode = false,
				SlotPointer = Pointer + 2
			};
		}

		public LTrieKid? ValueKid { get; private set; }
		public LTrieKids Kids { get; }
		public long Pointer { get; internal set; }

		
		internal async ValueTask SetValueKid(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			if (ValueKid is LTrieKid k && await Trie.OverwriteKidValue(k, value))
				return;
			// Write kid value in storage
			var kidPointer = await WriteNewKid(key, value);
			// Update pointer from node to kid value
			await Trie.StorageWriter.WritePointer(Pointer + 2, kidPointer);
			// Update in-memory representation
			ValueKid = CreateValueKid(kidPointer);
		}

		internal async ValueTask<(bool Relocated, LTrieKid Kid)> MakeLinkKid(byte kid)
		{
			if (this.Kids.GetKid(kid) is LTrieKid k)
			{
				if (k.LinkToNode)
					throw new InvalidOperationException("Another link already exists");
				var record = await Trie.ReadRecord(k.RecordPointer);
				var saveValueInNewNode = record.Key.Length == MinKeyLength + 1;
				var newNodePointer = await WriteNewNode(saveValueInNewNode ? 1 : 2);
				if (saveValueInNewNode)
				{
					await Trie.StorageWriter.WritePointer(newNodePointer + 2, record.Pointer);
				}
				// Update the kid in storage
				await Trie.StorageWriter.WriteKid(k.SlotPointer, kid, true, newNodePointer);
				if (!saveValueInNewNode)
				{
					var nextKid = record.Key.Span[this.MinKeyLength + 1];
					await Trie.StorageWriter.WriteKid(newNodePointer + 2 + Sizes.DefaultPointerLen, nextKid, false, record.Pointer);
				}

				// Update in-memory
				k.LinkToNode = true;
				k.RecordPointer = newNodePointer;
				// We don't need to update the in-memory representation of the new node
				// because it currently is not in memory
				return (false, k);
			}
			else
			{
				var relocated = false;
				if (!this.Kids.FreeSlotPointers.TryPeek(out var emptySlotPointer))
				{
					relocated = true;
					await Relocate(Kids.Count + 1);
					emptySlotPointer = Kids.FreeSlotPointers.Peek();
				}
				var newNodePointer = await WriteNewNode(1);
				await this.Trie.StorageWriter.WriteKid(emptySlotPointer, kid, false, newNodePointer);

				// Update in-memory
				k = new LTrieKid(kid)
				{
					LinkToNode = true,
					RecordPointer = newNodePointer,
					SlotPointer = emptySlotPointer
				};
				Kids.SetKid(k);
				return (relocated, k);
			}
		}

		private async ValueTask<long> WriteNewNode(int neededSlots)
		{
			var reservedSlots = LTrieGenerationNode.GetSlotReservationCount(neededSlots);
			var nodeSize = 2 + Sizes.DefaultPointerLen + (reservedSlots * Sizes.KidLength);
			using var owner = Trie.MemoryPool.Rent(nodeSize);
			owner.Memory.Span.ToBigEndian((ushort)(nodeSize - 2));
			owner.Memory.Span.Slice(2).ToBigEndianDynamic(0);
			owner.Memory.Span.Slice(2 + Sizes.DefaultPointerLen, reservedSlots * Sizes.KidLength).Fill(0);
			return await Trie.Storage.WriteToEnd(owner.Memory.Slice(0, nodeSize));
		}

		/// <summary>
		/// Set a kid to some value
		/// </summary>
		/// <param name="kid">kid</param>
		/// <param name="key">full key</param>
		/// <param name="value">full value</param>
		/// <returns>True if this generation node has been relocated</returns>
		internal async ValueTask<bool> SetValueKid(byte kid, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			if (this.Kids.GetKid(kid) is LTrieKid k)
			{
				if (await Trie.OverwriteKidValue(k, value))
					return false;
				// Write kid value in storage
				var kidPointer = await WriteNewKid(key, value);
				//Update the pointer in storage
				await this.Trie.StorageWriter.WriteKid(k.SlotPointer, kid, false, kidPointer);
				// Update in-memory representation
				k.LinkToNode = false;
				k.RecordPointer = kidPointer;
				return false;
			}
			else
			{
				bool relocated = false;
				if (!this.Kids.FreeSlotPointers.TryPeek(out var emptySlotPointer))
				{
					// We need to relocate the current node somewhere else to
					// increase the number of slots
					relocated = true;
					await Relocate(Kids.Count + 1);
					emptySlotPointer = Kids.FreeSlotPointers.Peek();
				}

				// Let's add the new value kid in one of the slot
				{
					// Write kid value in storage
					var kidPointer = await WriteNewKid(key, value);
					//Update the pointer in storage
					await Trie.StorageWriter.WriteKid(emptySlotPointer, kid, false, kidPointer);
					// Update in-memory representation
					this.Kids.FreeSlotPointers.Dequeue();
					this.Kids.SetKid(new LTrieKid(kid)
					{
						LinkToNode = false,
						RecordPointer = kidPointer,
						SlotPointer = emptySlotPointer
					});
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
			var stored = await root.ReadGenerationNode(Pointer, MinKeyLength, false);
			if (stored.Kids.Count != Kids.Count)
				throw new Exception("stored.Kids.Count != Kids.Count");
			if (stored.ValueKid?.RecordPointer != ValueKid?.RecordPointer)
				throw new Exception("stored.ValueKid?.RecordPointer != ValueKid?.RecordPointer");
			for (int i = 0; i < 256; i++)
			{
				var k1 = stored.Kids.GetKid((byte)i);
				var k2 = Kids.GetKid((byte)i);
				if (k1?.SlotPointer != k2?.SlotPointer)
					throw new Exception("stored?.SlotPointer != this?.SlotPointer");
				if (k1?.RecordPointer != k2?.RecordPointer)
					throw new Exception("stored?.RecordPointer != this?.RecordPointer");
				if (k1?.LinkToNode != k2?.LinkToNode)
					throw new Exception("stored?.LinkToNode != this?.LinkToNode");
			}
		}

		private async Task Relocate(int neededSlots)
		{
			var newSlotCount = GetSlotReservationCount(neededSlots);
			var lineLen = Sizes.DefaultPointerLen + (newSlotCount * Sizes.KidLength);
			using var owner = Trie.MemoryPool.Rent(2 + lineLen);
			int offset = 0;
			owner.Memory.Span.ToBigEndian((ushort)lineLen);
			offset += 2;
			if (ValueKid?.RecordPointer is long recordPointer)
				owner.Memory.Span.Slice(offset).ToBigEndianDynamic((ulong)recordPointer);
			else
				owner.Memory.Span.Slice(offset).ToBigEndianDynamic(0);
			offset += Sizes.DefaultPointerLen;
			foreach (var subKid in Kids.Enumerate().OrderBy(k => k.SlotPointer))
			{
				owner.Memory.Span[offset] = subKid.Value!.Value;
				owner.Memory.Span[offset + 1] = (byte)(subKid.LinkToNode ? 0 : 1);
				offset += 2;
				owner.Memory.Span.Slice(offset).ToBigEndianDynamic((ulong)subKid.RecordPointer);
				offset += Sizes.DefaultPointerLen;
			}
			owner.Memory.Span.Slice(offset).Fill(0);
			var newNodePointer = await Trie.Storage.WriteToEnd(owner.Memory.Slice(0, 2 + lineLen));
			// Update in-memory representation
			RelocateInMemory(newNodePointer);
			for (long location = Pointer + 2 + LineLength;
				location < Pointer + 2 + lineLen;
				location += Sizes.KidLength)
			{
				Kids.FreeSlotPointers.Enqueue(location);
			}
			LineLength = lineLen;
		}

		internal async ValueTask<bool> SetValueKid(byte? kid, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			if (kid is byte b)
				return await SetValueKid(b, key, value);
			await SetValueKid(key, value);
			return false;
		}

		private void RelocateInMemory(long newPointer)
		{
			var oldPointer = Pointer;
			var offset = newPointer - Pointer;
			Pointer += offset;
			foreach (var k in Kids.Enumerate())
			{
				k.SlotPointer += offset;
			}
			Trie.GenerationNodeCache?.Relocate(oldPointer, newPointer);
		}

		public static int GetSlotReservationCount(int kc)
		{
			if (kc > 256)
				throw new ArgumentOutOfRangeException(nameof(kc), "Kid count should be maximum 256");
			if (kc < 2) return 1;
			if (kc == 2) return 2;
			if (kc < 5) return 4;
			if (kc < 9) return 8;
			if (kc < 17) return 16;
			if (kc < 33) return 32;
			if (kc < 65) return 64;
			if (kc < 129) return 128;
			return 256;
		}

		internal async ValueTask<long> WriteNewKid(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			using var owner = Trie.MemoryPool.Rent(LTrieKidRecord.GetRecordSize(key, value));
			var len = LTrieKidRecord.WriteToSpan(owner.Memory.Span, key.Span, value.Span);
			var memory = owner.Memory.Slice(0, len);
			var kidPointer = await Trie.Storage.WriteToEnd(memory);
			return kidPointer;
		}
	}
}
