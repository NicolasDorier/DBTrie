using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.TrieModel
{
	public class LTrieKids
	{
		public LTrie Trie { get; }

		SortedList<byte, LTrieKid> Kids { get; set; }
		public LTrieKid? GetKid(byte kid)
		{
			Kids.TryGetValue(kid, out var k);
			return k;
		}
		internal LTrieKids(LTrieGenerationNode generationNode, LTrie trie, ReadOnlyMemory<byte> memory)
		{
			Trie = trie;
			Kids = new SortedList<byte, LTrieKid>(memory.Length / Sizes.KidLength);
			for (int j = 0; j < memory.Length; j += Sizes.KidLength)
			{
				var slotPointer = generationNode.Pointer + 2 + Sizes.DefaultPointerLen + j;
				var i = memory.Span[j];
				var kidPtr = (long)memory.Span.Slice(j + 2, Sizes.DefaultPointerLen).BigEndianToLongDynamic();
				if (kidPtr == 0 || GetKid(i) is LTrieKid)
				{
					FreeSlotPointers.Enqueue(slotPointer);
					continue;
				}
				var k = new LTrieKid(i);
				Count++;
				k.RecordPointer = kidPtr;
				k.SlotPointer = slotPointer;
				k.LinkToNode = memory.Span[j + 1] == 0;
				Kids.Add(i, k);
			}
		}
		public Queue<long> FreeSlotPointers { get; } = new Queue<long>();
		public int Count { get; internal set; }

		public void SetKid(LTrieKid lTrieKid)
		{
			if (lTrieKid.Value is null)
				throw new InvalidOperationException("Invalid kid");
			byte kid = lTrieKid.Value.Value;
			var oldKid = GetKid(kid);
			if (oldKid is LTrieKid)
			{
				Kids[kid] = lTrieKid;
			}
			else
			{
				Count++;
				Kids.Add(kid, lTrieKid);
			}
		}

		public LTrieKid GetFromRecordPointer(long pointer)
		{
			return this.Enumerate().First(k => k.RecordPointer == pointer);
		}

		public IEnumerable<LTrieKid> Enumerate()
		{
			return Kids.Values;
		}
	}
}
