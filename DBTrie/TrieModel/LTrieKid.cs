using System;
using System.Collections.Generic;
using System.Text;

namespace DBTrie.TrieModel
{
	public class LTrieKid
	{
		public LTrieKid(byte? value)
		{
			Value = value;
		}
		public byte? Value { get; }
		public long RecordPointer { get; set; }
		public long SlotPointer { get; set; }
		public bool LinkToNode { get; set; } = true;
	}
}
