using System;
using System.Collections.Generic;
using System.Text;

namespace DBTrie.TrieModel
{
	internal class Link
	{
		public Link(byte? label)
		{
			Label = label;
		}
		public byte? Label { get; set; }
		public long Pointer { get; set; }
		public long OwnPointer { get; set; }
		public bool LinkToNode { get; set; } = true;
	}
}
