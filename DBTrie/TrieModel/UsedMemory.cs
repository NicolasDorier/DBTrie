using System;
using System.Collections.Generic;
using System.Text;

namespace DBTrie.TrieModel
{
	public class UsedMemory
	{
		public long Pointer { get; set; }
		public int Size { get; set; }
		public long PointedBy { get; set; }
		public List<UsedMemory>? PointingTo { get; set; }
	}
}
