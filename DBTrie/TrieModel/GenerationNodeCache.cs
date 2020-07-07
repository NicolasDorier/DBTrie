using System;
using System.Collections.Generic;
using System.Text;

namespace DBTrie.TrieModel
{
	internal class GenerationNodeCache : Dictionary<long, LTrieNode>
	{
		public GenerationNodeCache()
		{
		}

		public void Relocate(long oldPointer, long newPointer)
		{
			if (oldPointer == newPointer)
				return;
			if (!this.TryGetValue(oldPointer, out var node) ||
				!this.TryAdd(newPointer, node) ||
				!this.Remove(oldPointer))
			{
				throw new InvalidOperationException("Bug in DBTrie (Code: 20184)");
			}
		}
	}
}
