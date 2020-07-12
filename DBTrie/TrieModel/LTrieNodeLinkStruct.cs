using System;
using System.Collections.Generic;
using System.Drawing;
using System.Text;

namespace DBTrie.TrieModel
{
	internal readonly struct LTrieNodeExternalLinkStruct
	{
		public LTrieNodeExternalLinkStruct(long ownPointer, ReadOnlySpan<byte> span)
		{
			OwnPointer = ownPointer;
			Value = span[0];
			LinkToNode = span[1] == 0;
			Pointer = (long)span.Slice(2).BigEndianToLongDynamic();
		}
		public readonly long OwnPointer;
		public readonly long Pointer;
		public readonly bool LinkToNode;
		public readonly byte Value;

		public readonly Link ToLinkObject()
		{
			return new Link(Value)
			{
				LinkToNode = LinkToNode,
				Pointer = Pointer,
				OwnPointer = OwnPointer
			};
		}
	}
}
