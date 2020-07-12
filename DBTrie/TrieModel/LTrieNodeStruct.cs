using System;
using System.Collections.Generic;
using System.Text;

namespace DBTrie.TrieModel
{
	internal readonly struct LTrieNodeStruct
	{
		public LTrieNodeStruct(long ownPointer, ReadOnlySpan<byte> data)
		{
			var lineLen = data.ReadUInt16BigEndian();
			OwnPointer = ownPointer;
			ExternalLinkSlotCount = (lineLen - Sizes.DefaultPointerLen) / Sizes.ExternalLinkLength;
			InternalLinkPointer = (long)data.Slice(2).BigEndianToLongDynamic();
			FirstExternalLink = new LTrieNodeExternalLinkStruct(OwnPointer + 2 + Sizes.DefaultPointerLen, data.Slice(2 + Sizes.DefaultPointerLen));
		}
		public readonly long OwnPointer;
		public readonly long InternalLinkPointer;
		public readonly int ExternalLinkSlotCount;
		public readonly LTrieNodeExternalLinkStruct FirstExternalLink;
		public readonly int GetRestLength()
		{
			return Sizes.ExternalLinkLength * (ExternalLinkSlotCount - 1);
		}
		public readonly long GetSecondExternalLinkOwnPointer()
		{
			return FirstExternalLink.OwnPointer + Sizes.ExternalLinkLength;
		}

		public readonly Link? GetInternalLinkObject()
		{
			if (InternalLinkPointer == 0)
				return null;
			return new Link(null)
			{
				LinkToNode = false,
				Pointer = InternalLinkPointer,
				OwnPointer = OwnPointer + 2
			};
		}
	}
}
