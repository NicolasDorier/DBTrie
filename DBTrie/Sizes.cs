using System;
using System.Collections.Generic;
using System.Text;

namespace DBTrie
{
	class Sizes
	{
		public const int RootSize = 64;
		public const int ExternalLinkLength = (DefaultPointerLen + 2);
		public const int MaximumNodeSize = 2 + DefaultPointerLen + (256 * (DefaultPointerLen + 2));
		public const int DefaultPointerLen = 5;
	}
}
