using System;
using System.Collections.Generic;
using System.Text;

namespace DBTrie
{
	class Sizes
	{
		public const int RootSize = 64;
		public const int KidLength = (DefaultPointerLen + 2);
		public const int MaximumKidLineLength = 2 + DefaultPointerLen + (256 * (DefaultPointerLen + 2));
		public const int DefaultPointerLen = 5;
		public const int Shift = DefaultPointerLen + 1 + 1;
	}
}
