using System;
using System.Collections.Generic;
using System.Text;

namespace DBTrie
{
	public enum EnumerationOrder
	{
		Ordered,
		/// <summary>
		/// Unordered queries are slightly faster
		/// </summary>
		Unordered
	}
}
