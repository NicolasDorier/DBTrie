using System;
using System.Collections.Generic;
using System.Text;

namespace DBTrie.Storage.Cache
{
	public class NoMorePageAvailableException : InvalidOperationException
	{
		public NoMorePageAvailableException():base("No page available to evict from cache, you should increase the maximum number of page, or commit more often.")
		{

		}
	}
}
