using DBTrie.TrieModel;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace DBTrie.Tests
{
	internal static class TestUtils
	{
		public static async Task AssertExists(this LTrie trie, string key)
		{
			using var result = await trie.GetValue("@utTestTa");
			Assert.NotNull(result);
		}
	}
}
