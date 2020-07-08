using DBTrie.Storage;
using DBTrie.TrieModel;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Tests
{
	static class Extensions
	{
        public static void Shuffle<T>(this Random rng, T[] array)
        {
            int n = array.Length;
            while (n > 1)
            {
                int k = rng.Next(n--);
                T temp = array[n];
                array[n] = array[k];
                array[k] = temp;
            }
        }
        public static T PickRandom<T>(this Random rng, T[] array)
        {
            return array[rng.Next(0, array.Length)];
        }

        public static async Task Write(this IStorage storage, long position, string txt)
        {
            await storage.Write(position, Encoding.UTF8.GetBytes(txt));
        }
        public static async Task WriteToEnd(this IStorage storage, string txt)
        {
            await storage.WriteToEnd(Encoding.UTF8.GetBytes(txt));
        }
        public static async Task<string> Read(this IStorage storage, long position, int length)
        {
            var bytes = new byte[length];
            await storage.Read(position, bytes);
            return Encoding.UTF8.GetString(bytes);
        }
    }
}
