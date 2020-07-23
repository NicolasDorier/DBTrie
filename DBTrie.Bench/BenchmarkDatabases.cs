using BenchmarkDotNet.Attributes;
using DBTrie.Storage;
using DBTrie.Tests;
using DBTrie.TrieModel;
using LevelDB;
using LiteDB;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Bench
{
    [MarkdownExporterAttribute.GitHub]
    [RankColumn, MemoryDiagnoser]
    public class BenchmarkDatabases
    {
        private string folder;
        private DBTrieEngine trie;
        private DB ldb;
        private LiteDatabase litedb;
        private ILiteCollection<LiteDbEntity> litedbCol;

        private int trieInsertCount = 0;
        private int ldbInsertCount = 0;
        private int litedbInsertCount = 0;
        private int trieGetCount = 0;
        private int ldbGetCount = 0;
        private int litedbGetCount = 0;
        private int trieDeleteCount = 0;
        private int ldbDeleteCount = 0;
        private int litedbDeleteCount = 0;

        private byte[] data = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        [GlobalSetup]
        public void Setup()
        {
            folder = @"BenchData";
            Directory.CreateDirectory($@"BenchData");
            Directory.CreateDirectory($@"{folder}\trie");
            trie = DBTrieEngine.OpenFromFolder($@"{folder}\trie").Result;
            ldb = new DB(new Options { CreateIfMissing = true }, $@"{folder}\ldb");
            Directory.CreateDirectory($@"{folder}\litedb");
            litedb = new LiteDatabase(new ConnectionString() { Filename = $@"{folder}\litedb\db" });
            this.litedbCol = litedb.GetCollection<LiteDbEntity>("tbl");
        }

        [GlobalCleanup]
        public async Task Cleanup()
        {
            await trie.DisposeAsync();
            ldb.Dispose();
            litedb.Dispose();
        }

        [Benchmark]
        public async Task TrieInsert()
        {
            using (var trx = await trie.OpenTransaction())
            {
                var tbl = trx.GetTable("tbl");
                for (int i = 0; i < 10; i++)
                {
                    await tbl.Insert(BitConverter.GetBytes(trieInsertCount++), data);
                }

                await tbl.Commit();
            }
        }

        [Benchmark]
        public void LeveldbInsert()
        {
            using (var batch = new WriteBatch())
            {
                for (int i = 0; i < 10; i++)
                {
                    batch.Put(new byte[] { 1 }.Concat(BitConverter.GetBytes(ldbInsertCount++)).ToArray(), data);
                }

                ldb.Write(batch);
            }
        }

        [Benchmark]
        public void LitedbInsert()
        {
            var list = new List<LiteDbEntity>();

            for (int i = 0; i < 10; i++)
            {
                list.Add(new LiteDbEntity { Table = 1, Key = BitConverter.GetBytes(litedbInsertCount++), Value = data });
            }

            litedbCol.InsertBulk(list);
        }

        [Benchmark]
        public async Task TrieGet()
        {
            using (var trx = await trie.OpenTransaction())
            {
                var tbl = trx.GetTable("tbl");
                for (int i = 0; i < 10; i++)
                {
                    await tbl.Get(BitConverter.GetBytes(trieGetCount++));
                }
            }
        }

        [Benchmark]
        public void LeveldbGet()
        {
            for (int i = 0; i < 10; i++)
            {
                ldb.Get(new byte[] { 1 }.Concat(BitConverter.GetBytes(ldbGetCount++)).ToArray());
            }
        }

        [Benchmark]
        public void LitedbGet()
        {
            for (int i = 0; i < 10; i++)
            {
                litedbCol.FindById(BitConverter.GetBytes(litedbGetCount++));
            }
        }

        [Benchmark]
        public async Task TrieDelete()
        {
            using (var trx = await trie.OpenTransaction())
            {
                var tbl = trx.GetTable("tbl");
                for (int i = 0; i < 10; i++)
                {
                    await tbl.Delete(BitConverter.GetBytes(trieDeleteCount++));
                }

                await tbl.Commit();
            }
        }

        [Benchmark]
        public void LeveldbDelete()
        {
            using (var batch = new WriteBatch())
            {
                for (int i = 0; i < 10; i++)
                {
                    batch.Delete(new byte[] { 1 }.Concat(BitConverter.GetBytes(ldbDeleteCount++)).ToArray());
                }

                ldb.Write(batch);
            }
        }

        [Benchmark]
        public void LitedbDelete()
        {
            for (int i = 0; i < 10; i++)
            {
                litedbCol.Delete(BitConverter.GetBytes(litedbDeleteCount++));
            }
        }
    }

    public class LiteDbEntity
    {
        public int Table { get; set; }

        [BsonId]
        public byte[] Key { get; set; }

        public byte[] Value { get; set; }
    }
}