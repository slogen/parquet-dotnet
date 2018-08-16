using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class GuidSchemaTests
   {
      struct T
      {
         public Guid A { get; set; }
         public Guid B { get; set; }
         public Guid? C { get; set; }
      }
      static Schema Schema { get; } = new Schema(
         new DataField<Guid>(nameof(T.A)),
         new DataField(nameof(T.B), typeof(Guid)),
         new DataField(nameof(T.C), DataType.Uuid, hasNulls: true)
         );
      static IList<T> WriteAndRead(ICollection<T> ts)
      {
         using (var ms = new MemoryStream())
         {
            ms.WriteSingleRowGroupParquetFile(
               Schema,
               ts.Count,
               new DataColumn[] {
                  new DataColumn(Schema.DataFieldAt(0), ts.Select(x => x.A).ToArray()),
                  new DataColumn(Schema.DataFieldAt(1), ts.Select(x => x.B).ToArray()),
                  new DataColumn(Schema.DataFieldAt(2), ts.Select(x => x.C).ToArray())
               });
            ms.Position = 0;
            ms.ReadSingleRowGroupFile(out Schema readSchema, out DataColumn[] columns);
            var a = (Guid[])columns[0].Data;
            var b = (Guid[])columns[1].Data;
            var c = (Guid?[])columns[2].Data;
            return Enumerable.Range(0, a.Length)
               .Select(i => new T { A = a[i], B = b[i], C = c[i] })
               .ToArray();
         }
      }
      static void AssertEqual(T expect, T actual)
      {
         Assert.Equal(expect.A, actual.A);
         Assert.Equal(expect.B, actual.B);
         Assert.Equal(expect.C, actual.C);
      }
      static void AssertEqual(ICollection<T> expect, ICollection<T> actual)
      {
         Assert.Equal(expect.Count, actual.Count);
         foreach(var x in expect.Zip(actual, (e, a) => new {e,a}))
            AssertEqual(x.e, x.a);
      }

      [Theory]
      [InlineData(1)]
      [InlineData(10)]
      public void guid_write_read(int rowCount)
      {
         IList<T> inputs = Enumerable.Range(0, rowCount)
            .Select(i => new T { A = Guid.NewGuid(), B = Guid.NewGuid(), C = i % 2 == 0 ? default(Guid?) : Guid.NewGuid() })
            .ToArray();
         IList<T> read = WriteAndRead(inputs);
         AssertEqual(inputs, read);
      }
   }
}