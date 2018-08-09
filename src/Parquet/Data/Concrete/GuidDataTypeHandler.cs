using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Parquet.Data.Concrete
{
   class GuidDataTypeHandler : BasicPrimitiveDataTypeHandler<Guid>
   {
      protected static byte[] FlipEndianOrder(byte[] x) => new[] {x[3], x[2], x[1], x[0], x[5], x[4], x[7], x[6], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15]};

      protected static byte[] ToNetworkBytes(Guid g)
      {
         byte[] bytes = g.ToByteArray();
         return BitConverter.IsLittleEndian ? FlipEndianOrder(bytes) : bytes;
      }

      protected static Guid FromNetworkBytes(byte[] bytes)
      {
         if (BitConverter.IsLittleEndian)
            bytes = FlipEndianOrder(bytes);
         return new Guid(bytes);
      }

      public GuidDataTypeHandler() : base(DataType.Uuid, Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.UUID)
      {
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return

            tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.UUID &&

            (
               tse.Type == Thrift.Type.FIXED_LEN_BYTE_ARRAY
            );
      }

      public override void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         base.CreateThrift(se, parent, container);

         //modify this element slightly
         Thrift.SchemaElement tse = container.Last();
         tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
         tse.Type_length = 16;
      }

      public override int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset, ParquetOptions formatOptions)
      {
         var ddest = (Guid[])dest;

         switch (tse.Type)
         {
            case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
               return ReadAsFixedLengthByteArray(tse, reader, ddest, offset);
            default:
               throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
         }

      }

      public override void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         switch(tse.Type)
         {
            case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
               WriteAsFixedLengthByteArray(tse, writer, values);
               break;
            default:
               throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
         }
      }

      private int ReadAsFixedLengthByteArray(Thrift.SchemaElement tse, BinaryReader reader, Guid[] dest, int offset)
      {
         int start = offset;
         int typeLength = tse.Type_length;

         //can't read if there is no type length set
         if (typeLength == 0) return 0;

         while (reader.BaseStream.Position + typeLength <= reader.BaseStream.Length)
         {
            byte[] bytes = reader.ReadBytes(typeLength);
            if (bytes.Length != 16 )
               throw new InvalidDataException("uuid requires multiple of 16 bytes");
            dest[offset++] = FromNetworkBytes(bytes);
         }

         return offset - start;
      }

      // ReSharper disable once UnusedParameter.Local -- tse cannot change, UUID is fixed length
      private void WriteAsFixedLengthByteArray(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         foreach (Guid d in values)
         {
            byte[] itemData = ToNetworkBytes(d);
            writer.Write(itemData);
         }
      }

   }
}
