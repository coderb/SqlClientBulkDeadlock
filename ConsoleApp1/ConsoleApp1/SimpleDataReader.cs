using System;
using System.Collections.Generic;
using System.Data;

namespace ConsoleApp1 {
    public sealed class SimpleDataReader : IDataReader {
        private readonly IEnumerator<object[]> enumerator;
        private readonly int fieldCount;
        private object[] values;
        private int readCount;
        private bool isClosed;

        public SimpleDataReader(int fieldCount, IEnumerable<object[]> enumerable) {
            this.fieldCount = fieldCount;
            this.enumerator = enumerable.GetEnumerator();
        }

        void IDisposable.Dispose() {
            Close();
        }

        public void Close() {
            try {
                this.enumerator.Dispose();
            } finally {
                this.isClosed = true;
            }
        }

        public bool IsClosed {
            get { return this.isClosed; }
        }

        public int FieldCount {
            get { return this.fieldCount; }
        }

        public int ReadCount {
            get { return this.readCount; }
        }

        public object GetValue(int i) {
            return this.values[i];
        }

        public bool Read() {
            if (this.enumerator.MoveNext()) {
                var values = this.enumerator.Current;
                if (values.Length != this.fieldCount) throw new ArgumentException();
                this.values = values;
                this.readCount++;
                return true;
            } else {
                this.values = new object[FieldCount];
                return false;
            }
        }

        #region NotImplemented
        public string GetName(int i) {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i) {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i) {
            throw new NotImplementedException();
        }

        public int GetValues(object[] values) {
            throw new NotImplementedException();
        }

        public int GetOrdinal(string name) {
            throw new NotImplementedException();
        }

        public bool GetBoolean(int i) {
            throw new NotImplementedException();
        }

        public byte GetByte(int i) {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) {
            throw new NotImplementedException();
        }

        public char GetChar(int i) {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i) {
            throw new NotImplementedException();
        }

        public short GetInt16(int i) {
            throw new NotImplementedException();
        }

        public int GetInt32(int i) {
            throw new NotImplementedException();
        }

        public long GetInt64(int i) {
            throw new NotImplementedException();
        }

        public float GetFloat(int i) {
            throw new NotImplementedException();
        }

        public double GetDouble(int i) {
            throw new NotImplementedException();
        }

        public string GetString(int i) {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i) {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i) {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i) {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i) {
            throw new NotImplementedException();
        }

        object IDataRecord.this[int i] {
            get { throw new NotImplementedException(); }
        }

        object IDataRecord.this[string name] {
            get { throw new NotImplementedException(); }
        }

        public DataTable GetSchemaTable() {
            throw new NotImplementedException();
        }

        public bool NextResult() {
            throw new NotImplementedException();
        }

        public int Depth { get { throw new NotImplementedException(); } }

        public int RecordsAffected { get { throw new NotImplementedException(); } }
        #endregion
    }
}