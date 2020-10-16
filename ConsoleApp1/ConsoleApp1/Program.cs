using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace ConsoleApp1 {
    class Program {
        private const string DSN = "<DSN HERE>";

        // create table tbTest(Dummy int)
        // insert tbTest values (1)
        private const string TableName = "dbo.tbTest";

        static async Task Main(string[] args) {
            // this works ok
            new Program().RunSync();
            // this deadlocks in code
            await new Program().RunAsync();
            Log("done");
        }

        private static void Log(string s) {
            System.Console.WriteLine(s);
        }

        public static T Unwrap<T>(object result) {
            if (result == DBNull.Value) {
                result = null;
            }
            return (T)result;
        }

        public SqlBulkCopy NewBulkCopy(SqlConnection concreteConn, SqlTransaction sqlTransaction) {
            var options = SqlBulkCopyOptions.Default;
            // var options = SqlBulkCopyOptions.TableLock;
            // options |= SqlBulkCopyOptions.CheckConstraints;
            // options |= SqlBulkCopyOptions.FireTriggers;
            return new SqlBulkCopy(concreteConn, options, sqlTransaction);
        }

        private void RunSync() {
            ExecuteTrans(trans => { 
                BulkCopy(trans, TableName, 1, GetEnumberable(trans));
                return 0; 
            });
        }

        private void ExecuteTrans<T>(Func<SqlTransaction, T> func) {
            try {
                using (var connection = new SqlConnection(DSN)) {
                    connection.Open();
                    var trans = connection.BeginTransaction();
                    Log("BEGIN TRANS");
                    func(trans);
                    Log("COMMIT TRANS");
                    trans.Commit();
                }
            } catch (Exception ex) {
                Log("Transaction exception: " + ex);
            }
        }

        public bool BulkCopy(SqlTransaction sqlTransaction, string tableName, int fieldCount, IEnumerable<object[]> dataEnumerable) {
            bool result;
            var transConnection = sqlTransaction.Connection;
            using (var bulk = NewBulkCopy(transConnection, sqlTransaction)) {
                bulk.DestinationTableName = tableName;
                using (var reader = new SimpleDataReader(fieldCount, dataEnumerable)) {
                    bulk.WriteToServer(reader);
                    result = reader.ReadCount > 0;
                }
            }
            return result;
        }

        public async Task<bool> BulkCopyAsync(SqlTransaction sqlTransaction, string tableName, int fieldCount, IAsyncEnumerable<object[]> dataEnumerable, bool continueOnCapturedContext) {
            bool result;
            var transConnection = sqlTransaction.Connection;
            using (var bulk = NewBulkCopy(transConnection, sqlTransaction)) {
                bulk.DestinationTableName = tableName;
                using (var reader = new SimpleDataReader(fieldCount, dataEnumerable.ToEnumerable())) {
                    await bulk.WriteToServerAsync(reader).ConfigureAwait(continueOnCapturedContext);
                    result = reader.ReadCount > 0;
                }
            }
            return result;
        }


        private IEnumerable<object[]> GetEnumberable(SqlTransaction transTransaction) {
            var sql = "select count(*) from " + TableName;
            var transConnection = transTransaction.Connection;
            var command1 = transConnection.CreateCommand();
            command1.Transaction = transTransaction;
            command1.CommandText = sql;
            int? existingCount = Unwrap<int>(command1.ExecuteScalar());
            Log("existing = " + existingCount);
            yield return new object[] { 1 };
            yield return new object[] { 2 };
            yield return new object[] { 3 };
        }


        private Task RunAsync() {
            var continueOnCapturedContext = false;
            return ExecuteTransAsync(async trans => { 
                await BulkCopyAsync(trans, TableName, 1, GetAsyncEnumberable(trans), continueOnCapturedContext);
                return 0; 
            }, continueOnCapturedContext);
        }

        private async Task ExecuteTransAsync<T>(Func<SqlTransaction, Task<T>> func, bool continueOnCapturedContext) {
            try {
                var connection = new SqlConnection(DSN);
                await using (connection.ConfigureAwait(continueOnCapturedContext)) {
                    await connection.OpenAsync().ConfigureAwait(continueOnCapturedContext);
                    var trans = (SqlTransaction)await connection.BeginTransactionAsync().ConfigureAwait(continueOnCapturedContext);
                    Log("BEGIN TRANS");
                    await func(trans).ConfigureAwait(continueOnCapturedContext);
                    Log("COMMIT TRANS");
                    await trans.CommitAsync().ConfigureAwait(continueOnCapturedContext);
                }
            } catch (Exception ex) {
                Log("Transaction exception " + ex.ToString());
            }
        }

        private async IAsyncEnumerable<object[]> GetAsyncEnumberable(SqlTransaction transTransaction) {
            var sql = "select count(*) from " + TableName;
            var transConnection = transTransaction.Connection;
            var command1 = transConnection.CreateCommand();
            command1.Transaction = transTransaction;
            command1.CommandText = sql;
            int? existingCount = Unwrap<int>(await command1.ExecuteScalarAsync());
            Log("existing = " + existingCount);
            yield return new object[] { 1 };
            yield return new object[] { 2 };
            yield return new object[] { 3 };
            await Task.Delay(0);
        }
    }
}
