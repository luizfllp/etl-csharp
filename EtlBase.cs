using System;
using System.Data;
using MySql.Data.MySqlClient;

namespace ETL
{
    class Program
    {
        static void Main(string[] args)
        {
            // Connection string for the source MySQL database
            string sourceConnStr = "Server=localhost;Database=source_db;Uid=root;Pwd=password;";

            // Connection string for the destination MySQL database
            string destConnStr = "Server=localhost;Database=dest_db;Uid=root;Pwd=password;";

            // SQL query to extract data from the source database
            string sql = "SELECT * FROM customers";

            // Create a DataTable to hold the extracted data
            DataTable dataTable = new DataTable();

            // Open a connection to the source database
            using (MySqlConnection conn = new MySqlConnection(sourceConnStr))
            {
                conn.Open();

                // Create a command to execute the SQL query
                using (MySqlCommand cmd = new MySqlCommand(sql, conn))
                {
                    // Use a MySqlDataAdapter to fill the DataTable with the results of the query
                    using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                    {
                        adapter.Fill(dataTable);
                    }
                }
            }

            // Transform the data in the DataTable (e.g. convert data types, rename columns, etc.)

            // Open a connection to the destination database
            using (MySqlConnection conn = new MySqlConnection(destConnStr))
            {
                conn.Open();

                // Use a MySqlBulkCopy to load the transformed data into the destination database
                using (MySqlBulkCopy bulkCopy = new MySqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "customers";
                    bulkCopy.WriteToServer(dataTable);
                }
            }
        }
    }
}
