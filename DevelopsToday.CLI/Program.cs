using System.CommandLine;
using DevelopsToday.CLI.Utils;

var csvFilePath = new Option<string>(
    "--csvFilePath",
    description: "Path to the CSV file."
);
var rootCommand = new RootCommand("ETL project for importing CSV data into SQL Server.")
{
    csvFilePath
};
const string connectionString =
    "Server=localhost;Database=DevelopsToday;Trusted_Connection=True;Encrypt=True;TrustServerCertificate=True;";

rootCommand.SetHandler(async (csvFilePath) => { await HandlerUtils.Handle(csvFilePath, connectionString); },
    csvFilePath);
await rootCommand.InvokeAsync(args);

//dotnet run --project "*Your Project Path*" --csvFilePath "*Your File Path*"
// Result: 29889 records in DB
/*
 * 9. Assume your program will be used on much larger data files. Describe in a few sentences what you would change if you knew it would be used for a 10GB CSV input file.
 * Batch Processing: Process the data in batches to avoid loading the entire file into memory at once. This can help manage memory usage and improve performance.
 * Implement logging and monitoring to track the progress of the data processing and identify any issues or bottlenecks.
 */
record Key(DateTime? tpep_pickup_datetime, DateTime? tpep_dropoff_datetime, int? passenger_count);