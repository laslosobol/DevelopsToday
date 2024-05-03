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

rootCommand.SetHandler(async (csvFilePath) =>
{
    await HandlerUtils.Handle(csvFilePath, connectionString);
}, csvFilePath);
await rootCommand.InvokeAsync(args);

//dotnet run --project "C:\Users\sobol\RiderProjects\DevelopsToday\DevelopsToday.CLI\DevelopsToday.CLI.csproj" --csvFilePath "C:\Users\sobol\RiderProjects\DevelopsToday\DevelopsToday.CLI\Utils\sample-cab-data.csv""
record Key(DateTime? tpep_pickup_datetime, DateTime? tpep_dropoff_datetime, int? passenger_count);