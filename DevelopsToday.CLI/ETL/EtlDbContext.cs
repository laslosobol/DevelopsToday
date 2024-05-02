using Microsoft.EntityFrameworkCore;

namespace DevelopsToday.CLI.ETL;

public class EtlDbContext : DbContext
{
    private readonly string _connectionString;

    public DbSet<EtlRecord> EtlRecords { get; set; }

    public EtlDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlServer(_connectionString);
    }
}