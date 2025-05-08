
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'extractions')
    BEGIN
        CREATE TABLE extractions (
            [id] INT IDENTITY(1,1) PRIMARY KEY, [extraction_datetime] DATETIME, [source_name] NVARCHAR(255), [date] NVARCHAR(50), [time] NVARCHAR(50), [datetime] NVARCHAR(50), [volume_march_central] NVARCHAR(255), [volume_march_de_blocs] NVARCHAR(255), [volume_introductions] NVARCHAR(255), [volume_offres_publiques] NVARCHAR(255), [volume_transferts] NVARCHAR(255), [volume_apports] NVARCHAR(255), [volume_augmentations_du_capital] NVARCHAR(255), [volume_pr_t_titre] NVARCHAR(255), [total] NVARCHAR(255)
        )
    END
    