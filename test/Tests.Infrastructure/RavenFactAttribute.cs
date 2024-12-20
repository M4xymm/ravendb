﻿using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenFactAttribute : FactAttribute, ITraitAttribute
{
    public readonly RavenTestCategory Category;
    private string _skip;

    public RavenFactAttribute(RavenTestCategory category)
    {
        Category = category;
    }

    public bool LicenseRequired { get; set; }

    public bool MsSqlRequired { get; set; }

    public bool ElasticSearchRequired { get; set; }

    public bool AzureQueueStorageRequired { get; set; }
    
    public bool AmazonSqsRequired { get; set; }

    public bool SnowflakeRequired { get; set; }
    
    public bool NightlyBuildRequired { get; set; }

    public override string Skip
    {
        get
        {
            return ShouldSkip(_skip, _category, licenseRequired: LicenseRequired, nightlyBuildRequired: NightlyBuildRequired, msSqlRequired: MsSqlRequired, elasticSearchRequired: ElasticSearchRequired, azureQueueStorageRequired: AzureQueueStorageRequired, snowflakeRequired: SnowflakeRequired, amazonSqsRequired: AmazonSqsRequired);
        }

        set => _skip = value;
    }


    internal static string ShouldSkip(string skip, RavenTestCategory category, bool licenseRequired, bool nightlyBuildRequired, bool msSqlRequired, bool elasticSearchRequired, bool azureQueueStorageRequired, bool snowflakeRequired, bool amazonSqsRequired)
    {
        var s = ShouldSkip(skip, category, licenseRequired: licenseRequired, nightlyBuildRequired: nightlyBuildRequired);
        if (s != null)
            return s;

        if (msSqlRequired && RequiresMsSqlFactAttribute.ShouldSkip(out skip))
            return skip;

        if (elasticSearchRequired && RequiresElasticSearchRetryFactAttribute.ShouldSkip(out skip))
            return skip;

        if (azureQueueStorageRequired && AzureQueueStorageHelper.ShouldSkip(out skip))
            return skip;
        
        if (amazonSqsRequired && AmazonSqsHelper.ShouldSkip(out skip))
            return skip;

        if (snowflakeRequired && SnowflakeHelper.ShouldSkip(out skip))
            return skip;
        
        return null;
    }

    internal static string ShouldSkip(string skip, RavenTestCategory category, bool licenseRequired, bool nightlyBuildRequired)
    {
        if (skip != null)
            return skip;

        if (RavenDataAttributeBase.Is32Bit)
        {
            if (category.HasFlag(RavenTestCategory.Sharding))
                return RavenDataAttributeBase.ShardingSkipMessage;
        }

        if (licenseRequired && LicenseRequiredFactAttribute.ShouldSkip())
            return LicenseRequiredFactAttribute.SkipMessage;

        if (nightlyBuildRequired && NightlyBuildFactAttribute.ShouldSkip(out skip))
            return skip;

        return null;
    }
}
