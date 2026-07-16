import { DatabaseSettingKey, ImportFromFileFormData } from "./importFromFileValidation";
import endpoints = require("endpoints");
import appUrl = require("common/appUrl");

type DatabaseItemType = Raven.Client.Documents.Smuggler.DatabaseItemType;
type DatabaseRecordItemType = Raven.Client.Documents.Smuggler.DatabaseRecordItemType;
type ImportOptions = Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions;

export type ImportCommandType = "PowerShell" | "Cmd" | "Bash";

const databaseSettingTokens: Record<keyof ImportFromFileFormData["configuration"]["databaseSettings"], DatabaseRecordItemType> = {
    settings: "Settings",
    conflictSolverConfig: "ConflictSolverConfig",
    client: "Client",
    revisions: "Revisions",
    refresh: "Refresh",
    expiration: "Expiration",
    documentsCompression: "DocumentsCompression",
    schemaValidation: "SchemaValidation",
    dataArchival: "DataArchival",
    timeSeries: "TimeSeries",
    sorters: "Sorters",
    analyzers: "Analyzers",
    postgreSqlIntegration: "PostgreSQLIntegration",
};

const ongoingTaskTokens: Record<keyof ImportFromFileFormData["configuration"]["ongoingTasks"], DatabaseRecordItemType> = {
    periodicBackups: "PeriodicBackups",
    externalReplications: "ExternalReplications",
    ravenEtls: "RavenEtls",
    sqlEtls: "SqlEtls",
    snowflakeEtls: "SnowflakeEtls",
    olapEtls: "OlapEtls",
    elasticSearchEtls: "ElasticSearchEtls",
    queueEtls: "QueueEtls",
    hubReplications: "HubPullReplications",
    sinkReplications: "SinkPullReplications",
    embeddingsGeneration: "EmbeddingsGenerations",
    genAi: "GenAiEtls",
    cdcSinks: "CdcSinks",
    aiAgents: "AiAgents",
    remoteAttachments: "RemoteAttachments",
};

const connectionStringTokens: Record<keyof ImportFromFileFormData["configuration"]["connectionStrings"], DatabaseRecordItemType> = {
    ravenConnectionStrings: "RavenConnectionStrings",
    sqlConnectionStrings: "SqlConnectionStrings",
    snowflakeConnectionStrings: "SnowflakeConnectionStrings",
    olapConnectionStrings: "OlapConnectionStrings",
    elasticSearchConnectionStrings: "ElasticSearchConnectionStrings",
    queueConnectionStrings: "QueueConnectionStrings",
    aiConnectionStrings: "AiConnectionStrings",
};

export function getDatabaseRecordTypes(
    formData: ImportFromFileFormData,
    restrictedSettingKeys: DatabaseSettingKey[] = []
): DatabaseRecordItemType[] {
    const { configuration } = formData;

    const isCustomized =
        !configuration.isImportAllSettings || configuration.isCustomizeOngoingTasks || restrictedSettingKeys.length > 0;

    if (!isCustomized) {
        // Knockout parity: non-customized mode
        return configuration.isIncludeIndexHistory ? ["IndexesHistory"] : ["None"];
    }

    const result: DatabaseRecordItemType[] = [];

    (Object.keys(databaseSettingTokens) as (keyof typeof databaseSettingTokens)[]).forEach((key) => {
        if (restrictedSettingKeys.includes(key)) {
            return; // license-restricted settings are never emitted
        }
        if (configuration.isImportAllSettings || configuration.databaseSettings[key]) {
            result.push(databaseSettingTokens[key]);
        }
    });

    if (configuration.isIncludeConnectionStringsAndOngoingTasks) {
        (Object.keys(ongoingTaskTokens) as (keyof typeof ongoingTaskTokens)[]).forEach((key) => {
            if (!configuration.isCustomizeOngoingTasks || configuration.ongoingTasks[key]) {
                result.push(ongoingTaskTokens[key]);
            }
        });
        (Object.keys(connectionStringTokens) as (keyof typeof connectionStringTokens)[]).forEach((key) => {
            if (!configuration.isCustomizeOngoingTasks || configuration.connectionStrings[key]) {
                result.push(connectionStringTokens[key]);
            }
        });
    }

    if (configuration.isIncludeIndexHistory) {
        result.push("IndexesHistory");
    }

    return result;
}

export function toImportDto(
    formData: ImportFromFileFormData,
    restrictedSettingKeys: DatabaseSettingKey[] = []
): ImportOptions {
    const { documents, collections, configuration, processing } = formData;

    const operateOnTypes: DatabaseItemType[] = [];

    const databaseRecordTypes = getDatabaseRecordTypes(formData, restrictedSettingKeys);

    if (databaseRecordTypes.length) {
        operateOnTypes.push("DatabaseRecord");
    }
    if (documents.isIncludeDocuments) {
        operateOnTypes.push("Documents");
    }
    if (documents.isIncludeConflicts) {
        operateOnTypes.push("Conflicts");
    }
    if (configuration.isIncludeIndexes) {
        operateOnTypes.push("Indexes");
    }
    if (documents.isIncludeRevisions) {
        operateOnTypes.push("RevisionDocuments");
    }
    if (configuration.isIncludeIdentities) {
        operateOnTypes.push("Identities");
    }
    if (documents.isIncludeCompareExchange) {
        operateOnTypes.push("CompareExchange");
    }
    if (documents.isIncludeCounters) {
        operateOnTypes.push("CounterGroups");
    }
    if (documents.isIncludeAttachments) {
        operateOnTypes.push("Attachments");
    }
    if (documents.isIncludeLegacyAttachments) {
        operateOnTypes.push("LegacyAttachments");
    }
    if (documents.isIncludeTimeSeries) {
        operateOnTypes.push("TimeSeries");
    }
    if (documents.isIncludeTimeSeriesDeletedRanges) {
        operateOnTypes.push("TimeSeriesDeletedRanges");
    }
    if (documents.isIncludeSubscriptions) {
        operateOnTypes.push("Subscriptions");
    }
    if (documents.isIncludeDocumentsTombstones) {
        operateOnTypes.push("Tombstones");
    }
    if (documents.isIncludeCompareExchangeTombstones) {
        operateOnTypes.push("CompareExchangeTombstones");
    }

    return {
        IncludeExpired: documents.isIncludeExpiredDocuments,
        IncludeArtificial: documents.isIncludeArtificialDocuments,
        IncludeArchived: documents.isIncludeArchivedDocuments,
        TransformScript: processing.isUseTransformScript ? processing.transformScript : "",
        RemoveAnalyzers: configuration.isRemoveAnalyzers,
        EncryptionKey: processing.isEncrypted ? processing.encryptionKey : undefined,
        OperateOnTypes: operateOnTypes.join(",") as DatabaseItemType,
        OperateOnDatabaseRecordTypes: (databaseRecordTypes.length
            ? databaseRecordTypes.join(",")
            : undefined) as DatabaseRecordItemType,
        Collections: collections.isImportAllCollections ? null : collections.includedCollections,
        MaxReadOpsPerSecond: processing.isSetMaxReadOpsPerSecond ? processing.maxReadOpsPerSecond : null,
    } as ImportOptions;
}

export function hasAnyInclude(formData: ImportFromFileFormData): boolean {
    const d = formData.documents;
    const c = formData.configuration;
    return (
        d.isIncludeDocuments ||
        d.isIncludeAttachments ||
        d.isIncludeCounters ||
        d.isIncludeRevisions ||
        d.isIncludeTimeSeries ||
        d.isIncludeTimeSeriesDeletedRanges ||
        d.isIncludeArtificialDocuments ||
        d.isIncludeArchivedDocuments ||
        d.isIncludeConflicts ||
        d.isIncludeCompareExchange ||
        d.isIncludeLegacyAttachments ||
        d.isIncludeDocumentsTombstones ||
        d.isIncludeCompareExchangeTombstones ||
        d.isIncludeSubscriptions ||
        c.isIncludeIndexes ||
        c.isIncludeIdentities ||
        c.isIncludeConnectionStringsAndOngoingTasks
    );
}

export function buildImportCurlCommand(
    commandType: ImportCommandType,
    formData: ImportFromFileFormData,
    databaseName: string,
    restrictedSettingKeys: DatabaseSettingKey[] = []
): string {
    const dto = toImportDto(formData, restrictedSettingKeys);
    if (!dto.TransformScript) {
        delete dto.TransformScript;
    }
    const json = JSON.stringify(dto);
    const fileName = formData.file?.name || "Dump of Database.ravendbdump";
    const commandEndpointUrl =
        appUrl.forServer() + appUrl.forDatabaseQuery(databaseName) + endpoints.databases.smuggler.smugglerImport;

    switch (commandType) {
        case "PowerShell":
            return `curl.exe -F 'importOptions=${json.replace(/"/g, '\\"')}' -F 'file=@.\\${fileName}' ${commandEndpointUrl}`;
        case "Cmd":
            return `curl.exe -F "importOptions=${json.replace(/"/g, '\\"')}" -F "file=@.\\${fileName}" ${commandEndpointUrl}`;
        case "Bash":
            return `curl -F 'importOptions=${json}' -F 'file=@${fileName}' ${commandEndpointUrl}`;
    }
}

export function getItemsToWarnAbout(formData: ImportFromFileFormData): string[] {
    const d = formData.documents;
    if (d.isIncludeDocuments) {
        return [];
    }
    const items: string[] = [];
    if (d.isIncludeCounters) {
        items.push("Counters");
    }
    if (d.isIncludeTimeSeries) {
        items.push("Time Series");
    }
    if (d.isIncludeRevisions) {
        items.push("Revisions");
    }
    return items;
}
