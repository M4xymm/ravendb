import { getDatabaseRecordTypes, toImportDto, getItemsToWarnAbout, buildImportCurlCommand } from "./importFromFileUtils";
import { ImportFromFileFormData, importFromFileSchema } from "./importFromFileValidation";

function createDefaultFormData(): ImportFromFileFormData {
    // Same shape as useImportFromFileForm's getDefaultFormData(true) (admin access) — all
    // toggles set per Knockout defaults (importDatabaseModel + smugglerDatabaseRecord).
    return {
        file: null,
        documents: {
            isIncludeDocuments: true,
            isIncludeAttachments: true,
            isIncludeCounters: true,
            isIncludeRevisions: true,
            isIncludeTimeSeries: true,
            isIncludeTimeSeriesDeletedRanges: true,
            isIncludeArtificialDocuments: false,
            isIncludeArchivedDocuments: true,
            isIncludeExpiredDocuments: true,
            isIncludeConflicts: true,
            isIncludeCompareExchange: true,
            isIncludeLegacyAttachments: false,
            isIncludeDocumentsTombstones: true,
            isIncludeCompareExchangeTombstones: true,
            isIncludeSubscriptions: true,
        },
        collections: {
            isImportAllCollections: true,
            includedCollections: [],
        },
        configuration: {
            isIncludeIndexes: true,
            isIncludeIndexHistory: false,
            isRemoveAnalyzers: false,
            isIncludeIdentities: true,
            isIncludeConnectionStringsAndOngoingTasks: true,
            isCustomizeOngoingTasks: false,
            ongoingTasks: {
                periodicBackups: true,
                externalReplications: true,
                ravenEtls: true,
                sqlEtls: true,
                snowflakeEtls: true,
                olapEtls: true,
                elasticSearchEtls: true,
                queueEtls: true,
                hubReplications: true,
                sinkReplications: true,
                embeddingsGeneration: true,
                genAi: true,
                cdcSinks: true,
                aiAgents: true,
                remoteAttachments: true,
            },
            connectionStrings: {
                ravenConnectionStrings: true,
                sqlConnectionStrings: true,
                snowflakeConnectionStrings: true,
                olapConnectionStrings: true,
                elasticSearchConnectionStrings: true,
                queueConnectionStrings: true,
                aiConnectionStrings: true,
            },
            isImportAllSettings: true,
            databaseSettings: {
                settings: true,
                conflictSolverConfig: true,
                client: true,
                revisions: true,
                refresh: true,
                expiration: true,
                documentsCompression: true,
                schemaValidation: true,
                dataArchival: true,
                timeSeries: true,
                sorters: true,
                analyzers: true,
                postgreSqlIntegration: true,
            },
        },
        processing: {
            isUseTransformScript: false,
            transformScript: "",
            isSetMaxReadOpsPerSecond: false,
            maxReadOpsPerSecond: null,
            isEncrypted: false,
            encryptionKey: "",
        },
    } as ImportFromFileFormData;
}

describe("importFromFileUtils", () => {
    describe("getDatabaseRecordTypes", () => {
        it("returns ['None'] in non-customized mode without index history", () => {
            expect(getDatabaseRecordTypes(createDefaultFormData())).toEqual(["None"]);
        });

        it("returns ['IndexesHistory'] in non-customized mode with index history", () => {
            const data = createDefaultFormData();
            data.configuration.isIncludeIndexHistory = true;
            expect(getDatabaseRecordTypes(data)).toEqual(["IndexesHistory"]);
        });

        it("emits explicit tokens minus restricted keys when restrictions exist", () => {
            const types = getDatabaseRecordTypes(createDefaultFormData(), ["documentsCompression", "dataArchival"]);
            expect(types).not.toContain("DocumentsCompression");
            expect(types).not.toContain("DataArchival");
            expect(types).toContain("Settings");
            expect(types).toContain("RavenConnectionStrings");
        });

        it("emits only checked settings in customize mode", () => {
            const data = createDefaultFormData();
            data.configuration.isImportAllSettings = false;
            Object.keys(data.configuration.databaseSettings).forEach(
                (key) => ((data.configuration.databaseSettings as any)[key] = false)
            );
            data.configuration.databaseSettings.settings = true;
            data.configuration.isIncludeConnectionStringsAndOngoingTasks = false;
            data.configuration.isIncludeIndexHistory = false;
            expect(getDatabaseRecordTypes(data)).toEqual(["Settings"]);
        });
    });

    describe("toImportDto", () => {
        it("maps default form data like Knockout defaults", () => {
            const dto = toImportDto(createDefaultFormData());
            const types = (dto.OperateOnTypes as string).split(",");
            expect(types).toEqual(
                expect.arrayContaining(["DatabaseRecord", "Documents", "Conflicts", "Indexes", "RevisionDocuments",
                    "Identities", "CompareExchange", "CounterGroups", "Attachments", "TimeSeries",
                    "TimeSeriesDeletedRanges", "Subscriptions", "Tombstones", "CompareExchangeTombstones"])
            );
            expect(types).not.toContain("LegacyAttachments");
            expect(dto.OperateOnDatabaseRecordTypes).toBe("None");
            expect(dto.Collections).toBeNull();
            expect(dto.EncryptionKey).toBeUndefined();
            expect(dto.MaxReadOpsPerSecond).toBeNull();
        });

        it("passes collections list when customize is on", () => {
            const data = createDefaultFormData();
            data.collections.isImportAllCollections = false;
            data.collections.includedCollections = ["Orders", "Employees"];
            expect(toImportDto(data).Collections).toEqual(["Orders", "Employees"]);
        });

        it("sets EncryptionKey and MaxReadOpsPerSecond only when enabled", () => {
            const data = createDefaultFormData();
            data.processing.isEncrypted = true;
            data.processing.encryptionKey = "key123";
            data.processing.isSetMaxReadOpsPerSecond = true;
            data.processing.maxReadOpsPerSecond = 500;
            const dto = toImportDto(data);
            expect(dto.EncryptionKey).toBe("key123");
            expect(dto.MaxReadOpsPerSecond).toBe(500);
        });
    });

    describe("getItemsToWarnAbout", () => {
        it("warns for counters/time series/revisions without documents", () => {
            const data = createDefaultFormData();
            data.documents.isIncludeDocuments = false;
            expect(getItemsToWarnAbout(data)).toEqual(["Counters", "Time Series", "Revisions"]);
        });

        it("returns empty when documents included", () => {
            expect(getItemsToWarnAbout(createDefaultFormData())).toEqual([]);
        });
    });

    describe("buildImportCurlCommand", () => {
        it.each(["PowerShell", "Cmd", "Bash"] as const)("builds %s command", (shell) => {
            const command = buildImportCurlCommand(shell, createDefaultFormData(), "db1");
            expect(command).toContain("importOptions=");
            expect(command).toContain("/smuggler/import");
        });
    });

    describe("importFromFileSchema", () => {
        it("rejects a RavenDB Snapshot file", async () => {
            await expect(
                importFromFileSchema.validateAt("file", { file: { name: "x.ravendb-snapshot" } as File })
            ).rejects.toThrow(/Snapshot/);
        });
    });
});
