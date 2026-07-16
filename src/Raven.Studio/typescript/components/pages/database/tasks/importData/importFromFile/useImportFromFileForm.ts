import { useEffect } from "react";
import { useForm, useWatch } from "react-hook-form";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { ImportFromFileFormData, importFromFileYupResolver } from "./importFromFileValidation";

export function getDefaultFormData(isAdminAccessOrAbove: boolean): ImportFromFileFormData {
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
            isIncludeIndexes: isAdminAccessOrAbove,
            isIncludeIndexHistory: false,
            isRemoveAnalyzers: false,
            isIncludeIdentities: true,
            isIncludeConnectionStringsAndOngoingTasks: true,
            isCustomizeOngoingTasks: false,
            ongoingTasks: {
                periodicBackups: isAdminAccessOrAbove,
                externalReplications: isAdminAccessOrAbove,
                ravenEtls: isAdminAccessOrAbove,
                sqlEtls: isAdminAccessOrAbove,
                snowflakeEtls: isAdminAccessOrAbove,
                olapEtls: isAdminAccessOrAbove,
                elasticSearchEtls: isAdminAccessOrAbove,
                queueEtls: isAdminAccessOrAbove,
                hubReplications: true,
                sinkReplications: isAdminAccessOrAbove,
                embeddingsGeneration: isAdminAccessOrAbove,
                genAi: isAdminAccessOrAbove,
                cdcSinks: isAdminAccessOrAbove,
                aiAgents: isAdminAccessOrAbove,
                remoteAttachments: isAdminAccessOrAbove,
            },
            connectionStrings: {
                ravenConnectionStrings: isAdminAccessOrAbove,
                sqlConnectionStrings: isAdminAccessOrAbove,
                snowflakeConnectionStrings: isAdminAccessOrAbove,
                olapConnectionStrings: isAdminAccessOrAbove,
                elasticSearchConnectionStrings: isAdminAccessOrAbove,
                queueConnectionStrings: isAdminAccessOrAbove,
                aiConnectionStrings: isAdminAccessOrAbove,
            },
            isImportAllSettings: true,
            databaseSettings: {
                settings: true,
                conflictSolverConfig: true,
                client: isAdminAccessOrAbove,
                revisions: isAdminAccessOrAbove,
                refresh: true,
                expiration: isAdminAccessOrAbove,
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
    };
}

export const defaultTransformScript =
    "this.collection = this['@metadata']['@collection'];\r\n" +
    "// current object is available under 'this' variable\r\n" +
    "// @change-vector, @id, @last-modified metadata fields are not available";

export function useImportFromFileForm() {
    const isAdminAccessOrAbove = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const form = useForm<ImportFromFileFormData>({
        resolver: importFromFileYupResolver,
        mode: "onChange",
        defaultValues: getDefaultFormData(isAdminAccessOrAbove),
    });

    const { control, setValue } = form;

    const documents = useWatch({ control, name: "documents" });
    const configuration = useWatch({ control, name: "configuration" });
    const isUseTransformScript = useWatch({ control, name: "processing.isUseTransformScript" });

    // Knockout parity: disabling documents forces attachments off (the reverse direction -
    // enabling counters/revisions/time series/attachments forcing documents on - is handled
    // via FormSwitch afterChange in DataToImportSection, matching Knockout's directional
    // subscriptions).
    useEffect(() => {
        if (!documents.isIncludeDocuments && documents.isIncludeAttachments) {
            setValue("documents.isIncludeAttachments", false);
        }
    }, [documents.isIncludeDocuments, documents.isIncludeAttachments, setValue]);

    // Knockout parity: disabling indexes forces analyzer-removal and index-history off (the
    // reverse direction is handled via FormSwitch afterChange in DataToImportSection).
    useEffect(() => {
        if (!configuration.isIncludeIndexes) {
            if (configuration.isRemoveAnalyzers) {
                setValue("configuration.isRemoveAnalyzers", false);
            }
            if (configuration.isIncludeIndexHistory) {
                setValue("configuration.isIncludeIndexHistory", false);
            }
        }
    }, [
        configuration.isIncludeIndexes,
        configuration.isRemoveAnalyzers,
        configuration.isIncludeIndexHistory,
        setValue,
    ]);

    useEffect(() => {
        if (isUseTransformScript) {
            setValue("processing.transformScript", defaultTransformScript);
        } else {
            setValue("processing.transformScript", "");
        }
    }, [isUseTransformScript, setValue]);

    return form;
}
