import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import { Icon } from "./Icon";
import { exhaustiveStringTuple } from "components/utils/common";
import IconName from "typings/server/icons";
import copyToClipboard from "common/copyToClipboard";
import { Button } from "reactstrap";

export default {
    title: "Bits/Icon",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const IconStory: StoryObj = {
    name: "Icon",
    render: () => (
        <div className="fs-3 d-grid" style={{ gridTemplateColumns: "auto auto auto" }}>
            {allIconNames.map((name) => (
                <div key={name}>
                    <Icon icon={name} />
                    {name}
                    <Button color="link" onClick={() => copyToClipboard.copy(name, "Copied to clipboard")}>
                        <Icon icon="copy-to-clipboard" />
                    </Button>
                </div>
            ))}
        </div>
    ),
};

const allIconNames = exhaustiveStringTuple<IconName>()(
    "about",
    "accept",
    "access-admin",
    "access-read-write",
    "access-read",
    "additional-assemblies",
    "additional-sources",
    "admin-logs",
    "administrator-js-console",
    "alert",
    "alerts",
    "api-keys",
    "apple",
    "arrow-dashed",
    "arrow-down",
    "arrow-filled-down",
    "arrow-filled-left",
    "arrow-filled-right",
    "arrow-filled-up",
    "arrow-left",
    "arrow-right",
    "arrow-thin-bottom",
    "arrow-thin-left",
    "arrow-thin-right",
    "arrow-thin-top",
    "arrow-up",
    "arrow",
    "attachment",
    "average",
    "aws",
    "azure-queue-storage-etl",
    "azure-queue-storage",
    "azure",
    "backup-history",
    "backup-snapshot",
    "backup",
    "backup2",
    "backups",
    "book",
    "bug",
    "calendar",
    "cancel",
    "certificate",
    "changes",
    "check",
    "checkmark",
    "chevron-down",
    "chevron-left",
    "chevron-right",
    "chevron-up",
    "chrome",
    "circle-filled",
    "circle",
    "clean",
    "clear",
    "clearance",
    "client-configuration",
    "client",
    "clock",
    "clone",
    "close",
    "cloud",
    "cluster-dashboard",
    "cluster-log",
    "cluster-member",
    "cluster-node",
    "cluster-promotable",
    "cluster-rehab",
    "cluster-watcher",
    "cluster-wide-database-settings",
    "cluster",
    "cmp-xchg",
    "code",
    "coffee",
    "collapse-horizontal",
    "collapse-vertical",
    "collapse",
    "collections-storage",
    "compact",
    "config",
    "conflicts-resolution",
    "conflicts",
    "connected",
    "connection-lost",
    "copy-to-clipboard",
    "copy",
    "corax-all-entries-match",
    "corax-backward",
    "corax-boosting-match",
    "corax-fallback",
    "corax-forward",
    "corax-include-null-match",
    "corax-memoization-match",
    "corax-multi-term-match",
    "corax-operator-and",
    "corax-operator-andnot",
    "corax-operator-or",
    "corax-phrase-query",
    "corax-sort-az",
    "corax-sort-za",
    "corax-sorting-match",
    "corax-spatial-match",
    "corax-term-match",
    "corax-unary-match",
    "crane",
    "create-sample-data",
    "crown",
    "csharp-logo",
    "csharp",
    "csv-export",
    "csv-import",
    "custom-analyzers",
    "custom-functions",
    "custom-sorters",
    "cut",
    "danger",
    "dashboard",
    "data-archival",
    "data-subscriptions",
    "database-client-configuration",
    "database-cutout",
    "database-home",
    "database-id",
    "database-record",
    "database-settings",
    "database-studio-configuration",
    "database",
    "dbgroup-member",
    "dbgroup-promotable",
    "dbgroup-rehab",
    "dbgroup-watcher",
    "dbgroup",
    "debug-advanced",
    "debug",
    "debug2",
    "default",
    "demote",
    "details",
    "diff",
    "disable",
    "disabled",
    "disconnect",
    "disconnected",
    "disk-io-viewer",
    "disk-support",
    "dismiss-all",
    "document-cutout",
    "document-expiration",
    "document-group",
    "document-metadata",
    "document",
    "document2",
    "documents-compression",
    "documents-query",
    "documents",
    "download",
    "drive",
    "dump-index-files",
    "edit",
    "edited",
    "elastic-search-etl",
    "elasticsearch",
    "embed",
    "empty-set",
    "encryption",
    "etag",
    "etl",
    "exclamation",
    "exit-fullscreen",
    "expand-horizontal",
    "expand-vertical",
    "expand",
    "experimental",
    "expiration",
    "export-database",
    "export",
    "expos-refresh",
    "external-replication",
    "facebook",
    "feedback",
    "file-import",
    "filesystem",
    "filter",
    "firefox",
    "flag",
    "fold",
    "folder",
    "force",
    "fullscreen",
    "gather-debug-information",
    "gcp",
    "generation",
    "github",
    "global-config",
    "global-cutout",
    "global",
    "googleplus",
    "graph-range",
    "graph",
    "group",
    "hammer-driver",
    "hammer",
    "hash",
    "help",
    "home",
    "hot-spare",
    "icons_backup",
    "icons_database",
    "identities",
    "import-database",
    "import",
    "indent",
    "index-batch-size",
    "index-cleanup",
    "index-errors",
    "index-export",
    "index-fields",
    "index-history",
    "index-import",
    "index-stats",
    "index",
    "indexes-query",
    "indexing-performance",
    "indexing-progess",
    "indexing",
    "infinity",
    "info",
    "integrations",
    "io-test",
    "javascript",
    "json",
    "kafka-etl",
    "kafka-sink",
    "kafka",
    "key",
    "kill-query",
    "latest",
    "lets-encrypt",
    "license-information",
    "license",
    "link",
    "linkedin",
    "linux",
    "list-of-indexes",
    "list",
    "load-index",
    "load-map-reduce",
    "lock-cutout",
    "lock-error",
    "lock-sidebyside",
    "lock",
    "logo",
    "logout",
    "logs",
    "magic-wand",
    "manage-connection-strings",
    "manage-dbgroup",
    "manage-ongoing-tasks",
    "manage-server-io-test",
    "manage-server-running-queries",
    "manage-server",
    "map-reduce-visualizer",
    "map-reduce",
    "map",
    "memory",
    "menu-collapse",
    "menu-expand",
    "menu-icons_checkbox-on",
    "menu-icons_replication-58",
    "menu-icons_settings",
    "menu-icons_srorage",
    "menu-icons_stats-36",
    "menu",
    "merge",
    "metrics",
    "minus",
    "nested-document-property",
    "new-counter",
    "new-database",
    "new-document",
    "new-filesystem",
    "new-time-series - Copy",
    "new-time-series",
    "newline",
    "newtab",
    "node-add",
    "node-leader",
    "node",
    "notifications",
    "olap-etl",
    "olap",
    "ongoing-tasks",
    "orchestrator",
    "order",
    "os",
    "other",
    "output-collection",
    "parallel-stacks",
    "paste",
    "patch",
    "path",
    "pause",
    "percent",
    "periodic-backup",
    "periodic-export-13",
    "periodic-export",
    "phone",
    "pin",
    "pinned",
    "placeholder (1)",
    "placeholder",
    "play",
    "play2",
    "plus",
    "postgresql",
    "postpone",
    "powerbi",
    "prefetches",
    "preview-off",
    "preview",
    "print",
    "processor",
    "promote",
    "pull-replication-agent",
    "pull-replication-hub",
    "queries",
    "query",
    "quotas",
    "rabbitmq-etl",
    "rabbitmq-sink",
    "rabbitmq",
    "random",
    "raven",
    "ravendb-data",
    "ravendb-etl",
    "reassign-cores",
    "recent",
    "reference-pattern",
    "referenced-collections",
    "refresh-stats",
    "refresh",
    "rejected",
    "reorder",
    "replace",
    "replication-stats",
    "replication",
    "reset-index",
    "reset",
    "resources",
    "restore-backup",
    "restore",
    "revert-revisions",
    "revert",
    "revisions-bin",
    "revisions",
    "road-cone",
    "rocket",
    "running-queries",
    "running-tasks",
    "save",
    "search",
    "server-settings",
    "server-smuggling",
    "server-topology",
    "server-wide-backup",
    "server-wide-custom-analyzers",
    "server-wide-custom-sorters",
    "server-wide-replication",
    "server-wide-tasks",
    "server",
    "settings",
    "shard",
    "sharding",
    "shield",
    "shuffle",
    "skip",
    "snapshot-backup",
    "sortby",
    "sparkles",
    "spatial-map-view",
    "sql-attachment",
    "sql-binary",
    "sql-boolean",
    "sql-boolean2",
    "sql-document-id",
    "sql-etl",
    "sql-many-to-one",
    "sql-number",
    "sql-one-to-many",
    "sql-replication-stats",
    "sql-replication",
    "sql-string",
    "sql-unsupported",
    "stack-traces",
    "star-filled",
    "star",
    "start",
    "stats-menu",
    "stats-running-queries",
    "stats",
    "stepdown",
    "stop",
    "storage-free",
    "storage-used",
    "storage",
    "studio-config",
    "studio-configuration",
    "subscription",
    "subscriptions",
    "sum",
    "support",
    "swap",
    "system-storage",
    "system",
    "table",
    "tarfic-watch",
    "tasks-list",
    "tasks-menu",
    "tasks",
    "terms",
    "test",
    "theme",
    "thread-stack-trace",
    "thumb-down",
    "thumb-up",
    "tick",
    "timeseries-settings",
    "timeseries",
    "toggle-off",
    "toggle-on",
    "topology",
    "traffic-watch",
    "traffic",
    "transaction-record-replay",
    "transaction-record",
    "transform-results",
    "transformer",
    "trash-cutout",
    "trash",
    "twitter",
    "umbrella",
    "unencrypted",
    "unfold",
    "unlock",
    "unsecure",
    "unsupported-browser",
    "upgrade-arrow",
    "upload",
    "user-info",
    "user",
    "vector",
    "versioning",
    "waiting",
    "warning",
    "web-socket",
    "windows",
    "x",
    "zombie"
);
