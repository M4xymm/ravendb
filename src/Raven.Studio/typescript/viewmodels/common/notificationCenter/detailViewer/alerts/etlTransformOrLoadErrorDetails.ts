import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import alert = require("common/notifications/models/alert");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import copyToClipboard = require("common/copyToClipboard");
import generalUtils = require("common/generalUtils");
import moment = require("moment");

class etlTransformOrLoadErrorDetails extends abstractAlertDetails {
    
    view = require("views/common/notificationCenter/detailViewer/alerts/etlTransformOrLoadErrorDetails.html");

    currentDetails = ko.observable<Raven.Server.Documents.ETL.EtlItemError>();
    
    tableItems: Raven.Server.Documents.ETL.EtlItemError[] = [];
    private gridController = ko.observable<virtualGridController<Raven.Server.Documents.ETL.EtlItemError>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.Documents.ETL.EtlItemError>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        this.tableItems = (this.alert.details() as Raven.Server.Documents.ETL.Stats.EtlErrors)?.ItemErrors ?? [];

        // newest first
        this.tableItems.reverse();
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            
            const previewColumn = new actionColumn<Raven.Server.Documents.ETL.EtlItemError>(
                grid, item => this.showDetails(item), "Preview", `<i class="icon-preview"></i>`, "70px",
            {
                title: () => 'Show item preview'
            });
            const dateColumn = new textColumn<Raven.Server.Documents.ETL.EtlItemError>(grid, x => generalUtils.formatUtcDateAsLocal(x.CreatedAt), "Date", "20%", {
                sortable: x => x.CreatedAt
            });
            const errorColumn = new textColumn<Raven.Server.Documents.ETL.EtlItemError>(grid, x => x.Error, "Error", "50%", {
                sortable: x => x.Error
            });
            const documentIdColumn = new textColumn<Raven.Server.Documents.ETL.EtlItemError>(grid, x => x.DocumentId || ' - ', "Document ID", "20%", {
                sortable: x => x.DocumentId,
                customComparator: generalUtils.sortAlphaNumeric
            });
            
            return this.alert.alertType() === "Etl_LoadError" ?
                [previewColumn, dateColumn, errorColumn, documentIdColumn] :
                [previewColumn, documentIdColumn, dateColumn, errorColumn];
            });

        this.columnPreview.install(".etlErrorDetails", ".js-etl-error-details-tooltip",
            (details: Raven.Server.Documents.ETL.EtlItemError,
             column: textColumn<Raven.Server.Documents.ETL.EtlItemError>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                if (!(column instanceof actionColumn)) {
                    
                    if (column.header === "Date") {
                        onValue(moment.utc(details.CreatedAt), details.CreatedAt);
                    } else {
                        const value = column.getCellValue(details);
                        if (value) {
                            onValue(generalUtils.escapeHtml(value), value);
                        }
                    }
                }
            });
    }
    
    private showDetails(item: Raven.Server.Documents.ETL.EtlItemError) {
        this.currentDetails(item);
    }
    
    copyToClipboard(item: Raven.Server.Documents.ETL.EtlItemError) {
        copyToClipboard.copy(item.Error, "Error has been copied to clipboard", document.getElementById("js-etl-error-details"));
    }

    private fetcher(): JQueryPromise<pagedResult<Raven.Server.Documents.ETL.EtlItemError>> {
        return $.Deferred<pagedResult<Raven.Server.Documents.ETL.EtlItemError>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && (notification.alertType() == "Etl_LoadError" || notification.alertType() == "Etl_TransformationError");
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new etlTransformOrLoadErrorDetails(alert, center));
    }
}

export = etlTransformOrLoadErrorDetails;
